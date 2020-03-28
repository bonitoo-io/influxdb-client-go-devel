package client

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"encoding/base64"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
	"time"
)

const (
	stringDatatype       = "string"
	doubleDatatype       = "double"
	boolDatatype         = "bool"
	longDatatype         = "long"
	uLongDatatype        = "unsignedLong"
	durationDatatype     = "duration"
	base64BinaryDataType = "base64Binary"
	timeDatatypeRFC      = "dateTime:RFC3339"
	timeDatatypeRFCNano  = "dateTime:RFC3339Nano"
)

type QueryApi interface {
	QueryString(query string) (string, error)
	QueryRaw(query string) (*QueryRawResult, error)
	Query(query string) (*QueryCSVResult, error)
}

type QueryApiImpl struct {
	org    string
	client InfluxDBClient
	url    string
}

type queryReq struct {
	Query   string      `json:"query"`
	Type    string      `json:"type"`
	Dialect dialect     `json:"dialect"`
	Extern  interface{} `json:"extern,omitempty"`
}

type dialect struct {
	Annotations    []string `json:"annotations,omitempty"`
	CommentPrefix  string   `json:"commentPrefix,omitempty"`
	DateTimeFormat string   `json:"dateTimeFormat,omitempty"`
	Delimiter      string   `json:"delimiter,omitempty"`
	Header         bool     `json:"header"`
}

func (q *QueryApiImpl) QueryString(query string) (string, error) {
	queryUrl, err := q.queryUrl()
	if err != nil {
		return "", err
	}
	var body string
	error := q.client.postRequest(queryUrl, strings.NewReader(query), func(req *http.Request) {
		req.Header.Add("Content-Type", "application/vnd.flux")
	},
		func(resp *http.Response) error {
			respBody, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				return err
			}
			body = string(respBody)
			return nil
		})
	if error != nil {
		return "", error
	}
	return body, nil
}

func (q *QueryApiImpl) QueryRaw(query string) (*QueryRawResult, error) {
	var queryResult *QueryRawResult
	queryUrl, err := q.queryUrl()
	if err != nil {
		return nil, err
	}
	qr := queryReq{Query: query, Type: "flux", Dialect: dialect{
		Annotations: []string{"datatype", "group", "default"},
		Delimiter:   ",",
		Header:      true,
	}}
	qrJson, err := json.Marshal(qr)
	if err != nil {
		return nil, err
	}
	error := q.client.postRequest(queryUrl, bytes.NewReader(qrJson), func(req *http.Request) {
		req.Header.Set("Content-Type", "application/json")
	},
		func(resp *http.Response) error {
			scan := bufio.NewScanner(resp.Body)
			queryResult = &QueryRawResult{Closer: resp.Body, scanner: scan}
			return nil
		})
	if error != nil {
		return queryResult, error
	}
	return queryResult, nil
}

func (q *QueryApiImpl) Query(query string) (*QueryCSVResult, error) {
	var queryResult *QueryCSVResult
	queryUrl, err := q.queryUrl()
	if err != nil {
		return nil, err
	}
	qr := queryReq{Query: query, Type: "flux", Dialect: dialect{
		Annotations: []string{"datatype", "group", "default"},
		Delimiter:   ",",
		Header:      true,
	}}
	qrJson, err := json.Marshal(qr)
	if err != nil {
		return nil, err
	}
	error := q.client.postRequest(queryUrl, bytes.NewReader(qrJson), func(req *http.Request) {
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Accept-Encoding", "gzip")
	},
		func(resp *http.Response) error {
			if resp.Header.Get("Content-Encoding") == "gzip" {
				resp.Body, err = gzip.NewReader(resp.Body)
				if err != nil {
					return err
				}
			}
			csvReader := csv.NewReader(resp.Body)
			csvReader.FieldsPerRecord = -1
			queryResult = &QueryCSVResult{Closer: resp.Body, csvReader: csvReader}
			return nil
		})
	if error != nil {
		return queryResult, error
	}
	return queryResult, nil
}

func (q *QueryApiImpl) queryUrl() (string, error) {
	if q.url == "" {
		u, err := url.Parse(q.client.ServerUrl())
		if err != nil {
			return "", err
		}
		u.Path = path.Join(u.Path, "/api/v2/query")

		params := u.Query()
		params.Set("org", q.org)
		u.RawQuery = params.Encode()
		q.url = u.String()
	}
	return q.url, nil
}

type QueryRawResult struct {
	io.Closer
	scanner *bufio.Scanner
	row     string
}

func (q *QueryRawResult) Next() bool {
	return q.scanner.Scan()
}

func (q *QueryRawResult) Row() string {
	return q.scanner.Text()
}

func (q *QueryRawResult) Err() error {
	return q.scanner.Err()
}

type QueryCSVResult struct {
	io.Closer
	csvReader  *csv.Reader
	tableIndex int
	table      *FluxTableMetadata
	record     *FluxRecord
	err        error
}

// TableIndex returns actual flux tableIndex index
func (q *QueryCSVResult) TableIndex() int {
	if q.tableIndex > 0 {
		return q.tableIndex - 1
	}
	return q.tableIndex
}

// Table returns actual flux tableIndex metadata
func (q *QueryCSVResult) Table() *FluxTableMetadata {
	return q.table
}

// Record returns last parsed tableIndex data row
func (q *QueryCSVResult) Record() *FluxRecord {
	return q.record
}

type parsingState int

const (
	parsingStateNormal parsingState = iota
	parsingStateNameRow
	parsingStateError
)

// Next advances to next row in query result.
// During the first time run it creates also tableIndex metadata
// Actual parsed row is available through #Record() function
func (q *QueryCSVResult) Next() bool {
	var row []string
	// set closing query in case of preliminary return
	closer := func() {
		if err := q.Close(); err != nil {
			message := err.Error()
			if q.err != nil {
				message = fmt.Sprintf("%s,%s", message, q.err.Error())
			}
			q.err = errors.New(message)
		}
	}
	defer func() {
		closer()
	}()
	parsingState := parsingStateNormal
readRow:
	row, q.err = q.csvReader.Read()
	if q.err == io.EOF {
		q.err = nil
		return false
	}
	if q.err != nil {
		return false
	}

	if len(row) <= 1 {
		goto readRow
	}
	switch row[0] {
	case "":
		if parsingState == parsingStateError {
			var message string
			if len(row) > 1 {
				message = row[1]
			} else {
				message = "unknown query error"
			}
			reference := ""
			if len(row) > 2 && len(row[2]) > 0 {
				reference = fmt.Sprintf(",%s", row[2])
			}
			q.err = fmt.Errorf("%s%s", message, reference)
			return false
		} else if parsingState == parsingStateNameRow {
			if row[1] == "error" {
				parsingState = parsingStateError
			} else {
				for i, n := range row[1:] {
					if q.table.Column(i) != nil {
						q.table.Column(i).SetName(n)
					}
				}
				parsingState = parsingStateNormal
			}
			goto readRow
		}
		if q.table == nil {
			q.err = errors.New("parsing error, table definition not found")
			return false
		}
		if len(row)-1 != len(q.table.Columns()) {
			q.err = fmt.Errorf("parsing error, row has different number of columns than table: %d vs %d", len(row)-1, len(q.table.Columns()))
			return false
		}
		values := make(map[string]interface{})
		for i, v := range row[1:] {
			if q.table.Column(i) != nil {
				values[q.table.Column(i).Name()], q.err = toValue(stringTernary(v, q.table.Column(i).DefaultValue()), q.table.Column(i).DataType())
				if q.err != nil {
					return false
				}
			}
		}
		q.record = newFluxRecord(q.table.Index(), values)
	case "#datatype":
		q.table = newFluxTableMetadata(q.tableIndex)
		q.tableIndex++
		for i, d := range row[1:] {
			q.table.AddColumn(newFluxColumn(i, d))
		}
		goto readRow
	case "#group":
		for i, g := range row[1:] {
			if q.table.Column(i) != nil {
				q.table.Column(i).SetGroup(g == "true")
			}
		}
		goto readRow
	case "#default":
		for i, c := range row[1:] {
			if q.table.Column(i) != nil {
				q.table.Column(i).SetDefaultValue(c)
			}
		}
		// there comes column names after defaults
		parsingState = parsingStateNameRow
		goto readRow
	}
	// don't close query
	closer = func() {}
	return true
}

func (q *QueryCSVResult) Err() error {
	return q.err
}

func stringTernary(a, b string) string {
	if a == "" {
		return b
	}
	return a
}

func toValue(s, t string) (interface{}, error) {
	switch t {
	case stringDatatype:
		return s, nil
	case timeDatatypeRFC:
		return time.Parse(time.RFC3339, s)
	case timeDatatypeRFCNano:
		return time.Parse(time.RFC3339Nano, s)
	case durationDatatype:
		return time.ParseDuration(s)
	case doubleDatatype:
		return strconv.ParseFloat(s, 64)
	case boolDatatype:
		if strings.ToLower(s) == "false" {
			return false, nil
		}
		return true, nil
	case longDatatype:
		return strconv.ParseInt(s, 10, 64)
	case uLongDatatype:
		return strconv.ParseUint(s, 10, 64)
	case base64BinaryDataType:
		return base64.StdEncoding.DecodeString(s)
	default:
		return nil, fmt.Errorf("%s has unknown data type %s", s, t)
	}
}
