// Copyright 2020 InfluxData, Inc. All rights reserved.
// Use of this source code is governed by MIT
// license that can be found in the LICENSE file.

package influxdb2

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
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

// QueryApi provides methods for performing synchronously flux query against InfluxDB server
type QueryApi interface {
	// QueryString executes flux query on the InfluxDB server and returns complete query result as a string without table annotations (data types, group, defaults)
	QueryString(ctx context.Context, query string) (string, error)
	// QueryString executes flux query on the InfluxDB server and returns QueryRawResult which parses streamed response and returns raw csv lines
	QueryRaw(ctx context.Context, query string) (*QueryRawResult, error)
	// Query executes flux query on the InfluxDB server and returns  QueryCSVResult which parses streamed response into structures representing flux table parts
	Query(ctx context.Context, query string) (*QueryCSVResult, error)
}

// queryApiImpl implements QueryApi interface
type queryApiImpl struct {
	org    string
	client InfluxDBClient
	url    string
}

// queryReq wraps flux query request parameters
type queryReq struct {
	Query   string      `json:"query"`
	Type    string      `json:"type"`
	Dialect dialect     `json:"dialect"`
	Extern  interface{} `json:"extern,omitempty"`
}

// dialect wraps flux query dialect properties
type dialect struct {
	Annotations    []string `json:"annotations,omitempty"`
	CommentPrefix  string   `json:"commentPrefix,omitempty"`
	DateTimeFormat string   `json:"dateTimeFormat,omitempty"`
	Delimiter      string   `json:"delimiter,omitempty"`
	Header         bool     `json:"header"`
}

func (q *queryApiImpl) QueryString(ctx context.Context, query string) (string, error) {
	queryUrl, err := q.queryUrl()
	if err != nil {
		return "", err
	}
	var body string
	error := q.client.postRequest(ctx, queryUrl, strings.NewReader(query), func(req *http.Request) {
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

func (q *queryApiImpl) QueryRaw(ctx context.Context, query string) (*QueryRawResult, error) {
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
	error := q.client.postRequest(ctx, queryUrl, bytes.NewReader(qrJson), func(req *http.Request) {
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

func (q *queryApiImpl) Query(ctx context.Context, query string) (*QueryCSVResult, error) {
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
	error := q.client.postRequest(ctx, queryUrl, bytes.NewReader(qrJson), func(req *http.Request) {
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

func (q *queryApiImpl) queryUrl() (string, error) {
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

// QueryRawResult parses streamed flux query response into lines with csv rows
// Walking though the result is done by repeatedly calling Next() until returns false.
// Lines are than returned by Row()
// Preliminary end can be caused by an error, so when Next() return false, check Err() for an error
type QueryRawResult struct {
	io.Closer
	scanner *bufio.Scanner
	row     string
}

// Next walks through the flux query result line by line
// Returns false in case of end or an error, otherwise true
func (q *QueryRawResult) Next() bool {
	return q.scanner.Scan()
}

// Row returns the actual flux query result csv line
// First three rows are the flux query table annotations
func (q *QueryRawResult) Row() string {
	return q.scanner.Text()
}

// Err returns an error raised during flux query response parsing
func (q *QueryRawResult) Err() error {
	return q.scanner.Err()
}

// QueryRawResult parses streamed flux query response into structures representing flux table parts
// Walking though the result is done by repeatedly calling Next() until returns false.
// Actual flux table info (columns with names, data types, etc) is returned by TableMetadata() method.
// Data are acquired by Record() method.
// Preliminary end can be caused by an error, so when Next() return false, check Err() for an error
type QueryCSVResult struct {
	io.Closer
	csvReader     *csv.Reader
	tablePosition int
	table         *FluxTableMetadata
	record        *FluxRecord
	err           error
}

// TablePosition returns actual flux table position in the result.
// Each new table is introduced by annotations
func (q *QueryCSVResult) TablePosition() int {
	if q.tablePosition > 0 {
		return q.tablePosition - 1
	}
	return q.tablePosition
}

// TableMetadata returns actual flux table metadata
func (q *QueryCSVResult) TableMetadata() *FluxTableMetadata {
	return q.table
}

// Record returns last parsed flux table data row
// Use Record methods to access value and row properties
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
// During the first time run it creates also table metadata
// Actual parsed row is available through Record() function
// Returns false in case of end or an error, otherwise true
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
		q.record = newFluxRecord(q.table.Position(), values)
	case "#datatype":
		q.table = newFluxTableMetadata(q.tablePosition)
		q.tablePosition++
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

// Err returns an error raised during flux query response parsing
func (q *QueryCSVResult) Err() error {
	return q.err
}

// stringTernary returns a if not empty, otherwise b
func stringTernary(a, b string) string {
	if a == "" {
		return b
	}
	return a
}

// toValues converts s into type by t
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
