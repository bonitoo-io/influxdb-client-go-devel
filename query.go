package client

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"net/http"
)

type QueryApi struct {
	org    string
	client *InfluxDBClient
}

func (q *QueryApi) QueryString(query string) (string, error) {
	url := fmt.Sprintf("%s/api/v2/query?org=%s", q.client.serverUrl, q.org)
	var body string
	err := q.client.postRequest(url, query, http.StatusOK, func(req *http.Request) {
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
	if err != nil {
		return "", err
	}
	return body, nil
}

type QueryResultCallback func(line string)

func (q *QueryApi) QueryRaw(query string, callback QueryResultCallback) error {
	url := fmt.Sprintf("%s/api/v2/query?org=%s", q.client.serverUrl, q.org)
	err := q.client.postRequest(url, query, http.StatusOK, func(req *http.Request) {
		req.Header.Add("Content-Type", "application/vnd.flux")
	},
		func(resp *http.Response) error {
			scan := bufio.NewScanner(resp.Body)
			for scan.Scan() {
				callback(scan.Text())
			}
			if err := scan.Err(); err != nil {
				return fmt.Errorf("error parsing response: %s", err.Error())
			}
			return nil
		})
	return err
}
