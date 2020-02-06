package client

import (
	"fmt"
	"net/http"
)

type WriteApi struct {
	org    string
	bucket string
	client *InfluxDBClient
}

func (w *WriteApi) WriteRecord(line string) error {
	url := fmt.Sprintf("%s/api/v2/write?org=%s&bucket=%s", w.client.serverUrl, w.org, w.bucket)
	return w.client.postRequest(url, line, http.StatusNoContent, nil, nil)
}
