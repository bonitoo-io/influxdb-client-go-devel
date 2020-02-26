package client

import (
	"fmt"
	"strings"
)

type WriteApi interface {
	WriteRecord(line string) error
}

type WriteApiImpl struct {
	org    string
	bucket string
	client *InfluxDBClient
}

func (w *WriteApiImpl) WriteRecord(line string) error {
	url := fmt.Sprintf("%s/api/v2/write?org=%s&bucket=%s", w.client.serverUrl, w.org, w.bucket)
	return w.client.postRequest(url, strings.NewReader(line), nil, nil)
}
