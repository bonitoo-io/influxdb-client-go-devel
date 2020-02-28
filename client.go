package client

import (
	"errors"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"time"
)

type Options struct {
	BatchSize int
	// flush interval in ms
	FlushInterval int
	MaxRetries    int
	// 0 error, 1 - warning, 2 - info, 3 - debug
	Debug int
	// Retry interval in sec
	RetryInterval int
}

// TODO: singleton?
func DefaultOptions() *Options {
	return &Options{BatchSize: 5000, MaxRetries: 3, RetryInterval: 60, FlushInterval: 1000}
}

type InfluxDBClient struct {
	serverUrl     string
	authorization string
	client        *http.Client
	options       Options
	writeApis     []WriteApi
}

type RequestCallback func(req *http.Request)
type ResponseCallback func(req *http.Response) error

func NewInfluxDBClientEmpty(serverUrl string) *InfluxDBClient {
	return NewInfluxDBClient(serverUrl, "")
}

func NewInfluxDBClient(serverUrl string, authToken string) *InfluxDBClient {
	return NewInfluxDBClientWithOptions(serverUrl, authToken, *DefaultOptions())
}

func NewInfluxDBClientWithOptions(serverUrl string, authToken string, options Options) *InfluxDBClient {
	client := &InfluxDBClient{
		serverUrl:     serverUrl,
		authorization: "Token " + authToken,
		client: &http.Client{
			Timeout: time.Second * 60,
			Transport: &http.Transport{
				DialContext: (&net.Dialer{
					Timeout: 30 * time.Second,
				}).DialContext,
				TLSHandshakeTimeout: 30 * time.Second,
			},
		},
		options:   options,
		writeApis: make([]WriteApi, 0, 5),
	}
	return client
}
func (c InfluxDBClient) Ready() (bool, error) {
	url := c.serverUrl + "/ready"
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return false, err
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()
	return resp.StatusCode == http.StatusOK, nil
}

func (c *InfluxDBClient) WriteAPI(org, bucket string) WriteApi {
	w := newWriteApiImpl(org, bucket, c)
	c.writeApis = append(c.writeApis, w)
	return w
}
func (c *InfluxDBClient) Close() {
	for _, w := range c.writeApis {
		w.close()
	}
}

func (c *InfluxDBClient) QueryAPI(org string) QueryApi {
	return &QueryApiImpl{
		org:    org,
		client: c,
	}
}

func (c *InfluxDBClient) postRequest(url string, body io.Reader, requestCallback RequestCallback, responseCallback ResponseCallback) error {
	req, err := http.NewRequest(http.MethodPost, url, body)
	if err != nil {
		return err
	}
	req.Header.Add("Authorization", c.authorization)
	req.Header.Add("User-Agent", "InfluxDB Go Client")
	if requestCallback != nil {
		requestCallback(req)
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		//TODO: read json
		respBody, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		return errors.New(string(respBody))
	}
	if responseCallback != nil {
		err := responseCallback(resp)
		if err != nil {
			return err
		}
	}
	return nil
}
