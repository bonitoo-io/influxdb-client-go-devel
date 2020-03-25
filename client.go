package client

import (
	"errors"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	url2 "net/url"
	"path"
	"time"

	. "github.com/bonitoo-io/influxdb-client-go/domain"
)

const (
	WritePrecisionNS WritePrecision = "ns"
	WritePrecisionUS WritePrecision = "us"
	WritePrecisionMS WritePrecision = "ms"
	WritePrecisionS  WritePrecision = "s"
)

type Options struct {
	// Maximum number of points sent to server in single request. Default 5000
	BatchSize int
	// Interval, in ms, in which is buffer flushed if it has not been already written (by reaching batch size) . Default 1000ms
	FlushInterval int
	// Maximum count of retry attempts of failed writes
	MaxRetries int
	// 0 error, 1 - warning, 2 - info, 3 - debug
	Debug uint
	// Default retry interval in sec, if not sent by server
	// Default  30s
	RetryInterval int
	// Precision to use in writes, default NS
	Precision WritePrecision
	// Whether to use GZip compression in requests. Default false
	UseGZip bool
}

// Options with default values
// TODO: singleton?
func DefaultOptions() *Options {
	return &Options{BatchSize: 5000, MaxRetries: 3, RetryInterval: 60, FlushInterval: 1000, Precision: WritePrecisionNS, UseGZip: false}
}

type InfluxDBClient interface {
	WriteAPI(org, bucket string) WriteApi
	Close()
	QueryAPI(org string) QueryApi
	postRequest(url string, body io.Reader, requestCallback RequestCallback, responseCallback ResponseCallback) error
	Options() *Options
	ServerUrl() string
	Setup(username, password, org, bucket string) (*SetupResponse, error)
	Ready() (bool, error)
}

type client struct {
	serverUrl     string
	authorization string
	options       Options
	writeApis     []WriteApi
	httpDoer      HttpRequestDoer
}

type RequestCallback func(req *http.Request)
type ResponseCallback func(req *http.Response) error

func NewInfluxDBClientEmpty(serverUrl string) InfluxDBClient {
	return NewInfluxDBClientWithToken(serverUrl, "")
}

func NewInfluxDBClientWithToken(serverUrl string, authToken string) InfluxDBClient {
	return NewInfluxDBClientWithOptions(serverUrl, authToken, *DefaultOptions())
}

func NewInfluxDBClientWithOptions(serverUrl string, authToken string, options Options) InfluxDBClient {
	client := &client{
		serverUrl:     serverUrl,
		authorization: "Token " + authToken,
		httpDoer: &http.Client{
			Timeout: time.Second * 60,
			Transport: &http.Transport{
				DialContext: (&net.Dialer{
					Timeout: 30 * time.Second,
				}).DialContext,
				TLSHandshakeTimeout: 30 * time.Second,
				WriteBufferSize:     500 * 1024,
			},
		},
		options:   options,
		writeApis: make([]WriteApi, 0, 5),
	}
	return client
}
func (c *client) Options() *Options {
	return &c.options
}

func (c *client) ServerUrl() string {
	return c.serverUrl
}

func (c *client) Ready() (bool, error) {
	url, err := url2.Parse(c.serverUrl)
	if err != nil {
		return false, err
	}
	url.Path = path.Join(url.Path, "ready")
	req, err := http.NewRequest(http.MethodGet, url.String(), nil)
	if err != nil {
		return false, err
	}
	resp, err := c.httpDoer.Do(req)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()
	return resp.StatusCode == http.StatusOK, nil
}

func (c *client) WriteAPI(org, bucket string) WriteApi {
	w := newWriteApiImpl(org, bucket, c)
	c.writeApis = append(c.writeApis, w)
	return w
}
func (c *client) Close() {
	for _, w := range c.writeApis {
		w.close()
	}
}

func (c *client) QueryAPI(org string) QueryApi {
	return &QueryApiImpl{
		org:    org,
		client: c,
	}
}

func (c *client) postRequest(url string, body io.Reader, requestCallback RequestCallback, responseCallback ResponseCallback) error {
	req, err := http.NewRequest(http.MethodPost, url, body)
	if err != nil {
		return err
	}
	req.Header.Add("Authorization", c.authorization)
	req.Header.Add("User-Agent", "InfluxDB Go Client")
	if requestCallback != nil {
		requestCallback(req)
	}
	resp, err := c.httpDoer.Do(req)
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
