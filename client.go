// Copyright 2020 InfluxData, Inc. All rights reserved.
// Use of this source code is governed by MIT
// license that can be found in the LICENSE file.

// package influxdb2 provides API for using InfluxDB client in Go
// It's intended to use with InfluxDB 2 server
package influxdb2

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"mime"
	"net"
	"net/http"
	url2 "net/url"
	"path"
	"runtime"
	"strconv"
	"sync"
	"time"

	"github.com/bonitoo-io/influxdb-client-go/domain"
)

const (
	Version = "1.0.0"
)

// Keeps once created User-Agent string
var userAgentCache string

// userAgent does lazy user-agent string initialisation
func userAgent() string {
	if userAgentCache == "" {
		userAgentCache = fmt.Sprintf("influxdb-client-go/%s  (%s; %s)", Version, runtime.GOOS, runtime.GOARCH)
	}
	return userAgentCache
}

// Options holds configuration properties for communicating with InfluxDB server
type Options struct {
	// Maximum number of points sent to server in single request. Default 5000
	BatchSize uint
	// Interval, in ms, in which is buffer flushed if it has not been already written (by reaching batch size) . Default 1000ms
	FlushInterval uint
	// Default retry interval in sec, if not sent by server. Default 30s
	RetryInterval uint
	// Maximum count of retry attempts of failed writes
	MaxRetries uint
	// Maximum number of points to keep for retry. Should be multiple of BatchSize. Default 10,000
	RetryBufferLimit uint
	// DebugLevel to filter log messages. Each level mean to log all categories bellow. 0 error, 1 - warning, 2 - info, 3 - debug
	Debug uint
	// Precision to use in writes for timestamp. In unit of duration: time.Nanosecond, time.Microsecond, time.Millisecond, time.Second
	// Default time.Nanosecond
	Precision time.Duration
	// Whether to use GZip compression in requests. Default false
	UseGZip bool
}

// DefaultOptions returns Options object with default values
func DefaultOptions() *Options {
	return &Options{BatchSize: 5000, MaxRetries: 3, RetryInterval: 60, FlushInterval: 1000, Precision: time.Nanosecond, UseGZip: false, RetryBufferLimit: 10000}
}

// Error represent error response from InfluxDBServer or http error
type Error struct {
	StatusCode int
	Code       string
	Message    string
	Err        error
	RetryAfter uint
}

// Error fulfils error interface
func (e *Error) Error() string {
	if e.Err != nil {
		return e.Err.Error()
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

// NewError returns newly created Error initialised with nested error and default values
func NewError(err error) *Error {
	return &Error{
		StatusCode: 0,
		Code:       "",
		Message:    "",
		Err:        err,
		RetryAfter: 0,
	}
}

// InfluxDBClient provides API to communicate with InfluxDBServer
// There two APIs for writing, WriteApi and WriteApiBlocking.
// WriteApi provides asynchronous, non-blocking, methods for writing time series data.
// WriteApiBlocking provides blocking methods for writing time series data
type InfluxDBClient interface {
	// WriteApi returns the asynchronous, non-blocking, Write client.
	WriteApi(org, bucket string) WriteApi
	// WriteApi returns the synchronous, blocking, Write client.
	WriteApiBlocking(org, bucket string) WriteApiBlocking
	// QueryAPI returns Query client
	QueryAPI(org string) QueryApi
	// Close ensures all ongoing asynchronous write clients finish
	Close()
	// Options returns the options associated with client
	Options() *Options
	// ServerUrl returns the url of the server url client talks to
	ServerUrl() string
	// Setup sends request to initialise new InfluxDB server with user, org and bucket
	// and returns details about newly created entities along with the authorization object
	Setup(ctx context.Context, username, password, org, bucket string) (*domain.OnboardingResponse, error)
	// Ready checks InfluxDB server is running
	Ready(ctx context.Context) (bool, error)
	// Internal  method for handling posts
	postRequest(ctx context.Context, url string, body io.Reader, requestCallback RequestCallback, responseCallback ResponseCallback) *Error
}

// client implements InfluxDBClient interface
type client struct {
	serverUrl     string
	authorization string
	options       Options
	writeApis     []WriteApi
	httpDoer      domain.HttpRequestDoer
	lock          sync.Mutex
}

// Http operation callbacks
type RequestCallback func(req *http.Request)
type ResponseCallback func(req *http.Response) error

func NewInfluxDBClientEmpty(serverUrl string) InfluxDBClient {
	return NewInfluxDBClientWithToken(serverUrl, "")
}

// NewInfluxDBClient creates InfluxDBClient for connecting to given serverUrl with provided authentication token, with default options
// Authentication token can be empty in case of connecting to newly installed InfluxDB server, which has not been set up yet.
// In such case Setup will set authentication token
func NewInfluxDBClientWithToken(serverUrl string, authToken string) InfluxDBClient {
	return NewInfluxDBClientWithOptions(serverUrl, authToken, *DefaultOptions())
}

// NewInfluxDBClientWithOptions creates InfluxDBClient for connecting to given serverUrl with provided authentication token
// and configured with custom Options
// Authentication token can be empty in case of connecting to newly installed InfluxDB server, which has not been set up yet.
// In such case Setup will set authentication token
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

func (c *client) Ready(ctx context.Context) (bool, error) {
	url, err := url2.Parse(c.serverUrl)
	if err != nil {
		return false, err
	}
	url.Path = path.Join(url.Path, "ready")
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url.String(), nil)
	if err != nil {
		return false, err
	}
	req.Header.Set("User-Agent", userAgent())
	resp, err := c.httpDoer.Do(req)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()
	return resp.StatusCode == http.StatusOK, nil
}

func (c *client) WriteApi(org, bucket string) WriteApi {
	w := newWriteApiImpl(org, bucket, c)
	c.writeApis = append(c.writeApis, w)
	return w
}

func (c *client) WriteApiBlocking(org, bucket string) WriteApiBlocking {
	w := newWriteApiBlockingImpl(org, bucket, c)
	return w
}

func (c *client) Close() {
	for _, w := range c.writeApis {
		w.close()
	}
}

func (c *client) QueryAPI(org string) QueryApi {
	return &queryApiImpl{
		org:    org,
		client: c,
	}
}

func (c *client) postRequest(ctx context.Context, url string, body io.Reader, requestCallback RequestCallback, responseCallback ResponseCallback) *Error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, body)
	if err != nil {
		return NewError(err)
	}
	req.Header.Set("Authorization", c.authorization)
	req.Header.Set("User-Agent", userAgent())
	if requestCallback != nil {
		requestCallback(req)
	}
	resp, err := c.httpDoer.Do(req)
	if err != nil {
		return NewError(err)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return c.handleHttpError(resp)
	}
	if responseCallback != nil {
		err := responseCallback(resp)
		if err != nil {
			return NewError(err)
		}
	}
	return nil
}

func (c *client) handleHttpError(r *http.Response) *Error {
	// successful status code range
	if r.StatusCode >= 200 && r.StatusCode < 300 {
		return nil
	}

	error := NewError(nil)
	error.StatusCode = r.StatusCode
	if v := r.Header.Get("Retry-After"); v != "" {
		r, err := strconv.ParseUint(v, 10, 32)
		if err == nil {
			error.RetryAfter = uint(r)
		}
	}
	// json encoded error
	ctype, _, _ := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if ctype == "application/json" {
		err := json.NewDecoder(r.Body).Decode(error)
		error.Err = err
		return error
	} else {
		body, err := ioutil.ReadAll(r.Body)
		if err != nil {
			error.Err = err
			return error
		}

		error.Code = r.Status
		error.Message = string(body)
	}

	if error.Code == "" && error.Message == "" {
		switch r.StatusCode {
		case http.StatusTooManyRequests:
			error.Code = "too many requests"
			error.Message = "exceeded rate limit"
		case http.StatusServiceUnavailable:
			error.Code = "unavailable"
			error.Message = "service temporarily unavailable"
		}
	}
	return error
}
