package client

import (
	"compress/gzip"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"
	"strings"
	"testing"
	"time"
)

type testClient struct {
	lines   []string
	options *Options
	t       *testing.T
	wasGzip bool
}

func (t *testClient) WriteAPI(org, bucket string) WriteApi {
	return nil
}

func (t *testClient) Close() {
	if len(t.lines) > 0 {
		t.lines = t.lines[:0]
	}
	t.wasGzip = false
}

func (t *testClient) QueryAPI(org string) QueryApi {
	return nil
}

func (t *testClient) postRequest(url string, body io.Reader, requestCallback RequestCallback, responseCallback ResponseCallback) error {
	req, err := http.NewRequest("POST", url, nil)
	if err != nil {
		return err
	}
	if requestCallback != nil {
		requestCallback(req)
	}
	if req.Header.Get("Content-Encoding") == "gzip" {
		body, err = gzip.NewReader(body)
		t.wasGzip = true
	}
	assert.Equal(t.t, url, fmt.Sprintf("%s/api/v2/write?bucket=my-bucket&org=my-org", t.ServerUrl()))
	bytes, err := ioutil.ReadAll(body)
	lines := strings.Split(string(bytes), "\n")
	lines = lines[:len(lines)-1]
	t.lines = append(t.lines, lines...)
	return err
}

func (t *testClient) Options() *Options {
	return t.options
}

func (t *testClient) ServerUrl() string {
	return "http://locahost:8900"
}

func (t *testClient) Setup(username, password, org, bucket string) (*SetupResponse, error) {
	return nil, nil
}
func (t *testClient) Ready() (bool, error) {
	return true, nil
}

func genPoints(num int) []*Point {
	points := make([]*Point, num)
	rand.Seed(321)

	t := time.Now()
	for i := 0; i < len(points); i++ {
		points[i] = NewPoint(
			"test",
			map[string]string{
				"id":       fmt.Sprintf("rack_%v", i%100),
				"vendor":   "AWS",
				"hostname": fmt.Sprintf("host_%v", i%10),
			},
			map[string]interface{}{
				"temperature": rand.Float64() * 80.0,
				"disk_free":   rand.Float64() * 1000.0,
				"disk_total":  (i/10 + 1) * 1000000,
				"mem_total":   (i/100 + 1) * 10000000,
				"mem_free":    rand.Float64() * 10000000.0,
			},
			t)
		if i%10 == 0 {
			t = t.Add(time.Second)
		}
	}
	return points
}

func TestWriteApiImpl_Write(t *testing.T) {
	client := &testClient{
		options: DefaultOptions(),
		t:       t,
	}
	client.options.BatchSize = 5
	writeApi := newWriteApiImpl("my-org", "my-bucket", client)
	points := genPoints(10)
	for _, p := range points {
		writeApi.Write(p)
	}
	writeApi.close()
	require.Len(t, client.lines, 10)
	for i, p := range points {
		line := p.ToLineProtocol(client.options.Precision)
		//cut off last \n char
		line = line[:len(line)-1]
		assert.Equal(t, client.lines[i], line)
	}
}

func TestGzipWithFlushing(t *testing.T) {
	client := &testClient{
		options: DefaultOptions(),
		t:       t,
	}
	client.options.BatchSize = 5
	client.options.UseGZip = true
	writeApi := newWriteApiImpl("my-org", "my-bucket", client)
	points := genPoints(5)
	for _, p := range points {
		writeApi.Write(p)
	}
	time.Sleep(time.Millisecond * 10)
	require.Len(t, client.lines, 5)
	assert.True(t, client.wasGzip)

	client.Close()
	client.options.UseGZip = false
	for _, p := range points {
		writeApi.Write(p)
	}
	time.Sleep(time.Millisecond * 10)
	require.Len(t, client.lines, 5)
	assert.False(t, client.wasGzip)

	writeApi.close()
}
