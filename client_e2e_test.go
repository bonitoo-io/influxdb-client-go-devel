// Copyright 2020 InfluxData, Inc. All rights reserved.
// Use of this source code is governed by MIT
// license that can be found in the LICENSE file.

package influxdb2

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"strconv"
	"testing"
	"time"
)

var e2e bool

func init() {
	flag.BoolVar(&e2e, "e2e", false, "run the end tests (requires a working influxdb instance on 127.0.0.1)")
	flag.StringVar(&authToken, "token", "", "authentication token")
}

func TestReady(t *testing.T) {
	if !e2e {
		t.Skip("e2e not enabled. Launch InfluxDB 2 on localhost and run test with -e2e")
	}
	client := NewClient("http://localhost:9999", "my-token-123")

	ok, err := client.Ready(context.Background())
	if err != nil {
		t.Error(err)
	}
	if !ok {
		t.Fail()
	}
}

var authToken string

func TestSetup(t *testing.T) {
	if !e2e {
		t.Skip("e2e not enabled. Launch InfluxDB 2 on localhost and run test with -e2e")
	}
	client := NewClientWithOptions("http://localhost:9999", "", DefaultOptions().SetLogLevel(2))
	response, err := client.Setup(context.Background(), "my-user", "my-password", "my-org", "my-bucket", 0)
	if err != nil {
		t.Error(err)
	}
	require.NotNil(t, response)
	authToken = *response.Auth.Token
	fmt.Println("Token:" + authToken)

	response, err = client.Setup(context.Background(), "my-user", "my-password", "my-org", "my-bucket", 0)
	require.NotNil(t, err)
	assert.Equal(t, "conflict: onboarding has already been completed", err.Error())
}
func TestWrite(t *testing.T) {
	if !e2e {
		t.Skip("e2e not enabled. Launch InfluxDB 2 on localhost and run test with -e2e")
	}
	client := NewClientWithOptions("http://localhost:9999", token(), DefaultOptions().SetLogLevel(3))
	writeApi := client.WriteApi("my-org", "my-bucket")
	for i, f := 0, 3.3; i < 10; i++ {
		writeApi.WriteRecord(fmt.Sprintf("test,a=%d,b=local f=%.2f,i=%di", i%2, f, i))
		//writeApi.Flush()
		f += 3.3
	}

	for i, f := int64(10), 33.0; i < 20; i++ {
		p := NewPoint("test",
			map[string]string{"a": strconv.FormatInt(i%2, 10), "b": "static"},
			map[string]interface{}{"f": f, "i": i},
			time.Now())
		writeApi.WritePoint(p)
		f += 3.3
	}

	client.Close()

}

func TestQueryRaw(t *testing.T) {
	if !e2e {
		t.Skip("e2e not enabled. Launch InfluxDB 2 on localhost and run test with -e2e")
	}
	client := NewClient("http://localhost:9999", token())

	queryApi := client.QueryApi("my-org")
	res, err := queryApi.QueryRaw(context.Background(), `from(bucket:"my-bucket")|> range(start: -1h) |> filter(fn: (r) => r._measurement == "test")`, nil)
	if err != nil {
		t.Error(err)
	} else {
		fmt.Println("QueryResult:")
		fmt.Println(res)
	}
}

func TestQuery(t *testing.T) {
	if !e2e {
		t.Skip("e2e not enabled. Launch InfluxDB 2 on localhost and run test with -e2e")
	}
	client := NewClient("http://localhost:9999", token())

	queryApi := client.QueryApi("my-org")
	fmt.Println("QueryResult")
	result, err := queryApi.Query(context.Background(), `from(bucket:"my-bucket")|> range(start: -24h) |> filter(fn: (r) => r._measurement == "test")`)
	if err != nil {
		t.Error(err)
	} else {
		for result.Next() {
			if result.TableChanged() {
				fmt.Printf("table: %s\n", result.TableMetadata().String())
			}
			fmt.Printf("row: %sv\n", result.Record().String())
		}
		if result.Err() != nil {
			t.Error(result.Err())
		}
	}
}

func TestSecureConnection(t *testing.T) {
	tlsC := &tls.Config{
		InsecureSkipVerify: true,
	}
	client := NewClientWithOptions("https://eu-central-1-1.aws.cloud2.influxdata.com/", "ty1R9RztfuVbP-B5XJufmAdyKqechWTMGLdsBDuY4BsR5sv2DuGO69vu-Tc7dZNx0iApmcFnt8dtsYYdG69tIQ==", DefaultOptions().SetTlsConfig(tlsC))
	write := client.WriteApiBlocking("cbd7bce713f63c02", "room_monitoring")
	p := NewPointWithMeasurement("air").
		AddTag("sensor", "cpu").
		AddTag("location", "server").
		AddField("temp", 3.5).
		AddField("hum", 58).
		SetTime(time.Now())
	err := write.WritePoint(context.Background(), p)
	if err != nil {
		t.Error(err)
	}

}

func token() string {
	if authToken == "" {
		authToken = "3a0_wuLowYq3mrd4Ja4aRKdVviwQNSYoWyL1px-rncENmppEYodf4UWswxk_kFGzTG2gOM5t7q3JuZJSP5cQ2Q=="
	}
	return authToken
}
