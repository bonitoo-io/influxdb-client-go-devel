package client

import (
	"log"
	"testing"
)

func TestReady(t *testing.T) {
	client := NewInfluxDBClient("http://localhost:9999", "my-token-123")
	ok, err := client.Ready()
	if err != nil {
		t.Error(err)
	}
	if !ok {
		t.Fail()
	}
}

func TestWrite(t *testing.T) {
	client := NewInfluxDBClient("http://localhost:9999", "my-token-123")

	writeApi := client.WriteAPI("my-org", "my-bucket")
	err := writeApi.WriteRecord("test,a=1,b=adsfasdf f=1.4,i=4i")
	if err != nil {
		t.Error(err)
	}
}

func TestQueryString(t *testing.T) {
	client := NewInfluxDBClient("http://localhost:9999", "my-token-123")

	queryApi := client.QueryAPI("my-org")
	res, err := queryApi.QueryString(`from(bucket:"my-bucket")|> range(start: -24h) |> filter(fn: (r) => r._measurement == "test")`)
	if err != nil {
		t.Error(err)
	}
	log.Println("QueryResult")
	log.Println(res)
}

func TestQueryRaw(t *testing.T) {
	client := NewInfluxDBClient("http://localhost:9999", "my-token-123")

	queryApi := client.QueryAPI("my-org")
	log.Println("QueryResult")
	err := queryApi.QueryRaw(`from(bucket:"my-bucket")|> range(start: -24h) |> filter(fn: (r) => r._measurement == "test")`, func(line string) {
		log.Println(line)
	})
	if err != nil {
		t.Error(err)
	}
}
