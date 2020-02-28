package client

import (
	"fmt"
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

var authToken string

func TestSetup(t *testing.T) {
	client := NewInfluxDBClientEmpty("http://localhost:9999")
	client.options.Debug = 2
	response, err := client.Setup("my-user", "my-password", "my-org", "my-bucket")
	if err != nil {
		t.Error(err)
	}
	if response != nil {
		authToken = response.Auth.Token
		fmt.Println("Token:" + authToken)
	}

}
func TestWrite(t *testing.T) {
	client := NewInfluxDBClient("http://localhost:9999", token())
	client.options.Debug = 3
	writeApi := client.WriteAPI("my-org", "my-bucket")
	for i, f := 0, 3.3; i < 10; i++ {
		writeApi.WriteRecord(fmt.Sprintf("test,a=%d,b=adsfasdf f=%.2f,i=%di", i%2, f, i))
		//writeApi.Flush()
		f += 3.3
	}
	client.Close()

}

func TestQueryString(t *testing.T) {
	client := NewInfluxDBClient("http://localhost:9999", token())

	queryApi := client.QueryAPI("my-org")
	res, err := queryApi.QueryString(`from(bucket:"my-bucket")|> range(start: -1h) |> filter(fn: (r) => r._measurement == "test")`)
	if err != nil {
		t.Error(err)
	}
	fmt.Println("QueryResult:")
	fmt.Println(res)
}

func TestQueryRaw(t *testing.T) {
	client := NewInfluxDBClient("http://localhost:9999", token())

	queryApi := client.QueryAPI("my-org")
	fmt.Println("QueryResult")
	result, err := queryApi.QueryRaw(`from(bucket:"my-bucket")|> range(start: -24h) |> filter(fn: (r) => r._measurement == "test")|> yield(name: "xxx")`)
	if err != nil {
		t.Error(err)
	}
	for i := 0; result.Next(); i++ {
		fmt.Print(i)
		fmt.Print(":")
		fmt.Println(result.Row())
	}
	if result.Err() != nil {
		t.Error(err)
	}
}

func TestQuery(t *testing.T) {

	client := NewInfluxDBClient("http://localhost:9999", token())

	queryApi := client.QueryAPI("my-org")
	fmt.Println("QueryResult")
	result, err := queryApi.Query(`from(bucket:"my-bucket")|> range(start: -24h) |> filter(fn: (r) => r._measurement == "test")`)
	if err != nil {
		t.Error(err)
	} else {

		lastTable := -1
		for result.Next() {
			if lastTable != result.TableIndex() {
				fmt.Printf("%#v\n", result.Table())
				lastTable = result.TableIndex()
			}
			fmt.Printf("%#v\n", result.Record())
		}
		if result.Err() != nil {
			t.Error(result.Err())
		}
	}
}

func token() string {
	if authToken == "" {
		authToken = "i3EE29aNIQqWPFHzUUTzvp83gZiro1ZQuAXFlESch2dG_8YUUlDWqtKSxwdMu7xXtlBDo9e1UHZ976GCmJ4PFg=="
	}
	return authToken
}
