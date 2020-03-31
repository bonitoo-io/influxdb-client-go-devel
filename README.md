# InfluxDB Client Go

[![CircleCI](https://circleci.com/gh/bonitoo-io/influxdb-client-go.svg?style=svg)](https://circleci.com/gh/bonitoo-io/influxdb-client-go)
[![codecov](https://codecov.io/gh/bonitoo-io/influxdb-client-go/branch/master/graph/badge.svg)](https://codecov.io/gh/bonitoo-io/influxdb-client-go)
[![License](https://img.shields.io/github/license/bonitoo-io/influxdb-client-go.svg)](https://github.com/bonitoo-io/influxdb-client-go/blob/master/LICENSE)

This repository contains the reference Go client for InfluxDB 2.

## Features

- InfluxDB 2 client
    - Querying data 
        - using the Flux language
        - into raw data, flux table representation
    - Writing data using
        - [Line Protocol](https://docs.influxdata.com/influxdb/v1.6/write_protocols/line_protocol_tutorial/) 
        - [Data Point](https://github.com/bonitoo-io/influxdb-client-go/blob/master/point.go)
        - both [asynchronous](https://github.com/bonitoo-io/influxdb-client-go/blob/master/write.go) or [synchronous](https://github.com/bonitoo-io/influxdb-client-go/blob/master/writeApiBlocking.go) manners
    - InfluxDB 2 API
        - setup
        - ready
     
## Installation
**Go 1.3** or later is required.

Add import to your source or directly edit go.mod

## Usage
```go
    import github.com/bonitoo-io/influxdb-client-go
```

```go
    client := NewInfluxDBClient("http://localhost:9999", token())
    writeApi := client.WriteApiBlocking("my-org", "my-bucket")
    p := NewPoint("stat",
        map[string]string{"unit": "temperature"},
        map[string]interface{}{"avg": strconv.FormatFloat(f, 'f', 2, 64), "max": 45},
    	time.Now())
    writeApi.WritePoint(context.Background(), p)
    
    queryApi := client.QueryApi("my-org")
    result, err := queryApi.Query(context.Background(), `from(bucket:"my-bucket")|> range(start: -24h) |> filter(fn: (r) => r._measurement == "stat")`)
    if err == nil {
        for result.Next() {
            if result.TableChanged() {
                fmt.Printf("%#v\n", result.TableMetadata())
            }
            fmt.Printf("%#v\n", result.Record())
        }
        if result.Err() != nil {
            fmt.Printf("Query error: %s\n", result.Err().Error())
        }
    }
```

## Contributing

If you would like to contribute code you can do through GitHub by forking the repository and sending a pull request into the `master` branch.

## License

The InfluxDB 2 Go Client is released under the [MIT License](https://opensource.org/licenses/MIT).






