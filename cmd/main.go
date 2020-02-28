package main

import (
	"errors"
	"flag"
	"fmt"
	clientb "github.com/bonitoo-io/influxdb-client-go"
	"sync"
	"time"
)

const (
	InfluxDB2Url    = "http://localhost:9999"
	InfluxDB2Bucket = "my-bucket"
	InfluxDB2Org    = "my-org"
	InfluxDB2Token  = "my-token"
)

type Writer interface {
	Write(id int, measurementName string, iteration int)
	Count(measurementName string) (int, error)
	Close() error
}

type WriterV2P struct {
	influx   *clientb.InfluxDBClient
	writeApi clientb.WriteApi
}
type WriterV2R struct {
	influx   *clientb.InfluxDBClient
	writeApi clientb.WriteApi
}

func main() {
	writerType := flag.String("type", "lines", "Type of writer (lines, points)")
	threadsCount := flag.Int("threadsCount", 2000, "how much Thread use to write into InfluxDB")
	secondsCount := flag.Int("secondsCount", 30, "how long write into InfluxDB")
	lineProtocolsCount := flag.Int("lineProtocolsCount", 100, "how much data writes in one batch")
	skipCount := flag.Bool("skipCount", false, "skip counting count")
	measurementName := flag.String("measurementName", fmt.Sprintf("sensor_%d", time.Now().UnixNano()), "writer measure destination")
	debugLevel := flag.Int("debugLevel", 0, "Log messages level: 0 - error, 1 - warning, 2 - info, 3 - debug")
	flag.Parse()

	expected := (*threadsCount) * (*secondsCount) * (*lineProtocolsCount)

	//green := color.New(color.FgHiGreen).SprintFunc()
	fmt.Println()
	fmt.Printf("------------- %s -------------", *writerType)
	fmt.Println()
	fmt.Println()
	fmt.Println("measurement:        ", *measurementName)
	fmt.Println("threadsCount:       ", *threadsCount)
	fmt.Println("secondsCount:       ", *secondsCount)
	fmt.Println("lineProtocolsCount: ", *lineProtocolsCount)
	fmt.Println()
	fmt.Println("expected size: ", expected)
	fmt.Println()

	var writer Writer
	influx := clientb.NewInfluxDBClientWithOptions(InfluxDB2Url, InfluxDB2Token, clientb.Options{
		BatchSize:     5000,
		Debug:         *debugLevel,
		RetryInterval: 30,
		FlushInterval: 1000,
	})
	if *writerType == "points" {
		writer = &WriterV2P{influx: influx, writeApi: influx.WriteAPI(InfluxDB2Org, InfluxDB2Bucket)}
	} else if *writerType == "lines" {
		writer = &WriterV2R{influx: influx, writeApi: influx.WriteAPI(InfluxDB2Org, InfluxDB2Bucket)}
	} else {
		fmt.Printf("unsupported client %s\n", *writerType)
		return
	}

	stopExecution := make(chan bool)
	var wg sync.WaitGroup
	wg.Add(*threadsCount)

	start := time.Now()

	for i := 1; i <= *threadsCount; i++ {
		go doLoad(&wg, stopExecution, i, *measurementName, *secondsCount, *lineProtocolsCount, writer)
	}

	go func() {
		time.Sleep(time.Duration(*secondsCount) * time.Second)
		fmt.Printf("\n\nThe time: %v seconds elapsed! Stopping all writers\n\n", *secondsCount)
		close(stopExecution)
	}()

	wg.Wait()

	if !*skipCount {
		fmt.Println()
		fmt.Println()
		fmt.Println("Querying InfluxDB ...")
		fmt.Println()

		total, err := writer.Count(*measurementName)
		if err != nil {
			panic(err)
		}
		fmt.Println("Results:")
		fmt.Println("-> expected:        ", expected)
		fmt.Println("-> total:           ", total)
		fmt.Println("-> rate [%]:        ", (float64(total)/float64(expected))*100)
		fmt.Println("-> rate [msg/sec]:  ", total / *secondsCount)
		fmt.Println()
		fmt.Println("Total time:", time.Since(start))
	}

	if err := writer.Close(); err != nil {
		panic(err)
	}
}

func doLoad(wg *sync.WaitGroup, stopExecution <-chan bool, id int, measurementName string, secondsCount int, lineProtocolsCount int, influx Writer) {
	defer wg.Done()

	for i := 1; i <= secondsCount; i++ {
		select {
		case <-stopExecution:
			return
		default:

			if id == 1 {
				fmt.Printf("\rwriting iterations: %v/%v", i, secondsCount)
			}

			start := i * lineProtocolsCount
			end := start + lineProtocolsCount
			for j := start; j < end; j++ {
				select {
				case <-stopExecution:
					return
				default:
					influx.Write(id, measurementName, j)
				}
			}
			time.Sleep(1 * time.Second)
		}
	}
}

func (w *WriterV2R) Write(id int, measurementName string, iteration int) {
	record := fmt.Sprintf("%s,id=%d temperature=%d %d", measurementName, id, time.Now().UnixNano(), time.Unix(0, int64(iteration)).UnixNano())
	w.writeApi.WriteRecord(record)
	//fmt.Println(record)
}

func (w *WriterV2R) Count(measurementName string) (int, error) {
	query := `from(bucket:"` + InfluxDB2Bucket + `") 
		|> range(start: 0, stop: now()) 
		|> filter(fn: (r) => r._measurement == "` + measurementName + `") 
		|> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
		|> drop(columns: ["id", "host"])
		|> count(column: "temperature")`

	queryApi := w.influx.QueryAPI(InfluxDB2Org)
	queryResult, err := queryApi.Query(query)
	if err != nil {
		return 0, err
	}
	total := 0
	if !queryResult.Next() {
		if queryResult.Err() != nil {
			return 0, queryResult.Err()
		} else {
			return 0, errors.New("unknown error")
		}
	} else {
		total = int(queryResult.Record().ValueByKey("temperature").(int64))
	}
	return total, nil
}

func (w *WriterV2R) Close() error {
	w.influx.Close()
	return nil
}

func (w *WriterV2P) Write(id int, measurementName string, iteration int) {
	point := clientb.NewPoint(
		map[string]interface{}{"temperature": fmt.Sprintf("%v", time.Now().UnixNano())},
		measurementName,
		map[string]string{"id": fmt.Sprintf("%v", id)},
		time.Unix(0, int64(iteration)))
	w.writeApi.Write(point)
}

func (w *WriterV2P) Count(measurementName string) (int, error) {
	query := `from(bucket:"` + InfluxDB2Bucket + `") 
		|> range(start: 0, stop: now()) 
		|> filter(fn: (r) => r._measurement == "` + measurementName + `") 
		|> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
		|> drop(columns: ["id", "host"])
		|> count(column: "temperature")`

	queryApi := w.influx.QueryAPI(InfluxDB2Org)
	queryResult, err := queryApi.Query(query)
	if err != nil {
		return 0, err
	}
	total := 0
	if !queryResult.Next() {
		if queryResult.Err() != nil {
			return 0, queryResult.Err()
		} else {
			return 0, errors.New("unknown error")
		}
	} else {
		total = int(queryResult.Record().ValueByKey("temperature").(int64))
	}
	return total, nil
}

func (w *WriterV2P) Close() error {
	w.influx.Close()
	return nil
}
