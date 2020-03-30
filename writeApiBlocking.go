package client

import (
	"strings"
)

// WriteApiBlocking offers blocking methods for writing time series data synchronously into an InfluxDB server.
type WriteApiBlocking interface {
	// WriteRecord writes line protocol record(s) into specified bucket.
	// WriteRecord writes without implicit batching. Batch is created from given number of arguments
	// Non-blocking alternative is available in the WriteApi interface
	WriteRecord(line ...string) error
	// WritePoint data point into specified bucket.
	// WritePoint writes without implicit batching. Batch is created from given number of arguments
	// Non-blocking alternative is available in the WriteApi interface
	WritePoint(point ...*Point) error
}

type writeApiBlockingImpl struct {
	service *writeService
}

func newWriteApiBlockingImpl(org string, bucket string, client InfluxDBClient) *writeApiBlockingImpl {
	return &writeApiBlockingImpl{service: newWriteService(org, bucket, client)}
}

func (w *writeApiBlockingImpl) write(line string) error {
	err := w.service.writeBatch(&batch{
		batch:         line,
		retryInterval: w.service.client.Options().RetryInterval,
	})
	return err
}

func (w *writeApiBlockingImpl) WriteRecord(line ...string) error {
	if len(line) > 0 {
		var sb strings.Builder
		for _, line := range line {
			b := []byte(line)
			b = append(b, 0xa)
			if _, err := sb.Write(b); err != nil {
				return err
			}
		}
		return w.write(sb.String())
	}
	return nil
}

func (w *writeApiBlockingImpl) WritePoint(point ...*Point) error {
	line, err := w.service.encodePoints(point...)
	if err != nil {
		return err
	}
	return w.write(line)
}
