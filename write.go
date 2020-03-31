// Copyright 2020 InfluxData, Inc. All rights reserved.
// Use of this source code is governed by MIT
// license that can be found in the LICENSE file.

package influxdb2

import (
	"context"
	"strings"
	"time"
)

// WriteApiBlocking is Write client interface with non-blocking methods for writing time series data asynchronously in batches into an InfluxDB server.
type WriteApi interface {
	// WriteRecord writes asynchronously line protocol record into bucket.
	// WriteRecord adds record into the buffer which is sent on the background when it reaches the batch size.
	// Blocking alternative is available in the WriteApiBlocking interface
	WriteRecord(line string)
	// WritePoint writes asynchronously Point into bucket.
	// WritePoint adds Point into the buffer which is sent on the background when it reaches the batch size.
	// Blocking alternative is available in the WriteApiBlocking interface
	WritePoint(point *Point)
	// Flush forces all pending writes from the buffer to be sent
	Flush()
	// Flushes all pending writes and stop async processes. After this the Write client cannot be used
	Close()
	// Errors return channel for reading errors which occurs during async writes
	Errors() <-chan error
}

type writeApiImpl struct {
	service     *writeService
	writeBuffer []string

	url         string
	writeCh     chan *batch
	bufferCh    chan string
	writeStop   chan int
	bufferStop  chan int
	bufferFlush chan int
	doneCh      chan int
	errCh       chan error
}

func newWriteApiImpl(org string, bucket string, client InfluxDBClient) *writeApiImpl {
	w := &writeApiImpl{
		service:     newWriteService(org, bucket, client),
		writeBuffer: make([]string, 0, client.Options().BatchSize+1),
		writeCh:     make(chan *batch),
		doneCh:      make(chan int),
		bufferCh:    make(chan string),
		bufferStop:  make(chan int),
		writeStop:   make(chan int),
		bufferFlush: make(chan int),
	}
	go w.bufferProc()
	go w.writeProc()

	return w
}

func (w *writeApiImpl) Errors() <-chan error {
	if w.errCh == nil {
		w.errCh = make(chan error)
	}
	return w.errCh
}

func (w *writeApiImpl) Flush() {
	w.bufferFlush <- 1
	w.waitForFlushing()
}

func (w *writeApiImpl) waitForFlushing() {
	for len(w.writeBuffer) > 0 {
		logger.Info("Waiting buffer is flushed")
		time.Sleep(time.Millisecond)
	}
	for len(w.writeCh) > 0 {
		logger.Info("Waiting buffer is written")
		time.Sleep(time.Millisecond)
	}
	//HACK: wait a bit till write finishes
	time.Sleep(time.Millisecond)
}

func (w *writeApiImpl) bufferProc() {
	logger.Info("Buffer proc started")
	ticker := time.NewTicker(time.Duration(w.service.client.Options().FlushInterval) * time.Millisecond)
x:
	for {
		select {
		case line := <-w.bufferCh:
			w.writeBuffer = append(w.writeBuffer, line)
			if len(w.writeBuffer) == int(w.service.client.Options().BatchSize) {
				w.flushBuffer()
			}
		case <-ticker.C:
			w.flushBuffer()
		case <-w.bufferFlush:
			w.flushBuffer()
		case <-w.bufferStop:
			ticker.Stop()
			w.flushBuffer()
			break x
		}
	}
	logger.Info("Buffer proc finished")
	w.doneCh <- 1
}

func (w *writeApiImpl) flushBuffer() {
	if len(w.writeBuffer) > 0 {
		//go func(lines []string) {
		logger.Info("sending batch")
		batch := &batch{batch: buffer(w.writeBuffer)}
		w.writeCh <- batch
		//	lines = lines[:0]
		//}(w.writeBuffer)
		//w.writeBuffer = make([]string,0, w.service.client.Options.BatchSize+1)
		w.writeBuffer = w.writeBuffer[:0]
	}
}

func (w *writeApiImpl) writeProc() {
	logger.Info("Write proc started")
x:
	for {
		select {
		case batch := <-w.writeCh:
			err := w.service.handleWrite(context.Background(), batch)
			if w.errCh != nil && err != nil {
				w.errCh <- err
			}
		case <-w.writeStop:
			logger.Info("Write proc: received stop")
			break x
		}
	}
	logger.Info("Write proc finished")
	w.doneCh <- 1
}

func (w *writeApiImpl) Close() {
	// Flush outstanding metrics
	w.Flush()
	w.bufferStop <- 1
	//wait for buffer proc
	<-w.doneCh
	close(w.bufferStop)
	close(w.bufferFlush)
	w.writeStop <- 1
	<-w.doneCh
	close(w.writeCh)
	//wait for write  and buffer proc
}

func (w *writeApiImpl) WriteRecord(line string) {
	b := []byte(line)
	b = append(b, 0xa)
	w.bufferCh <- string(b)
}

func (w *writeApiImpl) WritePoint(point *Point) {
	//w.bufferCh <- point.ToLineProtocol(w.service.client.Options().Precision)
	line, err := w.service.encodePoints(point)
	if err != nil {
		logger.Errorf("point encoding error: %s\n", err.Error())
	} else {
		w.bufferCh <- line
	}
}

func buffer(lines []string) string {
	return strings.Join(lines, "")
}
