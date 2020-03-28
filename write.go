package client

import (
	"bytes"
	"container/list"
	"io"
	"net/http"
	"net/url"
	"path"
	"strings"
	"time"

	"github.com/bonitoo-io/influxdb-client-go/internal/gzip"
	"github.com/bonitoo-io/influxdb-client-go/internal/log"
	lp "github.com/influxdata/line-protocol"
)

var logger log.Logger

type WriteApi interface {
	WriteRecord(line string)
	Write(point *Point)
	Flush()
	close()
}

type writeService struct {
	org              string
	bucket           string
	client           InfluxDBClient
	url              string
	lastWriteAttempt time.Time
	retryQueue       *queue
}

func newWriteService(org string, bucket string, client InfluxDBClient) *writeService {
	retryBufferLimit := client.Options().RetryBufferLimit / client.Options().BatchSize
	if retryBufferLimit == 0 {
		retryBufferLimit = 1
	}
	return &writeService{org: org, bucket: bucket, client: client, retryQueue: newQueue(int(retryBufferLimit))}
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
}

type batch struct {
	batch         string
	retryInterval uint
	retries       uint
}

type queue struct {
	list  *list.List
	limit int
}

func newQueue(limit int) *queue {
	return &queue{list: list.New(), limit: limit}
}
func (q *queue) push(batch *batch) bool {
	overWrite := false
	if q.list.Len() == q.limit {
		q.pop()
		overWrite = true
	}
	q.list.PushBack(batch)
	return overWrite
}

func (q *queue) pop() *batch {
	el := q.list.Front()
	if el != nil {
		q.list.Remove(el)
		return el.Value.(*batch)
	}
	return nil
}

func (q *queue) first() *batch {
	el := q.list.Front()
	return el.Value.(*batch)
}

func (q *queue) isEmpty() bool {
	return q.list.Len() == 0
}

func newWriteApiImpl(org string, bucket string, client InfluxDBClient) *writeApiImpl {
	logger.SetDebugLevel(client.Options().Debug)
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
func buffer(lines []string) string {
	return strings.Join(lines, "")
}

func (w *writeApiImpl) Flush() {
	w.bufferFlush <- 1
	w.waitForFlushing()
}

func (w *writeApiImpl) waitForFlushing() {
	for len(w.writeBuffer) > 0 {
		logger.InfoLn("Waiting buffer is flushed")
		time.Sleep(time.Millisecond)
	}
	for len(w.writeCh) > 0 {
		logger.InfoLn("Waiting buffer is written")
		time.Sleep(time.Millisecond)
	}
	time.Sleep(time.Millisecond)
}

func (w *writeApiImpl) bufferProc() {
	logger.InfoLn("Buffer proc started")
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
	logger.InfoLn("Buffer proc finished")
	w.doneCh <- 1
}

func (w *writeApiImpl) flushBuffer() {
	if len(w.writeBuffer) > 0 {
		//go func(lines []string) {
		logger.InfoLn("sending batch")
		batch := &batch{batch: buffer(w.writeBuffer)}
		w.writeCh <- batch
		//	lines = lines[:0]
		//}(w.writeBuffer)
		//w.writeBuffer = make([]string,0, w.service.client.Options.BatchSize+1)
		w.writeBuffer = w.writeBuffer[:0]
	}
}

func (w *writeApiImpl) writeProc() {
	logger.InfoLn("Write proc started")
x:
	for {
		select {
		case batch := <-w.writeCh:
			w.service.handleWrite(batch)
		case <-w.writeStop:
			logger.InfoLn("Write proc: received stop")
			break x
		}
	}
	logger.InfoLn("Write proc finished")
	w.doneCh <- 1
}

func (w *writeService) handleWrite(batch *batch) error {
	logger.DebugLn("Write proc: received write request")
	batchToWrite := batch
	retrying := false
	for {
		if !w.retryQueue.isEmpty() {
			logger.DebugLn("Write proc: taking batch from retry queue")
			if !retrying {
				b := w.retryQueue.first()
				// Can we write? In case of retryable error we must wait a bit
				if w.lastWriteAttempt.IsZero() || time.Now().After(w.lastWriteAttempt.Add(time.Second*time.Duration(b.retryInterval))) {
					retrying = true
				} else {
					logger.WarnLn("Write proc: cannot write yet, storing batch to queue")
					w.retryQueue.push(batch)
					batchToWrite = nil
				}
			}
			if retrying {
				batchToWrite = w.retryQueue.pop()
				batchToWrite.retries++
				if batch != nil {
					if w.retryQueue.push(batch) {
						logger.WarnLn("Write proc: Retry buffer full, discarding oldest batch")
					}
					batch = nil
				}
			}
		}
		if batchToWrite != nil {
			err := w.writeBatch(batchToWrite)
			batchToWrite = nil
			if err != nil {
				return err
			}
		} else {
			break
		}
	}
	return nil
}

func (w *writeApiImpl) close() {
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

func (w *writeService) writeBatch(batch *batch) error {
	wUrl, err := w.writeUrl()
	if err != nil {
		logger.ErrorF("%s\n", err.Error())
		return err
	}
	var body io.Reader
	body = strings.NewReader(batch.batch)
	logger.DebugF("Writing batch: %s", batch.batch)
	if w.client.Options().UseGZip {
		body, err = gzip.CompressWithGzip(body, 6)
		if err != nil {
			return err
		}
	}
	w.lastWriteAttempt = time.Now()
	error := w.client.postRequest(wUrl, body, func(req *http.Request) {
		if w.client.Options().UseGZip {
			req.Header.Set("Content-Encoding", "gzip")
		}
	}, nil)
	if error != nil {
		if error.StatusCode == http.StatusTooManyRequests || error.StatusCode == http.StatusServiceUnavailable {
			logger.ErrorF("Write error: %s\nBatch kept for retrying\n", error.Error())
			if error.RetryAfter > 0 {
				batch.retryInterval = error.RetryAfter
			}
			if batch.retries < w.client.Options().MaxRetries {
				if w.retryQueue.push(batch) {
					logger.WarnLn("Retry buffer full, discarding oldest batch")
				}
			}
		} else {
			logger.ErrorF("Write error: %s\n", error.Error())
		}
		return error
	} else {
		w.lastWriteAttempt = time.Now()
	}
	return nil
}

func (w *writeApiImpl) WriteRecord(line string) {
	b := []byte(line)
	b = append(b, 0xa)
	w.bufferCh <- string(b)
}

func (w *writeApiImpl) Write(point *Point) {
	//w.bufferCh <- point.ToLineProtocol(w.service.client.Options().Precision)
	line, err := w.service.encodePoint(point)
	if err != nil {
		logger.ErrorF("point encoding error: %s\n", err.Error())
	} else {
		w.bufferCh <- line
	}
}

func (w *writeService) encodePoint(point *Point) (string, error) {
	var buffer bytes.Buffer
	e := lp.NewEncoder(&buffer)
	e.SetFieldTypeSupport(lp.UintSupport)
	e.FailOnFieldErr(true)
	e.SetPrecision(w.client.Options().Precision)
	_, err := e.Encode(point)
	if err != nil {
		return "", err
	}
	return buffer.String(), nil
}

func (w *writeService) writeUrl() (string, error) {
	if w.url == "" {
		u, err := url.Parse(w.client.ServerUrl())
		if err != nil {
			return "", err
		}
		u.Path = path.Join(u.Path, "/api/v2/write")

		params := u.Query()
		params.Set("org", w.org)
		params.Set("bucket", w.bucket)
		params.Set("precision", precisionToString(w.client.Options().Precision))
		u.RawQuery = params.Encode()
		w.url = u.String()
	}
	return w.url, nil
}

func precisionToString(precision time.Duration) string {
	prec := "ns"
	switch precision {
	case time.Microsecond:
		prec = "us"
	case time.Millisecond:
		prec = "ms"
	case time.Second:
		prec = "s"
	}
	return prec
}
