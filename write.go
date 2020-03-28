package client

import (
	"bytes"
	"container/list"
	"io"
	"log"
	"net/http"
	"net/url"
	"path"
	"strings"
	"time"

	"github.com/bonitoo-io/influxdb-client-go/internal/gzip"
	lp "github.com/influxdata/line-protocol"
)

type WriteApi interface {
	WriteRecord(line string)
	Write(point *Point)
	Flush()
	close()
}

type writeApiImpl struct {
	org              string
	bucket           string
	client           InfluxDBClient
	writeBuffer      []string
	retryQueue       *queue
	url              string
	writeCh          chan *batch
	bufferCh         chan string
	writeStop        chan int
	bufferStop       chan int
	bufferFlush      chan int
	doneCh           chan int
	retryBufferLimit uint
	lastWriteAttempt time.Time
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
	retryBufferLimit := client.Options().RetryBufferLimit / client.Options().BatchSize
	if retryBufferLimit == 0 {
		retryBufferLimit = 1
	}
	w := &writeApiImpl{org: org,
		bucket:           bucket,
		client:           client,
		writeBuffer:      make([]string, 0, client.Options().BatchSize+1),
		retryQueue:       newQueue(int(retryBufferLimit)),
		writeCh:          make(chan *batch),
		doneCh:           make(chan int),
		bufferCh:         make(chan string),
		bufferStop:       make(chan int),
		writeStop:        make(chan int),
		bufferFlush:      make(chan int),
		retryBufferLimit: retryBufferLimit,
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
		if w.client.Options().Debug > 1 {
			log.Println("I! Waiting buffer is flushed")
		}
		time.Sleep(time.Millisecond)
	}
	for len(w.writeCh) > 0 {
		if w.client.Options().Debug > 1 {
			log.Println("I! Waiting buffer is written")
		}
		time.Sleep(time.Millisecond)
	}
	time.Sleep(time.Millisecond)
}

func (w *writeApiImpl) bufferProc() {
	if w.client.Options().Debug > 1 {
		log.Println("I! Buffer proc started")
	}
	ticker := time.NewTicker(time.Duration(w.client.Options().FlushInterval) * time.Millisecond)
x:
	for {
		select {
		case line := <-w.bufferCh:
			w.writeBuffer = append(w.writeBuffer, line)
			if len(w.writeBuffer) == int(w.client.Options().BatchSize) {
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
	if w.client.Options().Debug > 1 {
		log.Println("I! Buffer proc finished")
	}
	w.doneCh <- 1
}

func (w *writeApiImpl) flushBuffer() {
	if len(w.writeBuffer) > 0 {
		//go func(lines []string) {
		if w.client.Options().Debug > 1 {
			log.Println("I! sending batch")
		}
		batch := &batch{batch: buffer(w.writeBuffer)}
		w.writeCh <- batch
		//	lines = lines[:0]
		//}(w.writeBuffer)
		//w.writeBuffer = make([]string,0, w.client.options.BatchSize+1)
		w.writeBuffer = w.writeBuffer[:0]
	}
}

func (w *writeApiImpl) writeProc() {
	if w.client.Options().Debug > 1 {
		log.Println("I! Write proc started")
	}
	var err error
x:
	for {
		select {
		case batch := <-w.writeCh:
			if w.client.Options().Debug > 2 {
				log.Println("D! Write proc: received write request")
			}
			batchToWrite := batch
			retrying := false
			err = nil
		r:
			if !w.retryQueue.isEmpty() {
				if w.client.Options().Debug > 2 {
					log.Println("D! Write proc: taking batch from retry queue")
				}
				if !retrying {
					b := w.retryQueue.first()
					// Can we write? In case of retryable error we must wait a bit
					if w.lastWriteAttempt.IsZero() || time.Now().After(w.lastWriteAttempt.Add(time.Second*time.Duration(b.retryInterval))) {
						retrying = true
					} else {
						if w.client.Options().Debug > 0 {
							log.Println("W! Write proc: cannot write yet, storing batch to queue")
						}
						w.retryQueue.push(batch)
						batchToWrite = nil
					}
				}
				if retrying {
					batchToWrite = w.retryQueue.pop()
					batchToWrite.retries++
					if batch != nil {
						if w.retryQueue.push(batch) {
							if w.client.Options().Debug > 0 {
								log.Println("W! Write proc: Retry buffer full, discarding oldest batch")
							}
						}
						batch = nil
					}
				}
			}
			if batchToWrite != nil {
				err = w.write(batchToWrite)
				batchToWrite = nil
				if err != nil {
					retrying = false
				} else {
					goto r
				}

			}
		case <-w.writeStop:
			if w.client.Options().Debug > 1 {
				log.Println("I! Write proc: received stop")
			}
			break x
		}
	}
	if w.client.Options().Debug > 1 {
		log.Println("I! Write proc finished")
	}
	w.doneCh <- 1
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

func (w *writeApiImpl) write(batch *batch) error {
	url, err := w.writeUrl()
	if err != nil {
		log.Printf("E! %s\n", err.Error())
		return err
	}
	var body io.Reader
	body = strings.NewReader(batch.batch)
	if w.client.Options().Debug > 2 {
		log.Printf("D! Writing batch: %s", batch.batch)
	}
	if w.client.Options().UseGZip {
		body, err = gzip.CompressWithGzip(body, 6)
		if err != nil {
			return err
		}
	}
	w.lastWriteAttempt = time.Now()
	error := w.client.postRequest(url, body, func(req *http.Request) {
		if w.client.Options().UseGZip {
			req.Header.Set("Content-Encoding", "gzip")
		}
	}, nil)
	if error != nil {
		if error.StatusCode == http.StatusTooManyRequests || error.StatusCode == http.StatusServiceUnavailable {
			log.Printf("E! Write error: %s\nBatch kept for retrying\n", error.Error())
			if error.RetryAfter > 0 {
				batch.retryInterval = error.RetryAfter
			}
			if batch.retries < w.client.Options().MaxRetries {
				if w.retryQueue.push(batch) && w.client.Options().Debug > 0 {
					log.Println("W! Retry buffer full, discarding oldest batch")
				}
			}
		} else {
			log.Printf("E! Write error: %s\n", error.Error())
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
	//w.bufferCh <- point.ToLineProtocol(w.client.Options().Precision)
	var buffer bytes.Buffer
	e := lp.NewEncoder(&buffer)
	e.SetFieldTypeSupport(lp.UintSupport)
	e.FailOnFieldErr(true)
	e.SetPrecision(w.client.Options().Precision)
	_, err := e.Encode(point)
	if err != nil {
		log.Printf("[E] point encoding error: %s\n", err.Error())
	} else {
		w.bufferCh <- buffer.String()
	}
}

func (w *writeApiImpl) writeUrl() (string, error) {
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
