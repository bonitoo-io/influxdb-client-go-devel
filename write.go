package client

// TODO
// - flush proc
// - retry on error
import (
	"github.com/bonitoo-io/influxdb-client-go/internal/gzip"
	"io"
	"log"
	"net/http"
	"net/url"
	"path"
	"strings"
	"time"
)

type WriteApi interface {
	WriteRecord(line string)
	Write(point *Point)
	Flush()
	close()
}

type writeApiImpl struct {
	org         string
	bucket      string
	client      InfluxDBClient
	writeBuffer []string
	retryBuffer []*batch
	url         string
	writeCh     chan *batch
	bufferCh    chan string
	retryStop   chan int
	bufferStop  chan int
	bufferFlush chan int
	doneCh      chan int
}

type batch struct {
	batch   string
	retries int
}

func newWriteApiImpl(org string, bucket string, client InfluxDBClient) *writeApiImpl {
	w := &writeApiImpl{org: org,
		bucket:      bucket,
		client:      client,
		writeBuffer: make([]string, 0, client.Options().BatchSize+1),
		retryBuffer: make([]*batch, 0, 10),
		writeCh:     make(chan *batch),
		retryStop:   make(chan int),
		doneCh:      make(chan int),
		bufferCh:    make(chan string),
		bufferStop:  make(chan int),
		bufferFlush: make(chan int),
	}
	go w.bufferProc()
	go w.writeProc()
	go w.retryProc()

	return w
}
func buffer(lines []string) string {
	return strings.Join(lines, "")
}

func (w *writeApiImpl) Flush() {
	w.bufferFlush <- 1
	for len(w.writeBuffer) > 0 {
		if w.client.Options().Debug > 1 {
			log.Println("I! Waiting buffer is flushed")
		}
		time.Sleep(time.Millisecond)
	}
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
			if len(w.writeBuffer) == w.client.Options().BatchSize {
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
			log.Println("I! Writing batch")
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
	for batch := range w.writeCh {
		w.write(batch)
	}
	if w.client.Options().Debug > 1 {
		log.Println("I! Write proc finished")
	}
	w.doneCh <- 1
}

func (w *writeApiImpl) retryProc() {
	if w.client.Options().Debug > 1 {
		log.Println("I! Retry proc started")
	}
	ticker := time.NewTicker(time.Second * time.Duration(w.client.Options().RetryInterval))
x:
	for {
		select {
		case <-ticker.C:
			//w.writeCh <- batch
		case <-w.retryStop:
			ticker.Stop()
			break x
		}
	}

	if w.client.Options().Debug > 1 {
		log.Println("I! Retry proc finished")
	}
	w.doneCh <- 1
}

func (w *writeApiImpl) close() {
	// Flush outstanding metrics
	w.Flush()
	w.bufferStop <- 1
	// TODO: let procs finish on close request, than close channels
	for len(w.writeCh) > 0 {
		if w.client.Options().Debug > 1 {
			log.Printf("I! Waiting for outstanding batches: %d\n", len(w.writeCh))
		}
		time.Sleep(time.Millisecond)
	}
	close(w.bufferStop)
	close(w.bufferFlush)
	close(w.writeCh)
	w.retryStop <- 1
	close(w.retryStop)
	//wait for write proc, retry and buffer proc
	<-w.doneCh
	<-w.doneCh
	<-w.doneCh
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
	err = w.client.postRequest(url, body, func(req *http.Request) {
		if w.client.Options().UseGZip {
			req.Header.Set("Content-Encoding", "gzip")
		}
	}, nil)
	if err != nil {
		if w.client.Options().Debug > 0 {
			log.Printf("W! Write error: %s\nBatch kept for retrying\n", err.Error())
		}
		//
		if batch.retries < w.client.Options().MaxRetries {
			w.retryBuffer = append(w.retryBuffer, batch)
		}
		return err
	}
	return nil
}

func (w *writeApiImpl) WriteRecord(line string) {
	b := []byte(line)
	b = append(b, 0xa)
	w.bufferCh <- string(b)
}

func (w *writeApiImpl) Write(point *Point) {
	w.bufferCh <- point.ToLineProtocol(w.client.Options().Precision)
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
		u.RawQuery = params.Encode()
		w.url = u.String()
	}
	return w.url, nil
}
