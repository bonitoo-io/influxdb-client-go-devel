package client

// TODO
// - flush proc
// - retry on error
import (
	"log"
	"net/url"
	"path"
	"strings"
	"time"
)

type WriteApi interface {
	WriteRecord(line string)
	Flush() error
	close()
}

type writeApiImpl struct {
	org         string
	bucket      string
	client      *InfluxDBClient
	writeBuffer []string
	retryBuffer []*batch
	url         string
	writeCh     chan *batch
	tickerStop  chan int
	doneCh      chan int
}

type batch struct {
	batch   string
	retries int
}

func newWriteApiImpl(org string, bucket string, client *InfluxDBClient) *writeApiImpl {
	w := &writeApiImpl{org: org,
		bucket:      bucket,
		client:      client,
		writeBuffer: make([]string, 0, client.options.BatchSize+1),
		retryBuffer: make([]*batch, 0, 10),
		writeCh:     make(chan *batch),
		tickerStop:  make(chan int),
		doneCh:      make(chan int),
	}

	go w.writeProc()
	go w.retryProc()
	return w
}
func buffer(lines []string) *strings.Builder {
	var builder strings.Builder
	for _, line := range lines {
		builder.WriteString(line)
		builder.WriteString("\n")
	}
	return &builder
}

func (w *writeApiImpl) Flush() error {
	if len(w.writeBuffer) > 0 {
		batch := &batch{batch: buffer(w.writeBuffer).String()}
		w.writeCh <- batch
		w.writeBuffer = w.writeBuffer[:0]
	}
	return nil
}

func (w *writeApiImpl) writeProc() {
	if w.client.options.Debug > 1 {
		log.Println("I! Write proc started")
	}
	for batch := range w.writeCh {
		w.write(batch)
	}
	if w.client.options.Debug > 1 {
		log.Println("I! Write proc finished")
	}
	w.doneCh <- 1
}

func (w *writeApiImpl) retryProc() {
	if w.client.options.Debug > 1 {
		log.Println("I! Retry proc started")
	}
	ticker := time.NewTicker(time.Second * time.Duration(w.client.options.RetryInterval))

x:
	for {
		select {
		case <-ticker.C:
			//w.writeCh <- batch
		case <-w.tickerStop:
			ticker.Stop()
			break x
		}
	}

	if w.client.options.Debug > 1 {
		log.Println("I! Retry proc finished")
	}
	w.doneCh <- 1
}

func (w *writeApiImpl) close() {
	// Flush outstanding metrics
	w.Flush()
	for len(w.writeCh) > 0 {
		if w.client.options.Debug > 1 {
			log.Printf("I! Waiting for outstanding batches: %d\n", len(w.writeCh))
		}
		time.Sleep(time.Second)
	}
	close(w.writeCh)
	w.tickerStop <- 1
	close(w.tickerStop)
	//wait for write proc and retry proc
	<-w.doneCh
	<-w.doneCh
}

func (w *writeApiImpl) write(batch *batch) error {
	url, err := w.writeUrl()
	if err != nil {
		log.Printf("E! %s\n", err.Error())
		return err
	}
	if w.client.options.Debug > 2 {
		log.Printf("D! Writing batch: %s", batch.batch)
	}
	err = w.client.postRequest(url, strings.NewReader(batch.batch), nil, nil)
	if err != nil {
		if w.client.options.Debug > 0 {
			log.Printf("W! Write error: %s\nBatch kept for retrying\n", err.Error())
		}
		//
		if batch.retries < w.client.options.MaxRetries {
			w.retryBuffer = append(w.retryBuffer, batch)
		}
		return err
	}
	return nil
}

func (w *writeApiImpl) WriteRecord(line string) {
	w.writeBuffer = append(w.writeBuffer, line)
	if len(w.writeBuffer) == w.client.options.BatchSize {
		batch := &batch{batch: buffer(w.writeBuffer).String()}
		w.writeCh <- batch
		w.writeBuffer = w.writeBuffer[:0]
	}
}

func (w *writeApiImpl) writeUrl() (string, error) {
	if w.url == "" {
		u, err := url.Parse(w.client.serverUrl)
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
