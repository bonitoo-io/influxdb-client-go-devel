// Copyright 2020 InfluxData, Inc. All rights reserved.
// Use of this source code is governed by MIT
// license that can be found in the LICENSE file.

package influxdb2

import (
	"context"
	"github.com/stretchr/testify/assert"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestUserAgent(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(100 * time.Millisecond)
		if r.Header.Get("User-Agent") == userAgent() {
			w.WriteHeader(http.StatusOK)
		} else {
			w.WriteHeader(http.StatusNotFound)
		}
	}))

	defer server.Close()
	c := NewInfluxDBClientWithToken(server.URL, "x")
	ready, err := c.Ready(context.Background())
	assert.True(t, ready)
	assert.Nil(t, err)

	err = c.WriteApiBlocking("o", "b").WriteRecord(context.Background(), "a,a=a a=1i")
	assert.Nil(t, err)
}
