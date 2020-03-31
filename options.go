// Copyright 2020 InfluxData, Inc. All rights reserved.
// Use of this source code is governed by MIT
// license that can be found in the LICENSE file.

package influxdb2

import "time"

// Options holds configuration properties for communicating with InfluxDB server
type Options struct {
	// Maximum number of points sent to server in single request. Default 1000
	BatchSize uint
	// Interval, in ms, in which is buffer flushed if it has not been already written (by reaching batch size) . Default 1000ms
	FlushInterval uint
	// Default retry interval in ms, if not sent by server. Default 30s
	RetryInterval uint
	// Maximum count of retry attempts of failed writes
	MaxRetries uint
	// Maximum number of points to keep for retry. Should be multiple of BatchSize. Default 10,000
	RetryBufferLimit uint
	// DebugLevel to filter log messages. Each level mean to log all categories bellow. 0 error, 1 - warning, 2 - info, 3 - debug
	DebugLevel uint
	// Precision to use in writes for timestamp. In unit of duration: time.Nanosecond, time.Microsecond, time.Millisecond, time.Second
	// Default time.Nanosecond
	Precision time.Duration
	// Whether to use GZip compression in requests. Default false
	UseGZip bool
}

// DefaultOptions returns Options object with default values
func DefaultOptions() *Options {
	return &Options{BatchSize: 1000, MaxRetries: 3, RetryInterval: 1000, FlushInterval: 1000, Precision: time.Nanosecond, UseGZip: false, RetryBufferLimit: 10000}
}
