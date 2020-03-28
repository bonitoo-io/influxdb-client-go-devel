package client

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	lp "github.com/influxdata/line-protocol"
)

// Point is represents InfluxDB point, holding tags and fields
type Point struct {
	measurement string
	tags        []*lp.Tag
	fields      []*lp.Field
	timestamp   time.Time
}

// TagList returns a slice containing tags of a Metric.
func (m *Point) TagList() []*lp.Tag {
	return m.tags
}

// FieldList returns a slice containing the fields of a Metric.
func (m *Point) FieldList() []*lp.Field {
	return m.fields
}

// SetTime set timestamp for a Point.
func (m *Point) SetTime(timestamp time.Time) {
	m.timestamp = timestamp
}

// Time is the timestamp of a metric.
func (m *Point) Time() time.Time {
	return m.timestamp
}

// SortTags orders the tags of a metric alphanumerically by key.
// This is just here as a helper, to make it easy to keep tags sorted if you are creating a Point manually.
func (m *Point) SortTags() {
	sort.Slice(m.tags, func(i, j int) bool { return m.tags[i].Key < m.tags[j].Key })
}

// SortFields orders the fields of a metric alphanumerically by key.
func (m *Point) SortFields() {
	sort.Slice(m.fields, func(i, j int) bool { return m.fields[i].Key < m.fields[j].Key })
}

// AddTag adds an lp.Tag to a metric.
func (m *Point) AddTag(k, v string) {
	for i, tag := range m.tags {
		if k == tag.Key {
			m.tags[i].Value = v
			return
		}
	}
	m.tags = append(m.tags, &lp.Tag{Key: k, Value: v})
}

// AddField adds an lp.Field to a metric.
func (m *Point) AddField(k string, v interface{}) {
	for i, field := range m.fields {
		if k == field.Key {
			m.fields[i].Value = v
			return
		}
	}
	m.fields = append(m.fields, &lp.Field{Key: k, Value: convertField(v)})
}

// Name returns the name of the metric.
func (m *Point) Name() string {
	return m.measurement
}

// ToLineProtocol creates InfluxDB line protocol string from the Point, converting associated timestamp according to precision
// and write result to the string builder
func (m *Point) ToLineProtocolBuffer(sb *strings.Builder, precision time.Duration) {
	escapeKey(sb, m.Name())
	sb.WriteRune(',')
	for i, t := range m.tags {
		if i > 0 {
			sb.WriteString(",")
		}
		escapeKey(sb, t.Key)
		sb.WriteString("=")
		escapeKey(sb, t.Value)
	}
	sb.WriteString(" ")
	for i, f := range m.fields {
		if i > 0 {
			sb.WriteString(",")
		}
		escapeKey(sb, f.Key)
		sb.WriteString("=")
		switch f.Value.(type) {
		case string:
			sb.WriteString(`"`)
			escapeValue(sb, f.Value.(string))
			sb.WriteString(`"`)
		default:
			sb.WriteString(fmt.Sprintf("%v", f.Value))
		}
		switch f.Value.(type) {
		case int64:
			sb.WriteString("i")
		case uint64:
			sb.WriteString("u")
		}
	}
	if !m.timestamp.IsZero() {
		sb.WriteString(" ")
		switch precision {
		case time.Microsecond:
			sb.WriteString(strconv.FormatInt(m.Time().UnixNano()/1000, 10))
		case time.Millisecond:
			sb.WriteString(strconv.FormatInt(m.Time().UnixNano()/1000000, 10))
		case time.Second:
			sb.WriteString(strconv.FormatInt(m.Time().Unix(), 10))
		default:
			sb.WriteString(strconv.FormatInt(m.Time().UnixNano(), 10))
		}
	}
	sb.WriteString("\n")
}

// ToLineProtocol creates InfluxDB line protocol string from the Point, converting associated timestamp according to precision
func (m *Point) ToLineProtocol(precision time.Duration) string {
	var sb strings.Builder
	sb.Grow(1024)
	m.ToLineProtocolBuffer(&sb, precision)
	return sb.String()
}

func convertField(v interface{}) interface{} {
	switch v := v.(type) {
	case bool, int64, string, float64:
		return v
	case int:
		return int64(v)
	case uint:
		return uint64(v)
	case uint64:
		return v
	case []byte:
		return string(v)
	case int32:
		return int64(v)
	case int16:
		return int64(v)
	case int8:
		return int64(v)
	case uint32:
		return uint64(v)
	case uint16:
		return uint64(v)
	case uint8:
		return uint64(v)
	case float32:
		return float64(v)
	case time.Time:
		return v.Format(time.RFC3339Nano)
	case time.Duration:
		return v.String()
	default:
		panic("unsupported type")
	}
}

func escapeKey(sb *strings.Builder, key string) {
	for _, r := range key {
		switch r {
		case ' ', ',', '=':
			sb.WriteString(`\`)
		}
		sb.WriteRune(r)
	}
}

func escapeValue(sb *strings.Builder, value string) {
	for _, r := range value {
		switch r {
		case '\\', '"':
			sb.WriteString(`\`)
		}
		sb.WriteRune(r)
	}
}

// NewPointWithMeasurement creates a empty Point with just a measurement name
func NewPointWithMeasurement(measurement string) *Point {
	return &Point{measurement: measurement}
}

// NewPoint creates a *Point from measurement name, tags, fields and a timestamp.
func NewPoint(
	measurement string,
	tags map[string]string,
	fields map[string]interface{},
	ts time.Time,
) *Point {
	m := &Point{
		measurement: measurement,
		tags:        nil,
		fields:      nil,
		timestamp:   ts,
	}

	if len(tags) > 0 {
		m.tags = make([]*lp.Tag, 0, len(tags))
		for k, v := range tags {
			m.tags = append(m.tags,
				&lp.Tag{Key: k, Value: v})
		}
	}

	m.fields = make([]*lp.Field, 0, len(fields))
	for k, v := range fields {
		v := convertField(v)
		if v == nil {
			continue
		}
		m.fields = append(m.fields, &lp.Field{Key: k, Value: v})
	}
	m.SortFields()
	m.SortTags()
	return m
}
