package client

import (
	"sort"
	"time"

	lp "github.com/influxdata/line-protocol"
)

// Point is a Metric,
// that has methods to make it easy to add tags and fields
type Point struct {
	NameStr string
	Tags    []*lp.Tag
	Fields  []*lp.Field
	TS      time.Time
}

// TagList returns a slice containing Tags of a Metric.
func (m *Point) TagList() []*lp.Tag {
	return m.Tags
}

// FieldList returns a slice containing the Fields of a Metric.
func (m *Point) FieldList() []*lp.Field {
	return m.Fields
}

// Time is the timestamp of a metric.
func (m *Point) Time() time.Time {
	return m.TS
}

// SortTags orders the tags of a metric alphanumerically by key.
// This is just here as a helper, to make it easy to keep tags sorted if you are creating a Point manually.
func (m *Point) SortTags() {
	sort.Slice(m.Tags, func(i, j int) bool { return m.Tags[i].Key < m.Tags[j].Key })
}

// SortFields orders the fields of a metric alphnumerically by key.
func (m *Point) SortFields() {
	sort.Slice(m.Fields, func(i, j int) bool { return m.Fields[i].Key < m.Fields[j].Key })
}

// AddTag adds an lp.Tag to a metric.
func (m *Point) AddTag(k, v string) {
	for i, tag := range m.Tags {
		if k == tag.Key {
			m.Tags[i].Value = v
			return
		}
	}
	m.Tags = append(m.Tags, &lp.Tag{Key: k, Value: v})
}

// AddField adds an lp.Field to a metric.
func (m *Point) AddField(k string, v interface{}) {
	for i, field := range m.Fields {
		if k == field.Key {
			m.Fields[i].Value = v
			return
		}
	}
	m.Fields = append(m.Fields, &lp.Field{Key: k, Value: convertField(v)})
}

// Name returns the name of the metric.
func (m *Point) Name() string {
	return m.NameStr
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
		return uint64(v)
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
	default:
		panic("unsupported type")
	}
}

// NewPoint creates a *Point from tags, fields and a timestamp.
func NewPoint(
	fields map[string]interface{},
	name string,
	tags map[string]string,
	ts time.Time,
) *Point {
	m := &Point{
		NameStr: name,
		Tags:    nil,
		Fields:  nil,
		TS:      ts,
	}

	if len(tags) > 0 {
		m.Tags = make([]*lp.Tag, 0, len(tags))
		for k, v := range tags {
			m.Tags = append(m.Tags,
				&lp.Tag{Key: k, Value: v})
		}
	}

	m.Fields = make([]*lp.Field, 0, len(fields))
	for k, v := range fields {
		v := convertField(v)
		if v == nil {
			continue
		}
		m.Fields = append(m.Fields, &lp.Field{Key: k, Value: v})
	}
	m.SortFields()
	m.SortTags()
	return m
}
