package client

import (
	"bytes"
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"

	lp "github.com/influxdata/line-protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var points []*Point

func init() {
	points = make([]*Point, 5000)
	rand.Seed(321)

	t := time.Now()
	for i := 0; i < len(points); i++ {
		points[i] = NewPoint(
			"test",
			map[string]string{
				"id":       fmt.Sprintf("rack_%v", i%100),
				"vendor":   "AWS",
				"hostname": fmt.Sprintf("host_%v", i%10),
			},
			map[string]interface{}{
				"temperature": rand.Float64() * 80.0,
				"disk_free":   rand.Float64() * 1000.0,
				"disk_total":  (i/10 + 1) * 1000000,
				"mem_total":   (i/100 + 1) * 10000000,
				"mem_free":    rand.Float64() * 10000000.0,
			},
			t)
		if i%10 == 0 {
			t = t.Add(time.Second)
		}
	}
}

func TestNewPoint(t *testing.T) {
	p := NewPoint(
		"test",
		map[string]string{
			"id":        "10ad=",
			"ven=dor":   "AWS",
			`host"name`: `ho\st "a"`,
			`x\" x`:     "a b",
		},
		map[string]interface{}{
			"float64":  80.1234567,
			"float32":  float32(80.0),
			"int":      -1234567890,
			"int8":     int8(-34),
			"int16":    int16(-3456),
			"int32":    int32(-34567),
			"int64":    int64(-1234567890),
			"uint":     uint(12345677890),
			"uint8":    uint8(34),
			"uint16":   uint16(3456),
			"uint32":   uint32(34578),
			"uint 64":  uint64(41234567890),
			"bo\\ol":   false,
			`"string"`: `six, "seven", eight`,
			"stri=ng":  `six=seven\, eight`,
			"time":     time.Date(2020, time.March, 20, 10, 30, 23, 123456789, time.UTC),
			"duration": 4*time.Hour + 24*time.Minute + 3*time.Second,
		},
		time.Unix(60, 70))
	verifyPoint(t, p)
}

func verifyPoint(t *testing.T, p *Point) {
	assert.Equal(t, p.Name(), "test")
	require.Len(t, p.TagList(), 4)
	assert.Equal(t, p.TagList(), []*lp.Tag{
		{Key: `host"name`, Value: "ho\\st \"a\""},
		{Key: "id", Value: "10ad="},
		{Key: `ven=dor`, Value: "AWS"},
		{Key: "x\\\" x", Value: "a b"}})
	require.Len(t, p.FieldList(), 17)
	assert.Equal(t, p.FieldList(), []*lp.Field{
		{Key: `"string"`, Value: `six, "seven", eight`},
		{Key: "bo\\ol", Value: false},
		{Key: "duration", Value: "4h24m3s"},
		{Key: "float32", Value: 80.0},
		{Key: "float64", Value: 80.1234567},
		{Key: "int", Value: int64(-1234567890)},
		{Key: "int16", Value: int64(-3456)},
		{Key: "int32", Value: int64(-34567)},
		{Key: "int64", Value: int64(-1234567890)},
		{Key: "int8", Value: int64(-34)},
		{Key: "stri=ng", Value: `six=seven\, eight`},
		{Key: "time", Value: "2020-03-20T10:30:23.123456789Z"},
		{Key: "uint", Value: uint64(12345677890)},
		{Key: "uint 64", Value: uint64(41234567890)},
		{Key: "uint16", Value: uint64(3456)},
		{Key: "uint32", Value: uint64(34578)},
		{Key: "uint8", Value: uint64(34)},
	})
	line := p.ToLineProtocol(time.Nanosecond)
	assert.True(t, strings.HasSuffix(line, "\n"))
	//cut off last \n char
	line = line[:len(line)-1]
	assert.Equal(t, line, `test,host"name=ho\st\ "a",id=10ad\=,ven\=dor=AWS,x\"\ x=a\ b "string"="six, \"seven\", eight",bo\ol=false,duration="4h24m3s",float32=80,float64=80.1234567,int=-1234567890i,int16=-3456i,int32=-34567i,int64=-1234567890i,int8=-34i,stri\=ng="six=seven\\, eight",time="2020-03-20T10:30:23.123456789Z",uint=12345677890u,uint\ 64=41234567890u,uint16=3456u,uint32=34578u,uint8=34u 60000000070`)
}

func TestPointAdd(t *testing.T) {
	p := NewPointWithMeasurement("test")
	p.AddTag("id", "10ad=")
	p.AddTag("ven=dor", "AWS")
	p.AddTag(`host"name`, "host_a")
	//test re-setting same tag
	p.AddTag(`host"name`, `ho\st "a"`)
	p.AddTag(`x\" x`, "a b")
	p.SortTags()

	p.AddField("float64", 80.1234567)
	p.AddField("float32", float32(80.0))
	p.AddField("int", -1234567890)
	p.AddField("int8", int8(-34))
	p.AddField("int16", int16(-3456))
	p.AddField("int32", int32(-34567))
	p.AddField("int64", int64(-1234567890))
	p.AddField("uint", uint(12345677890))
	p.AddField("uint8", uint8(34))
	p.AddField("uint16", uint16(3456))
	p.AddField("uint32", uint32(34578))
	p.AddField("uint 64", uint64(0))
	// test re-setting same field
	p.AddField("uint 64", uint64(41234567890))
	p.AddField("bo\\ol", false)
	p.AddField(`"string"`, `six, "seven", eight`)
	p.AddField("stri=ng", `six=seven\, eight`)
	p.AddField("time", time.Date(2020, time.March, 20, 10, 30, 23, 123456789, time.UTC))
	p.AddField("duration", time.Duration(4*time.Hour+24*time.Minute+3*time.Second))
	p.SortFields()

	p.SetTime(time.Unix(60, 70))
	verifyPoint(t, p)
}

func TestPrecision(t *testing.T) {
	p := NewPointWithMeasurement("test")
	p.AddTag("id", "10")
	p.AddField("float64", 80.1234567)

	p.SetTime(time.Unix(60, 89))
	line := p.ToLineProtocol(time.Nanosecond)
	assert.Equal(t, line, "test,id=10 float64=80.1234567 60000000089\n")

	p.SetTime(time.Unix(60, 56789))
	line = p.ToLineProtocol(time.Microsecond)
	assert.Equal(t, line, "test,id=10 float64=80.1234567 60000056\n")

	p.SetTime(time.Unix(60, 123456789))
	line = p.ToLineProtocol(time.Millisecond)
	assert.Equal(t, line, "test,id=10 float64=80.1234567 60123\n")

	p.SetTime(time.Unix(60, 123456789))
	line = p.ToLineProtocol(time.Second)
	assert.Equal(t, line, "test,id=10 float64=80.1234567 60\n")
}

var s string

func BenchmarkPointEncoderSingle(b *testing.B) {
	for n := 0; n < b.N; n++ {
		var buff strings.Builder
		for _, p := range points {
			var buffer bytes.Buffer
			e := lp.NewEncoder(&buffer)
			e.Encode(p)
			buff.WriteString(buffer.String())
		}
		s = buff.String()
	}
}

func BenchmarkPointEncoderMulti(b *testing.B) {
	for n := 0; n < b.N; n++ {
		var buffer bytes.Buffer
		e := lp.NewEncoder(&buffer)
		for _, p := range points {
			e.Encode(p)
		}
		s = buffer.String()
	}
}

func BenchmarkPointStringSingle(b *testing.B) {
	for n := 0; n < b.N; n++ {
		var buff strings.Builder
		for _, p := range points {
			buff.WriteString(p.ToLineProtocol(time.Nanosecond))
		}
		s = buff.String()
	}
}

func BenchmarkPointStringMulti(b *testing.B) {
	for n := 0; n < b.N; n++ {
		var buff strings.Builder
		for _, p := range points {
			p.ToLineProtocolBuffer(&buff, time.Nanosecond)
		}
		s = buff.String()
	}
}
