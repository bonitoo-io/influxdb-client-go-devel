// Copyright 2020 InfluxData, Inc. All rights reserved.
// Use of this source code is governed by MIT
// license that can be found in the LICENSE file.

package client

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func mustParseTime(s string) time.Time {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		panic(err)
	}
	return t
}

func TestQueryCVSResultSingleTable(t *testing.T) {
	csvTable := `#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,double,string,string,string,string
#group,false,false,true,true,false,false,true,true,true,true
#default,_result,,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,a,b
,,0,2020-02-17T22:19:49.747562847Z,2020-02-18T22:19:49.747562847Z,2020-02-18T10:34:08.135814545Z,1.4,f,test,1,adsfasdf
,,0,2020-02-17T22:19:49.747562847Z,2020-02-18T22:19:49.747562847Z,2020-02-18T22:08:44.850214724Z,6.6,f,test,1,adsfasdf

`
	expectedTable := &FluxTableMetadata{position: 0,
		columns: []*FluxColumn{
			{dataType: "string", defaultValue: "_result", name: "result", group: false, index: 0},
			{dataType: "long", defaultValue: "", name: "table", group: false, index: 1},
			{dataType: "dateTime:RFC3339", defaultValue: "", name: "_start", group: true, index: 2},
			{dataType: "dateTime:RFC3339", defaultValue: "", name: "_stop", group: true, index: 3},
			{dataType: "dateTime:RFC3339", defaultValue: "", name: "_time", group: false, index: 4},
			{dataType: "double", defaultValue: "", name: "_value", group: false, index: 5},
			{dataType: "string", defaultValue: "", name: "_field", group: true, index: 6},
			{dataType: "string", defaultValue: "", name: "_measurement", group: true, index: 7},
			{dataType: "string", defaultValue: "", name: "a", group: true, index: 8},
			{dataType: "string", defaultValue: "", name: "b", group: true, index: 9},
		},
	}
	expectedRecord1 := &FluxRecord{table: 0,
		values: map[string]interface{}{
			"result":       "_result",
			"table":        int64(0),
			"_start":       mustParseTime("2020-02-17T22:19:49.747562847Z"),
			"_stop":        mustParseTime("2020-02-18T22:19:49.747562847Z"),
			"_time":        mustParseTime("2020-02-18T10:34:08.135814545Z"),
			"_value":       1.4,
			"_field":       "f",
			"_measurement": "test",
			"a":            "1",
			"b":            "adsfasdf",
		},
	}

	expectedRecord2 := &FluxRecord{table: 0,
		values: map[string]interface{}{
			"result":       "_result",
			"table":        int64(0),
			"_start":       mustParseTime("2020-02-17T22:19:49.747562847Z"),
			"_stop":        mustParseTime("2020-02-18T22:19:49.747562847Z"),
			"_time":        mustParseTime("2020-02-18T22:08:44.850214724Z"),
			"_value":       6.6,
			"_field":       "f",
			"_measurement": "test",
			"a":            "1",
			"b":            "adsfasdf",
		},
	}

	reader := strings.NewReader(csvTable)
	csvReader := csv.NewReader(reader)
	csvReader.FieldsPerRecord = -1
	queryResult := &QueryCSVResult{Closer: ioutil.NopCloser(reader), csvReader: csvReader}
	require.True(t, queryResult.Next(), queryResult.Err())
	require.Nil(t, queryResult.Err())

	require.Equal(t, queryResult.table, expectedTable)
	require.NotNil(t, queryResult.Record())
	require.Equal(t, queryResult.Record(), expectedRecord1)

	require.True(t, queryResult.Next(), queryResult.Err())
	require.Nil(t, queryResult.Err())
	require.NotNil(t, queryResult.Record())
	require.Equal(t, queryResult.Record(), expectedRecord2)

	require.False(t, queryResult.Next())
	require.Nil(t, queryResult.Err())
}

func TestQueryCVSResultMultiTables(t *testing.T) {
	csvTable := `#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,double,string,string,string,string
#group,false,false,true,true,false,false,true,true,true,true
#default,_result,,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,a,b
,,0,2020-02-17T22:19:49.747562847Z,2020-02-18T22:19:49.747562847Z,2020-02-18T10:34:08.135814545Z,1.4,f,test,1,adsfasdf
,,0,2020-02-17T22:19:49.747562847Z,2020-02-18T22:19:49.747562847Z,2020-02-18T22:08:44.850214724Z,6.6,f,test,1,adsfasdf

#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,long,string,string,string,string
#group,false,false,true,true,false,false,true,true,true,true
#default,_result,,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,a,b
,,1,2020-02-17T22:19:49.747562847Z,2020-02-18T22:19:49.747562847Z,2020-02-18T10:34:08.135814545Z,4,i,test,1,adsfasdf
,,1,2020-02-17T22:19:49.747562847Z,2020-02-18T22:19:49.747562847Z,2020-02-18T22:08:44.850214724Z,-1,i,test,1,adsfasdf

#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,bool,string,string,string,string
#group,false,false,true,true,false,false,true,true,true,true
#default,_result,,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,a,b
,,2,2020-02-17T22:19:49.747562847Z,2020-02-18T22:19:49.747562847Z,2020-02-18T22:08:44.62797864Z,false,f,test,0,adsfasdf
,,2,2020-02-17T22:19:49.747562847Z,2020-02-18T22:19:49.747562847Z,2020-02-18T22:08:44.969100374Z,true,f,test,0,adsfasdf

#datatype,string,long,dateTime:RFC3339Nano,dateTime:RFC3339Nano,dateTime:RFC3339Nano,unsignedLong,string,string,string,string
#group,false,false,true,true,false,false,true,true,true,true
#default,_result,,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,a,b
,,3,2020-02-17T22:19:49.747562847Z,2020-02-18T22:19:49.747562847Z,2020-02-18T22:08:44.62797864Z,0,i,test,0,adsfasdf
,,3,2020-02-17T22:19:49.747562847Z,2020-02-18T22:19:49.747562847Z,2020-02-18T22:08:44.969100374Z,2,i,test,0,adsfasdf

`
	expectedTable1 := &FluxTableMetadata{position: 0,
		columns: []*FluxColumn{
			{dataType: "string", defaultValue: "_result", name: "result", group: false, index: 0},
			{dataType: "long", defaultValue: "", name: "table", group: false, index: 1},
			{dataType: "dateTime:RFC3339", defaultValue: "", name: "_start", group: true, index: 2},
			{dataType: "dateTime:RFC3339", defaultValue: "", name: "_stop", group: true, index: 3},
			{dataType: "dateTime:RFC3339", defaultValue: "", name: "_time", group: false, index: 4},
			{dataType: "double", defaultValue: "", name: "_value", group: false, index: 5},
			{dataType: "string", defaultValue: "", name: "_field", group: true, index: 6},
			{dataType: "string", defaultValue: "", name: "_measurement", group: true, index: 7},
			{dataType: "string", defaultValue: "", name: "a", group: true, index: 8},
			{dataType: "string", defaultValue: "", name: "b", group: true, index: 9},
		},
	}
	expectedRecord11 := &FluxRecord{table: 0,
		values: map[string]interface{}{
			"result":       "_result",
			"table":        int64(0),
			"_start":       mustParseTime("2020-02-17T22:19:49.747562847Z"),
			"_stop":        mustParseTime("2020-02-18T22:19:49.747562847Z"),
			"_time":        mustParseTime("2020-02-18T10:34:08.135814545Z"),
			"_value":       1.4,
			"_field":       "f",
			"_measurement": "test",
			"a":            "1",
			"b":            "adsfasdf",
		},
	}
	expectedRecord12 := &FluxRecord{table: 0,
		values: map[string]interface{}{
			"result":       "_result",
			"table":        int64(0),
			"_start":       mustParseTime("2020-02-17T22:19:49.747562847Z"),
			"_stop":        mustParseTime("2020-02-18T22:19:49.747562847Z"),
			"_time":        mustParseTime("2020-02-18T22:08:44.850214724Z"),
			"_value":       6.6,
			"_field":       "f",
			"_measurement": "test",
			"a":            "1",
			"b":            "adsfasdf",
		},
	}

	expectedTable2 := &FluxTableMetadata{position: 1,
		columns: []*FluxColumn{
			{dataType: "string", defaultValue: "_result", name: "result", group: false, index: 0},
			{dataType: "long", defaultValue: "", name: "table", group: false, index: 1},
			{dataType: "dateTime:RFC3339", defaultValue: "", name: "_start", group: true, index: 2},
			{dataType: "dateTime:RFC3339", defaultValue: "", name: "_stop", group: true, index: 3},
			{dataType: "dateTime:RFC3339", defaultValue: "", name: "_time", group: false, index: 4},
			{dataType: "long", defaultValue: "", name: "_value", group: false, index: 5},
			{dataType: "string", defaultValue: "", name: "_field", group: true, index: 6},
			{dataType: "string", defaultValue: "", name: "_measurement", group: true, index: 7},
			{dataType: "string", defaultValue: "", name: "a", group: true, index: 8},
			{dataType: "string", defaultValue: "", name: "b", group: true, index: 9},
		},
	}
	expectedRecord21 := &FluxRecord{table: 1,
		values: map[string]interface{}{
			"result":       "_result",
			"table":        int64(1),
			"_start":       mustParseTime("2020-02-17T22:19:49.747562847Z"),
			"_stop":        mustParseTime("2020-02-18T22:19:49.747562847Z"),
			"_time":        mustParseTime("2020-02-18T10:34:08.135814545Z"),
			"_value":       int64(4),
			"_field":       "i",
			"_measurement": "test",
			"a":            "1",
			"b":            "adsfasdf",
		},
	}
	expectedRecord22 := &FluxRecord{table: 1,
		values: map[string]interface{}{
			"result":       "_result",
			"table":        int64(1),
			"_start":       mustParseTime("2020-02-17T22:19:49.747562847Z"),
			"_stop":        mustParseTime("2020-02-18T22:19:49.747562847Z"),
			"_time":        mustParseTime("2020-02-18T22:08:44.850214724Z"),
			"_value":       int64(-1),
			"_field":       "i",
			"_measurement": "test",
			"a":            "1",
			"b":            "adsfasdf",
		},
	}

	expectedTable3 := &FluxTableMetadata{position: 2,
		columns: []*FluxColumn{
			{dataType: "string", defaultValue: "_result", name: "result", group: false, index: 0},
			{dataType: "long", defaultValue: "", name: "table", group: false, index: 1},
			{dataType: "dateTime:RFC3339", defaultValue: "", name: "_start", group: true, index: 2},
			{dataType: "dateTime:RFC3339", defaultValue: "", name: "_stop", group: true, index: 3},
			{dataType: "dateTime:RFC3339", defaultValue: "", name: "_time", group: false, index: 4},
			{dataType: "bool", defaultValue: "", name: "_value", group: false, index: 5},
			{dataType: "string", defaultValue: "", name: "_field", group: true, index: 6},
			{dataType: "string", defaultValue: "", name: "_measurement", group: true, index: 7},
			{dataType: "string", defaultValue: "", name: "a", group: true, index: 8},
			{dataType: "string", defaultValue: "", name: "b", group: true, index: 9},
		},
	}
	expectedRecord31 := &FluxRecord{table: 2,
		values: map[string]interface{}{
			"result":       "_result",
			"table":        int64(2),
			"_start":       mustParseTime("2020-02-17T22:19:49.747562847Z"),
			"_stop":        mustParseTime("2020-02-18T22:19:49.747562847Z"),
			"_time":        mustParseTime("2020-02-18T22:08:44.62797864Z"),
			"_value":       false,
			"_field":       "f",
			"_measurement": "test",
			"a":            "0",
			"b":            "adsfasdf",
		},
	}
	expectedRecord32 := &FluxRecord{table: 2,
		values: map[string]interface{}{
			"result":       "_result",
			"table":        int64(2),
			"_start":       mustParseTime("2020-02-17T22:19:49.747562847Z"),
			"_stop":        mustParseTime("2020-02-18T22:19:49.747562847Z"),
			"_time":        mustParseTime("2020-02-18T22:08:44.969100374Z"),
			"_value":       true,
			"_field":       "f",
			"_measurement": "test",
			"a":            "0",
			"b":            "adsfasdf",
		},
	}

	expectedTable4 := &FluxTableMetadata{position: 3,
		columns: []*FluxColumn{
			{dataType: "string", defaultValue: "_result", name: "result", group: false, index: 0},
			{dataType: "long", defaultValue: "", name: "table", group: false, index: 1},
			{dataType: "dateTime:RFC3339Nano", defaultValue: "", name: "_start", group: true, index: 2},
			{dataType: "dateTime:RFC3339Nano", defaultValue: "", name: "_stop", group: true, index: 3},
			{dataType: "dateTime:RFC3339Nano", defaultValue: "", name: "_time", group: false, index: 4},
			{dataType: "unsignedLong", defaultValue: "", name: "_value", group: false, index: 5},
			{dataType: "string", defaultValue: "", name: "_field", group: true, index: 6},
			{dataType: "string", defaultValue: "", name: "_measurement", group: true, index: 7},
			{dataType: "string", defaultValue: "", name: "a", group: true, index: 8},
			{dataType: "string", defaultValue: "", name: "b", group: true, index: 9},
		},
	}
	expectedRecord41 := &FluxRecord{table: 3,
		values: map[string]interface{}{
			"result":       "_result",
			"table":        int64(3),
			"_start":       mustParseTime("2020-02-17T22:19:49.747562847Z"),
			"_stop":        mustParseTime("2020-02-18T22:19:49.747562847Z"),
			"_time":        mustParseTime("2020-02-18T22:08:44.62797864Z"),
			"_value":       uint64(0),
			"_field":       "i",
			"_measurement": "test",
			"a":            "0",
			"b":            "adsfasdf",
		},
	}
	expectedRecord42 := &FluxRecord{table: 3,
		values: map[string]interface{}{
			"result":       "_result",
			"table":        int64(3),
			"_start":       mustParseTime("2020-02-17T22:19:49.747562847Z"),
			"_stop":        mustParseTime("2020-02-18T22:19:49.747562847Z"),
			"_time":        mustParseTime("2020-02-18T22:08:44.969100374Z"),
			"_value":       uint64(2),
			"_field":       "i",
			"_measurement": "test",
			"a":            "0",
			"b":            "adsfasdf",
		},
	}

	reader := strings.NewReader(csvTable)
	csvReader := csv.NewReader(reader)
	csvReader.FieldsPerRecord = -1
	queryResult := &QueryCSVResult{Closer: ioutil.NopCloser(reader), csvReader: csvReader}
	require.True(t, queryResult.Next(), queryResult.Err())
	require.Nil(t, queryResult.Err())

	require.Equal(t, queryResult.TableMetadata(), expectedTable1)
	require.NotNil(t, queryResult.Record())
	require.Equal(t, queryResult.Record(), expectedRecord11)

	require.True(t, queryResult.Next(), queryResult.Err())
	require.Nil(t, queryResult.Err())
	require.Equal(t, queryResult.TableMetadata(), expectedTable1)
	require.NotNil(t, queryResult.Record())
	require.Equal(t, queryResult.Record(), expectedRecord12)

	require.True(t, queryResult.Next(), queryResult.Err())
	require.Nil(t, queryResult.Err())

	require.Equal(t, queryResult.table, expectedTable2)
	require.NotNil(t, queryResult.Record())
	require.Equal(t, queryResult.Record(), expectedRecord21)

	require.True(t, queryResult.Next(), queryResult.Err())
	require.Nil(t, queryResult.Err())

	require.Equal(t, queryResult.table, expectedTable2)
	require.NotNil(t, queryResult.Record())
	require.Equal(t, queryResult.Record(), expectedRecord22)

	require.True(t, queryResult.Next(), queryResult.Err())
	require.Nil(t, queryResult.Err(), queryResult.Err())

	require.Equal(t, queryResult.table, expectedTable3)
	require.NotNil(t, queryResult.Record())
	require.Equal(t, queryResult.Record(), expectedRecord31)

	require.True(t, queryResult.Next(), queryResult.Err())
	require.Nil(t, queryResult.Err())

	require.Equal(t, queryResult.table, expectedTable3)
	require.NotNil(t, queryResult.Record())
	require.Equal(t, queryResult.Record(), expectedRecord32)

	require.True(t, queryResult.Next(), queryResult.Err())
	require.Nil(t, queryResult.Err())

	require.Equal(t, queryResult.table, expectedTable4)
	require.NotNil(t, queryResult.Record())
	require.Equal(t, queryResult.Record(), expectedRecord41)

	require.True(t, queryResult.Next(), queryResult.Err())
	require.Nil(t, queryResult.Err())

	require.Equal(t, queryResult.table, expectedTable4)
	require.NotNil(t, queryResult.Record())
	require.Equal(t, queryResult.Record(), expectedRecord42)

	require.False(t, queryResult.Next())
	require.Nil(t, queryResult.Err())
}

func TestQueryRawResultMultiTables(t *testing.T) {
	csvRows := []string{`#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,double,string,string,string,string`,
		`#group,false,false,true,true,false,false,true,true,true,true`,
		`#default,_result,,,,,,,,,`,
		`,result,table,_start,_stop,_time,_value,_field,_measurement,a,b`,
		`,,0,2020-02-17T22:19:49.747562847Z,2020-02-18T22:19:49.747562847Z,2020-02-18T10:34:08.135814545Z,1.4,f,test,1,adsfasdf`,
		`,,0,2020-02-17T22:19:49.747562847Z,2020-02-18T22:19:49.747562847Z,2020-02-18T22:08:44.850214724Z,6.6,f,test,1,adsfasdf`,
		``,
		`#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,long,string,string,string,string`,
		`#group,false,false,true,true,false,false,true,true,true,true`,
		`#default,_result,,,,,,,,,`,
		`,result,table,_start,_stop,_time,_value,_field,_measurement,a,b`,
		`,,1,2020-02-17T22:19:49.747562847Z,2020-02-18T22:19:49.747562847Z,2020-02-18T10:34:08.135814545Z,4,i,test,1,adsfasdf`,
		`,,1,2020-02-17T22:19:49.747562847Z,2020-02-18T22:19:49.747562847Z,2020-02-18T22:08:44.850214724Z,1,i,test,1,adsfasdf`,
		``,
	}
	csvTable := strings.Join(csvRows, "\r\n")
	csvTable = fmt.Sprintf("%s\r\n", csvTable)
	reader := strings.NewReader(csvTable)
	queryResult := &QueryRawResult{Closer: ioutil.NopCloser(reader), scanner: bufio.NewScanner(reader)}
	for i, row := range csvRows {
		require.True(t, queryResult.Next(), queryResult.Err())
		require.Nil(t, queryResult.Err(), i)
		assert.Equal(t, queryResult.Row(), row, i)
	}
	require.False(t, queryResult.Next())
	require.Nil(t, queryResult.Err())

}

func TestErrorInRow(t *testing.T) {
	csvRowsError := []string{
		`#datatype,string,string`,
		`#group,true,true`,
		`#default,,`,
		`,error,reference`,
		`,failed to create physical plan: invalid time bounds from procedure from: bounds contain zero time,897`}
	csvTable := makeCSVstring(csvRowsError)
	reader := strings.NewReader(csvTable)
	csvReader := csv.NewReader(reader)
	csvReader.FieldsPerRecord = -1
	queryResult := &QueryCSVResult{Closer: ioutil.NopCloser(reader), csvReader: csvReader}

	require.False(t, queryResult.Next())
	require.NotNil(t, queryResult.Err())
	assert.Equal(t, "failed to create physical plan: invalid time bounds from procedure from: bounds contain zero time,897", queryResult.Err().Error())

	csvRowsErrorNoReference := []string{
		`#datatype,string,string`,
		`#group,true,true`,
		`#default,,`,
		`,error,reference`,
		`,failed to create physical plan: invalid time bounds from procedure from: bounds contain zero time,`}
	csvTable = makeCSVstring(csvRowsErrorNoReference)
	reader = strings.NewReader(csvTable)
	csvReader = csv.NewReader(reader)
	csvReader.FieldsPerRecord = -1
	queryResult = &QueryCSVResult{Closer: ioutil.NopCloser(reader), csvReader: csvReader}

	require.False(t, queryResult.Next())
	require.NotNil(t, queryResult.Err())
	assert.Equal(t, "failed to create physical plan: invalid time bounds from procedure from: bounds contain zero time", queryResult.Err().Error())

}

func makeCSVstring(rows []string) string {
	csvTable := strings.Join(rows, "\r\n")
	return fmt.Sprintf("%s\r\n", csvTable)
}
