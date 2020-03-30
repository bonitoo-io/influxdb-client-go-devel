package client

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestWritePoint(t *testing.T) {
	client := &testClient{
		options: DefaultOptions(),
		t:       t,
	}
	client.options.BatchSize = 5
	writeApi := newWriteApiBlockingImpl("my-org", "my-bucket", client)
	points := genPoints(10)
	err := writeApi.WritePoint(points...)
	require.Nil(t, err)
	require.Len(t, client.lines, 10)
	for i, p := range points {
		line := p.ToLineProtocol(client.options.Precision)
		//cut off last \n char
		line = line[:len(line)-1]
		assert.Equal(t, client.lines[i], line)
	}
}

func TestWriteRecord(t *testing.T) {
	client := &testClient{
		options: DefaultOptions(),
		t:       t,
	}
	client.options.BatchSize = 5
	writeApi := newWriteApiBlockingImpl("my-org", "my-bucket", client)
	lines := genRecords(10)
	err := writeApi.WriteRecord(lines...)
	require.Nil(t, err)
	require.Len(t, client.lines, 10)
	for i, l := range lines {
		assert.Equal(t, l, client.lines[i])
	}
	client.Close()

	err = writeApi.WriteRecord()
	require.Nil(t, err)
	require.Len(t, client.lines, 0)

	client.replyError = &Error{Code: "invalid", Message: "data"}
	err = writeApi.WriteRecord(lines...)
	require.NotNil(t, err)
	require.Equal(t, "invalid: data", err.Error())
}
