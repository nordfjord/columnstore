package format

import (
	"bytes"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInt64TupleRoundTrip(t *testing.T) {
	var buf bytes.Buffer
	w := NewInt64TupleWriter(&buf)
	require.NoError(t, w.Write(1, 10))
	require.NoError(t, w.Write(2, -5))

	r := NewInt64TupleReader(bytes.NewReader(buf.Bytes()))
	row, val, err := r.Next()
	require.NoError(t, err)
	assert.Equal(t, int32(1), row)
	assert.Equal(t, int64(10), val)

	row, val, err = r.Next()
	require.NoError(t, err)
	assert.Equal(t, int32(2), row)
	assert.Equal(t, int64(-5), val)

	_, _, err = r.Next()
	require.ErrorIs(t, err, io.EOF)
}

func TestFloat64TupleRoundTrip(t *testing.T) {
	var buf bytes.Buffer
	w := NewFloat64TupleWriter(&buf)
	require.NoError(t, w.Write(1, 1.25))
	require.NoError(t, w.Write(2, -9.5))

	r := NewFloat64TupleReader(bytes.NewReader(buf.Bytes()))
	row, val, err := r.Next()
	require.NoError(t, err)
	assert.Equal(t, int32(1), row)
	assert.Equal(t, 1.25, val)

	row, val, err = r.Next()
	require.NoError(t, err)
	assert.Equal(t, int32(2), row)
	assert.Equal(t, -9.5, val)

	_, _, err = r.Next()
	require.ErrorIs(t, err, io.EOF)
}

func TestBoolTupleRoundTrip(t *testing.T) {
	var buf bytes.Buffer
	w := NewBoolTupleWriter(&buf)
	require.NoError(t, w.Write(1, false))
	require.NoError(t, w.Write(2, true))

	r := NewBoolTupleReader(bytes.NewReader(buf.Bytes()))
	row, val, err := r.Next()
	require.NoError(t, err)
	assert.Equal(t, int32(1), row)
	assert.False(t, val)

	row, val, err = r.Next()
	require.NoError(t, err)
	assert.Equal(t, int32(2), row)
	assert.True(t, val)

	_, _, err = r.Next()
	require.ErrorIs(t, err, io.EOF)
}

func TestStringTupleRoundTrip(t *testing.T) {
	var buf bytes.Buffer
	w := NewStringTupleWriter(&buf)
	require.NoError(t, w.Write(1, ""))
	require.NoError(t, w.Write(2, "hello"))

	r := NewStringTupleReader(bytes.NewReader(buf.Bytes()))
	row, val, err := r.Next()
	require.NoError(t, err)
	assert.Equal(t, int32(1), row)
	assert.Equal(t, "", val)

	row, val, err = r.Next()
	require.NoError(t, err)
	assert.Equal(t, int32(2), row)
	assert.Equal(t, "hello", val)

	_, _, err = r.Next()
	require.ErrorIs(t, err, io.EOF)
}

func TestTupleWriterMonotonicRowID(t *testing.T) {
	var buf bytes.Buffer
	w := NewInt64TupleWriter(&buf)
	require.NoError(t, w.Write(1, 1))
	require.Error(t, w.Write(1, 2))
	require.Error(t, w.Write(0, 2))
}

func TestInt64TupleLittleEndianEncoding(t *testing.T) {
	var buf bytes.Buffer
	w := NewInt64TupleWriter(&buf)
	require.NoError(t, w.Write(1, 2))

	got := buf.Bytes()
	require.Len(t, got, 12)
	assert.Equal(t, []byte{1, 0, 0, 0}, got[:4])
	assert.Equal(t, []byte{2, 0, 0, 0, 0, 0, 0, 0}, got[4:12])
}

func TestInt64TupleMalformedStream(t *testing.T) {
	r := NewInt64TupleReader(bytes.NewReader([]byte{1, 2, 3}))
	_, _, err := r.Next()
	require.ErrorIs(t, err, io.ErrUnexpectedEOF)
}

func TestBoolTupleMalformedValue(t *testing.T) {
	data := []byte{1, 0, 0, 0, 2, 0, 0, 0}
	r := NewBoolTupleReader(bytes.NewReader(data))
	_, _, err := r.Next()
	require.Error(t, err)
}

func TestStringTupleMalformedStream(t *testing.T) {
	// rowid=1, length=5, payload only 2 bytes
	data := []byte{1, 0, 0, 0, 5, 0, 0, 0, 'h', 'i'}
	r := NewStringTupleReader(bytes.NewReader(data))
	_, _, err := r.Next()
	require.ErrorIs(t, err, io.ErrUnexpectedEOF)
}
