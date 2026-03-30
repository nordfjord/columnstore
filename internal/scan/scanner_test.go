package scan

import (
	"bytes"
	"encoding/binary"
	"io"
	"testing"

	"github.com/nordfjord/columnstore/internal/format"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInt64ScannerAdvanceTo(t *testing.T) {
	buf := mustInt64Stream(t,
		tupleInt64{1, 100},
		tupleInt64{3, 200},
		tupleInt64{7, 300},
	)

	s := NewInt64Scanner(bytes.NewReader(buf))

	ok, err := s.AdvanceTo(3)
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, int32(3), s.RowID())

	ok, err = s.AdvanceTo(2)
	require.NoError(t, err)
	assert.False(t, ok)
	assert.Equal(t, int32(3), s.RowID())

	ok, err = s.AdvanceTo(7)
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, int32(7), s.RowID())
}

func TestStringScannerAdvanceTo(t *testing.T) {
	buf := mustStringStream(t,
		tupleStr{1, "A"},
		tupleStr{5, "B"},
	)

	s := NewStringScanner(bytes.NewReader(buf))
	ok, err := s.AdvanceTo(5)
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, "B", s.Value())
}

func TestCountByTimeAndEq(t *testing.T) {
	date := NewInt64Scanner(bytes.NewReader(mustInt64Stream(t,
		tupleInt64{1, 1704067200},
		tupleInt64{2, 1704153600},
		tupleInt64{3, 1704240000},
	)))

	entityType := NewStringScanner(bytes.NewReader(mustStringStream(t,
		tupleStr{1, "SalesTransaction"},
		tupleStr{3, "Invoice"},
	)))

	count, err := CountByTimeAndEq(date, entityType, 1704067200, 1704240000, "SalesTransaction")
	require.NoError(t, err)
	assert.Equal(t, int64(1), count)
}

func writeAll(w io.Writer, b []byte) error {
	for len(b) > 0 {
		n, err := w.Write(b)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		b = b[n:]
	}
	return nil
}

func TestScannerRejectsNonMonotonicRowIDs(t *testing.T) {
	var buf bytes.Buffer
	w := format.NewInt64TupleWriter(&buf)
	require.NoError(t, w.Write(2, 100))

	var malformed [12]byte
	binary.LittleEndian.PutUint32(malformed[0:4], uint32(1))
	binary.LittleEndian.PutUint64(malformed[4:12], uint64(200))
	require.NoError(t, writeAll(&buf, malformed[:]))

	s := NewInt64Scanner(bytes.NewReader(buf.Bytes()))
	_, err := s.Next()
	require.NoError(t, err)
	_, err = s.Next()
	require.Error(t, err)
}

type tupleInt64 struct {
	rowID int32
	value int64
}

func mustInt64Stream(t *testing.T, tuples ...tupleInt64) []byte {
	t.Helper()

	var buf bytes.Buffer
	w := format.NewInt64TupleWriter(&buf)
	for _, tp := range tuples {
		require.NoError(t, w.Write(tp.rowID, tp.value))
	}
	return buf.Bytes()
}

type tupleStr struct {
	rowID int32
	value string
}

func mustStringStream(t *testing.T, tuples ...tupleStr) []byte {
	t.Helper()

	var buf bytes.Buffer
	w := format.NewStringTupleWriter(&buf)
	for _, tp := range tuples {
		require.NoError(t, w.Write(tp.rowID, tp.value))
	}
	return buf.Bytes()
}
