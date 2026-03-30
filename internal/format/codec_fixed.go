package format

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
)

const tupleReadBatchBytes = 64 * 1024

type fixedTupleBuffer struct {
	r   io.Reader
	buf []byte
	pos int
	end int
	eof bool
}

func newFixedTupleBuffer(r io.Reader, tupleBytes int) fixedTupleBuffer {
	bufferBytes := max(tupleReadBatchBytes, tupleBytes)
	return fixedTupleBuffer{r: r, buf: make([]byte, bufferBytes)}
}

func (b *fixedTupleBuffer) nextChunk(tupleBytes int) ([]byte, error) {
	if err := b.ensure(tupleBytes); err != nil {
		return nil, err
	}
	chunk := b.buf[b.pos : b.pos+tupleBytes]
	b.pos += tupleBytes
	return chunk, nil
}

func (b *fixedTupleBuffer) ensure(tupleBytes int) error {
	available := b.end - b.pos
	if available >= tupleBytes {
		return nil
	}

	if b.eof {
		if available == 0 {
			return io.EOF
		}
		return io.ErrUnexpectedEOF
	}

	if available > 0 {
		copy(b.buf[:available], b.buf[b.pos:b.end])
	}
	b.pos = 0
	b.end = available

	for b.end < tupleBytes {
		n, err := b.r.Read(b.buf[b.end:])
		if n > 0 {
			b.end += n
		}
		if err == nil {
			continue
		}
		if errors.Is(err, io.EOF) {
			b.eof = true
			if b.end >= tupleBytes {
				return nil
			}
			if b.end == 0 {
				return io.EOF
			}
			return io.ErrUnexpectedEOF
		}
		return err
	}

	return nil
}

type Int64TupleWriter struct {
	w       io.Writer
	hasLast bool
	last    int32
}

func NewInt64TupleWriter(w io.Writer) *Int64TupleWriter { return &Int64TupleWriter{w: w} }

func (tw *Int64TupleWriter) Write(rowID int32, value int64) error {
	if err := validateMonotonicRowID(tw.hasLast, tw.last, rowID); err != nil {
		return err
	}
	var buf [12]byte
	binary.LittleEndian.PutUint32(buf[0:4], uint32(rowID))
	binary.LittleEndian.PutUint64(buf[4:12], uint64(value))
	if err := writeAll(tw.w, buf[:]); err != nil {
		return fmt.Errorf("writing int64 tuple: %w", err)
	}
	tw.last = rowID
	tw.hasLast = true
	return nil
}

type Int64TupleReader struct {
	buf fixedTupleBuffer
}

func NewInt64TupleReader(r io.Reader) *Int64TupleReader {
	return &Int64TupleReader{buf: newFixedTupleBuffer(r, 12)}
}

func (tr *Int64TupleReader) Next() (rowID int32, value int64, err error) {
	buf, err := tr.buf.nextChunk(12)
	if err != nil {
		return 0, 0, err
	}
	rowID = int32(binary.LittleEndian.Uint32(buf[0:4]))
	value = int64(binary.LittleEndian.Uint64(buf[4:12]))
	return rowID, value, nil
}

type Float64TupleWriter struct {
	w       io.Writer
	hasLast bool
	last    int32
}

func NewFloat64TupleWriter(w io.Writer) *Float64TupleWriter { return &Float64TupleWriter{w: w} }

func (tw *Float64TupleWriter) Write(rowID int32, value float64) error {
	if err := validateMonotonicRowID(tw.hasLast, tw.last, rowID); err != nil {
		return err
	}
	var buf [12]byte
	binary.LittleEndian.PutUint32(buf[0:4], uint32(rowID))
	binary.LittleEndian.PutUint64(buf[4:12], math.Float64bits(value))
	if err := writeAll(tw.w, buf[:]); err != nil {
		return fmt.Errorf("writing float64 tuple: %w", err)
	}
	tw.last = rowID
	tw.hasLast = true
	return nil
}

type Float64TupleReader struct {
	buf fixedTupleBuffer
}

func NewFloat64TupleReader(r io.Reader) *Float64TupleReader {
	return &Float64TupleReader{buf: newFixedTupleBuffer(r, 12)}
}

func (tr *Float64TupleReader) Next() (rowID int32, value float64, err error) {
	buf, err := tr.buf.nextChunk(12)
	if err != nil {
		return 0, 0, err
	}
	rowID = int32(binary.LittleEndian.Uint32(buf[0:4]))
	value = math.Float64frombits(binary.LittleEndian.Uint64(buf[4:12]))
	return rowID, value, nil
}

type BoolTupleWriter struct {
	w       io.Writer
	hasLast bool
	last    int32
}

func NewBoolTupleWriter(w io.Writer) *BoolTupleWriter { return &BoolTupleWriter{w: w} }

func (tw *BoolTupleWriter) Write(rowID int32, value bool) error {
	if err := validateMonotonicRowID(tw.hasLast, tw.last, rowID); err != nil {
		return err
	}
	var buf [8]byte
	binary.LittleEndian.PutUint32(buf[0:4], uint32(rowID))
	if value {
		binary.LittleEndian.PutUint32(buf[4:8], 1)
	}
	if err := writeAll(tw.w, buf[:]); err != nil {
		return fmt.Errorf("writing bool tuple: %w", err)
	}
	tw.last = rowID
	tw.hasLast = true
	return nil
}

type BoolTupleReader struct {
	buf fixedTupleBuffer
}

func NewBoolTupleReader(r io.Reader) *BoolTupleReader {
	return &BoolTupleReader{buf: newFixedTupleBuffer(r, 8)}
}

func (tr *BoolTupleReader) Next() (rowID int32, value bool, err error) {
	buf, err := tr.buf.nextChunk(8)
	if err != nil {
		return 0, false, err
	}
	rowID = int32(binary.LittleEndian.Uint32(buf[0:4]))
	raw := binary.LittleEndian.Uint32(buf[4:8])
	switch raw {
	case 0:
		return rowID, false, nil
	case 1:
		return rowID, true, nil
	default:
		return 0, false, fmt.Errorf("invalid bool tuple value %d", raw)
	}
}
