package format

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

const MaxStringBytes = 1 << 20 // 1 MiB
const stringTupleReadBatchBytes = 1 << 20

type StringTupleWriter struct {
	w       io.Writer
	hasLast bool
	last    int32
}

func NewStringTupleWriter(w io.Writer) *StringTupleWriter { return &StringTupleWriter{w: w} }

func (tw *StringTupleWriter) Write(rowID int32, value string) error {
	if err := validateMonotonicRowID(tw.hasLast, tw.last, rowID); err != nil {
		return err
	}
	if len(value) > MaxStringBytes {
		value = value[:MaxStringBytes]
	}
	var header [8]byte
	binary.LittleEndian.PutUint32(header[0:4], uint32(rowID))
	binary.LittleEndian.PutUint32(header[4:8], uint32(len(value)))
	if err := writeAll(tw.w, header[:]); err != nil {
		return fmt.Errorf("writing string tuple header: %w", err)
	}
	if err := writeAll(tw.w, []byte(value)); err != nil {
		return fmt.Errorf("writing string tuple payload: %w", err)
	}
	tw.last = rowID
	tw.hasLast = true
	return nil
}

type StringTupleReader struct {
	r   io.Reader
	buf []byte
	pos int
	end int
	eof bool
}

func NewStringTupleReader(r io.Reader) *StringTupleReader {
	bufferBytes := max(stringTupleReadBatchBytes, MaxStringBytes+8)
	return &StringTupleReader{r: r, buf: make([]byte, bufferBytes)}
}

func (tr *StringTupleReader) NextBytes() (rowID int32, value []byte, err error) {
	if err := tr.ensure(8); err != nil {
		return 0, nil, err
	}
	header := tr.buf[tr.pos : tr.pos+8]
	rowID = int32(binary.LittleEndian.Uint32(header[0:4]))
	length := int32(binary.LittleEndian.Uint32(header[4:8]))
	if length < 0 {
		return 0, nil, fmt.Errorf("invalid string tuple length %d", length)
	}
	if length > MaxStringBytes {
		return 0, nil, fmt.Errorf("string tuple length %d exceeds max %d", length, MaxStringBytes)
	}

	tupleBytes := 8 + int(length)
	if err := tr.ensure(tupleBytes); err != nil {
		if errors.Is(err, io.EOF) {
			return 0, nil, io.ErrUnexpectedEOF
		}
		return 0, nil, err
	}
	b := tr.buf[tr.pos+8 : tr.pos+tupleBytes]
	tr.pos += tupleBytes
	return rowID, b, nil
}

func (tr *StringTupleReader) ensure(need int) error {
	available := tr.end - tr.pos
	if available >= need {
		return nil
	}

	if tr.eof {
		if available == 0 {
			return io.EOF
		}
		return io.ErrUnexpectedEOF
	}

	if available > 0 {
		copy(tr.buf[:available], tr.buf[tr.pos:tr.end])
	}
	tr.pos = 0
	tr.end = available

	for tr.end < need {
		n, err := tr.r.Read(tr.buf[tr.end:])
		if n > 0 {
			tr.end += n
		}
		if err == nil {
			continue
		}
		if errors.Is(err, io.EOF) {
			tr.eof = true
			if tr.end >= need {
				return nil
			}
			if tr.end == 0 {
				return io.EOF
			}
			return io.ErrUnexpectedEOF
		}
		return err
	}

	return nil
}

func (tr *StringTupleReader) Next() (rowID int32, value string, err error) {
	rowID, b, err := tr.NextBytes()
	if err != nil {
		return 0, "", err
	}
	return rowID, string(b), nil
}
