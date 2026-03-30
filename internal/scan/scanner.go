package scan

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"sync/atomic"

	format "github.com/nordfjord/columnstore/internal/format"
	hashutil "github.com/nordfjord/columnstore/internal/hashutil"
)

const ScannerBufferSize = 512 * 1024

// Int64Scanner is a forward-only tuple scanner.
type Int64Scanner struct {
	r     *format.Int64TupleReader
	rowID int32
	value int64
	valid bool
	eof   bool
}

func NewInt64Scanner(r io.Reader) *Int64Scanner {
	return &Int64Scanner{r: format.NewInt64TupleReader(r)}
}

func OpenInt64Scanner(path string) (*Int64Scanner, io.Closer, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, nil, err
	}

	return NewInt64Scanner(bufio.NewReaderSize(f, ScannerBufferSize)), f, nil
}

func (s *Int64Scanner) Next() (bool, error) {
	if s.eof {
		s.valid = false
		return false, nil
	}

	rowID, value, err := s.r.Next()
	if err != nil {
		if errors.Is(err, io.EOF) {
			s.eof = true
			s.valid = false
			return false, nil
		}
		return false, err
	}

	if s.valid && rowID <= s.rowID {
		return false, fmt.Errorf("non-monotonic rowid stream: prev=%d next=%d", s.rowID, rowID)
	}

	s.rowID = rowID
	s.value = value
	s.valid = true
	return true, nil
}

func (s *Int64Scanner) AdvanceTo(target int32) (bool, error) {
	if !s.valid {
		ok, err := s.Next()
		if err != nil || !ok {
			return false, err
		}
	}

	for s.valid && s.rowID < target {
		ok, err := s.Next()
		if err != nil {
			return false, err
		}
		if !ok {
			return false, nil
		}
	}

	return s.valid && s.rowID == target, nil
}

func (s *Int64Scanner) RowID() int32 { return s.rowID }
func (s *Int64Scanner) Value() int64 { return s.value }
func (s *Int64Scanner) Valid() bool  { return s.valid }
func (s *Int64Scanner) ValueAny() any {
	return s.value
}

// Float64Scanner is a forward-only tuple scanner.
type Float64Scanner struct {
	r     *format.Float64TupleReader
	rowID int32
	value float64
	valid bool
	eof   bool
}

func NewFloat64Scanner(r io.Reader) *Float64Scanner {
	return &Float64Scanner{r: format.NewFloat64TupleReader(r)}
}

func OpenFloat64Scanner(path string) (*Float64Scanner, io.Closer, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, nil, err
	}

	return NewFloat64Scanner(bufio.NewReaderSize(f, ScannerBufferSize)), f, nil
}

func (s *Float64Scanner) Next() (bool, error) {
	if s.eof {
		s.valid = false
		return false, nil
	}

	rowID, value, err := s.r.Next()
	if err != nil {
		if errors.Is(err, io.EOF) {
			s.eof = true
			s.valid = false
			return false, nil
		}
		return false, err
	}

	if s.valid && rowID <= s.rowID {
		return false, fmt.Errorf("non-monotonic rowid stream: prev=%d next=%d", s.rowID, rowID)
	}

	s.rowID = rowID
	s.value = value
	s.valid = true
	return true, nil
}

func (s *Float64Scanner) AdvanceTo(target int32) (bool, error) {
	if !s.valid {
		ok, err := s.Next()
		if err != nil || !ok {
			return false, err
		}
	}

	for s.valid && s.rowID < target {
		ok, err := s.Next()
		if err != nil {
			return false, err
		}
		if !ok {
			return false, nil
		}
	}

	return s.valid && s.rowID == target, nil
}

func (s *Float64Scanner) RowID() int32   { return s.rowID }
func (s *Float64Scanner) Value() float64 { return s.value }
func (s *Float64Scanner) Valid() bool    { return s.valid }
func (s *Float64Scanner) ValueAny() any  { return s.value }

// BoolScanner is a forward-only tuple scanner.
type BoolScanner struct {
	r     *format.BoolTupleReader
	rowID int32
	value bool
	valid bool
	eof   bool
}

func NewBoolScanner(r io.Reader) *BoolScanner {
	return &BoolScanner{r: format.NewBoolTupleReader(r)}
}

func OpenBoolScanner(path string) (*BoolScanner, io.Closer, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, nil, err
	}

	return NewBoolScanner(bufio.NewReaderSize(f, ScannerBufferSize)), f, nil
}

func (s *BoolScanner) Next() (bool, error) {
	if s.eof {
		s.valid = false
		return false, nil
	}

	rowID, value, err := s.r.Next()
	if err != nil {
		if errors.Is(err, io.EOF) {
			s.eof = true
			s.valid = false
			return false, nil
		}
		return false, err
	}

	if s.valid && rowID <= s.rowID {
		return false, fmt.Errorf("non-monotonic rowid stream: prev=%d next=%d", s.rowID, rowID)
	}

	s.rowID = rowID
	s.value = value
	s.valid = true
	return true, nil
}

func (s *BoolScanner) AdvanceTo(target int32) (bool, error) {
	if !s.valid {
		ok, err := s.Next()
		if err != nil || !ok {
			return false, err
		}
	}

	for s.valid && s.rowID < target {
		ok, err := s.Next()
		if err != nil {
			return false, err
		}
		if !ok {
			return false, nil
		}
	}

	return s.valid && s.rowID == target, nil
}

func (s *BoolScanner) RowID() int32  { return s.rowID }
func (s *BoolScanner) Value() bool   { return s.value }
func (s *BoolScanner) Valid() bool   { return s.valid }
func (s *BoolScanner) ValueAny() any { return s.value }

// StringScanner is a forward-only tuple scanner.
type StringScanner struct {
	r       *format.StringTupleReader
	rowID   int32
	value   string
	valueID uint64
	valid   bool
	eof     bool
	intern  map[uint64][]internedStringEntry
	scanID  uint64
	nextID  uint64
}

type internedStringEntry struct {
	value string
	id    uint64
}

var nextStringScannerID atomic.Uint64

func NewStringScanner(r io.Reader) *StringScanner {
	return &StringScanner{
		r:      format.NewStringTupleReader(r),
		intern: make(map[uint64][]internedStringEntry, 16),
		scanID: nextStringScannerID.Add(1),
		nextID: 1,
	}
}

func OpenStringScanner(path string) (*StringScanner, io.Closer, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, nil, err
	}

	return NewStringScanner(bufio.NewReaderSize(f, ScannerBufferSize)), f, nil
}

func (s *StringScanner) Next() (bool, error) {
	if s.eof {
		s.valid = false
		return false, nil
	}

	rowID, raw, err := s.r.NextBytes()
	if err != nil {
		if errors.Is(err, io.EOF) {
			s.eof = true
			s.valid = false
			return false, nil
		}
		return false, err
	}

	if s.valid && rowID <= s.rowID {
		return false, fmt.Errorf("non-monotonic rowid stream: prev=%d next=%d", s.rowID, rowID)
	}

	value, valueID := s.internString(raw)
	s.rowID = rowID
	s.value = value
	s.valueID = valueID
	s.valid = true
	return true, nil
}

func (s *StringScanner) AdvanceTo(target int32) (bool, error) {
	if !s.valid {
		ok, err := s.Next()
		if err != nil || !ok {
			return false, err
		}
	}

	for s.valid && s.rowID < target {
		ok, err := s.Next()
		if err != nil {
			return false, err
		}
		if !ok {
			return false, nil
		}
	}

	return s.valid && s.rowID == target, nil
}

func (s *StringScanner) RowID() int32    { return s.rowID }
func (s *StringScanner) Value() string   { return s.value }
func (s *StringScanner) ValueID() uint64 { return s.valueID }
func (s *StringScanner) Valid() bool     { return s.valid }
func (s *StringScanner) ValueAny() any {
	return s.value
}

func (s *StringScanner) InternedValues() map[uint64]string {
	if s == nil || len(s.intern) == 0 {
		return nil
	}
	out := make(map[uint64]string, len(s.intern))
	for _, bucket := range s.intern {
		for i := range bucket {
			if _, exists := out[bucket[i].id]; !exists {
				out[bucket[i].id] = bucket[i].value
			}
		}
	}
	return out
}

func (s *StringScanner) internString(raw []byte) (string, uint64) {
	h := hashutil.XXH64Bytes(raw)
	bucket := s.intern[h]
	for i := range bucket {
		if stringEqualsBytes(bucket[i].value, raw) {
			return bucket[i].value, bucket[i].id
		}
	}
	value := string(raw)
	id := (s.scanID << 32) | s.nextID
	s.nextID++
	s.intern[h] = append(bucket, internedStringEntry{value: value, id: id})
	return value, id
}

func stringEqualsBytes(value string, raw []byte) bool {
	if len(value) != len(raw) {
		return false
	}
	for i := range raw {
		if value[i] != raw[i] {
			return false
		}
	}
	return true
}

// CountByTimeAndEq performs streaming merge-join style counting.
func CountByTimeAndEq(date *Int64Scanner, filter *StringScanner, start, end int64, expected string) (int64, error) {
	var count int64
	for {
		ok, err := date.Next()
		if err != nil {
			return 0, err
		}
		if !ok {
			break
		}

		ts := date.Value()
		if ts < start || ts >= end {
			continue
		}

		matched, err := filter.AdvanceTo(date.RowID())
		if err != nil {
			return 0, err
		}
		if matched && filter.Value() == expected {
			count++
		}
	}

	return count, nil
}
