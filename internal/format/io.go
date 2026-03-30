package format

import (
	"fmt"
	"io"
)

func validateMonotonicRowID(hasLast bool, last int32, rowID int32) error {
	if rowID < 0 {
		return fmt.Errorf("rowid must be >= 0")
	}
	if hasLast && rowID <= last {
		return fmt.Errorf("rowid must be strictly increasing: last=%d next=%d", last, rowID)
	}
	return nil
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
