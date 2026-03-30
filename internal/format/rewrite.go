package format

import (
	"errors"
	"fmt"
	"io"

	"github.com/nordfjord/columnstore/internal/colkind"
)

func RewriteColumnTuples(out io.Writer, in io.Reader, kind colkind.ColumnKind, keepRowIDs map[int32]struct{}) error {
	switch kind {
	case colkind.KindInt64:
		tr := NewInt64TupleReader(in)
		tw := NewInt64TupleWriter(out)
		return rewriteTypedTuples(tr.Next, tw.Write, keepRowIDs)
	case colkind.KindFloat64:
		tr := NewFloat64TupleReader(in)
		tw := NewFloat64TupleWriter(out)
		return rewriteTypedTuples(tr.Next, tw.Write, keepRowIDs)
	case colkind.KindBool:
		tr := NewBoolTupleReader(in)
		tw := NewBoolTupleWriter(out)
		return rewriteTypedTuples(tr.Next, tw.Write, keepRowIDs)
	case colkind.KindString:
		tr := NewStringTupleReader(in)
		tw := NewStringTupleWriter(out)
		return rewriteTypedTuples(tr.Next, tw.Write, keepRowIDs)
	default:
		return fmt.Errorf("unsupported tuple kind %d", kind)
	}
}

func rewriteTypedTuples[T any](
	next func() (int32, T, error),
	write func(int32, T) error,
	keepRowIDs map[int32]struct{},
) error {
	for {
		rowID, value, err := next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		if _, ok := keepRowIDs[rowID]; !ok {
			continue
		}
		if err := write(rowID, value); err != nil {
			return err
		}
	}
}
