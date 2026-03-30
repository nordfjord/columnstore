package columnstore

import (
	"fmt"
	"io"
	"path/filepath"

	"github.com/nordfjord/columnstore/internal/colkind"
	"github.com/nordfjord/columnstore/internal/scan"
)

type metricValueScanner interface {
	AdvanceTo(target int32) (bool, error)
	ValueAny() any
}

type openedColumnScanner struct {
	scanner metricValueScanner
	kind    colkind.ColumnKind
	closer  io.Closer
}

func openMetricScannerIfExists(partitionPath, column string, columns map[string]bool) (metricValueScanner, colkind.ColumnKind, io.Closer, bool, error) {
	kind, exists, err := colkind.ResolveColumnKind(columns, column)
	if err != nil {
		return nil, 0, nil, false, err
	}
	if !exists {
		return nil, 0, nil, false, nil
	}

	switch kind {
	case colkind.KindInt64:
		scanner, closer, err := scan.OpenInt64Scanner(filepath.Join(partitionPath, column+".int64"))
		if err != nil {
			return nil, 0, nil, false, err
		}
		return scanner, colkind.KindInt64, closer, true, nil
	case colkind.KindFloat64:
		scanner, closer, err := scan.OpenFloat64Scanner(filepath.Join(partitionPath, column+".float64"))
		if err != nil {
			return nil, 0, nil, false, err
		}
		return scanner, colkind.KindFloat64, closer, true, nil
	case colkind.KindBool:
		scanner, closer, err := scan.OpenBoolScanner(filepath.Join(partitionPath, column+".bool"))
		if err != nil {
			return nil, 0, nil, false, err
		}
		return scanner, colkind.KindBool, closer, true, nil
	case colkind.KindString:
		scanner, closer, err := scan.OpenStringScanner(filepath.Join(partitionPath, column+".str"))
		if err != nil {
			return nil, 0, nil, false, err
		}
		return scanner, colkind.KindString, closer, true, nil
	default:
		return nil, 0, nil, false, fmt.Errorf("unsupported column kind %d for %s", kind, column)
	}
}
