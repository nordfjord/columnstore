package colkind

import (
	"fmt"
	"path/filepath"
	"strings"
)

type ColumnKind uint8

const (
	KindInt64   ColumnKind = 1
	KindFloat64 ColumnKind = 2
	KindBool    ColumnKind = 3
	KindString  ColumnKind = 4
)

var KindToExt = map[ColumnKind]string{
	KindInt64:   "int64",
	KindFloat64: "float64",
	KindBool:    "bool",
	KindString:  "str",
}

var orderedColumnKinds = []struct {
	kind ColumnKind
	ext  string
}{
	{kind: KindInt64, ext: "int64"},
	{kind: KindFloat64, ext: "float64"},
	{kind: KindBool, ext: "bool"},
	{kind: KindString, ext: "str"},
}

func InferColumnKind(value any) (kind ColumnKind, ext string, err error) {
	switch value.(type) {
	case int64:
		return KindInt64, "int64", nil
	case float64:
		return KindFloat64, "float64", nil
	case bool:
		return KindBool, "bool", nil
	case string:
		return KindString, "str", nil
	default:
		return 0, "", fmt.Errorf("unsupported column value type %T", value)
	}
}

func ResolveColumnKind(columns map[string]bool, column string) (ColumnKind, bool, error) {
	var (
		kind  ColumnKind
		found bool
	)
	for _, candidate := range orderedColumnKinds {
		if !columns[column+"."+candidate.ext] {
			continue
		}
		if found {
			return 0, false, fmt.Errorf("multiple type files found for column %q", column)
		}
		found = true
		kind = candidate.kind
	}
	return kind, found, nil
}

func ResolveColumnKindFromPath(path string) (ColumnKind, error) {
	ext := strings.TrimPrefix(filepath.Ext(path), ".")
	for kindRaw, candidate := range KindToExt {
		if candidate == ext {
			return kindRaw, nil
		}
	}
	return 0, fmt.Errorf("unsupported column file extension for %s", path)
}
