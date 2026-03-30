package columnstore

import (
	"fmt"
	"strings"

	"github.com/nordfjord/columnstore/internal/colkind"
	"github.com/nordfjord/columnstore/internal/filter"
	"github.com/nordfjord/columnstore/internal/scan"
)

type compiledRowFilter interface {
	MatchRow(rowID int32) (bool, error)
}

type partitionRowFilter struct {
	filter compiledRowFilter
}

func newPartitionRowFilter(
	columns map[string]bool,
	filter filter.Filter,
	getColumnScanner func(column string) (*openedColumnScanner, bool, error),
) (*partitionRowFilter, bool, error) {
	columnKinds, err := newPartitionColumnKinds(columns)
	if err != nil {
		return nil, false, err
	}
	if !filter.CanMatch(columnKinds) {
		return nil, false, nil
	}

	normalizedFilter := filter.Optimize(columnKinds)
	compiled, err := compilePartitionFilter(normalizedFilter, getColumnScanner)
	if err != nil {
		return nil, false, err
	}
	return &partitionRowFilter{filter: compiled}, true, nil
}

func newPartitionColumnKinds(columns map[string]bool) (map[string]colkind.ColumnKind, error) {
	kinds := make(map[string]colkind.ColumnKind, len(columns))
	for name := range columns {
		for kind, ext := range colkind.KindToExt {
			suffix := "." + ext
			if !strings.HasSuffix(name, suffix) {
				continue
			}
			column := strings.TrimSuffix(name, suffix)
			if existing, ok := kinds[column]; ok && existing != kind {
				return nil, fmt.Errorf("multiple type files found for column %q", column)
			}
			kinds[column] = kind
		}
	}
	return kinds, nil
}

func (e *partitionRowFilter) Match(rowID int32) (bool, error) {
	return e.filter.MatchRow(rowID)
}

func compilePartitionFilter(
	f filter.Filter,
	getColumnScanner func(column string) (*openedColumnScanner, bool, error),
) (compiledRowFilter, error) {
	switch f := f.(type) {
	case filter.EqFilter:
		return compileEqFilter(f, getColumnScanner)
	case filter.ExistsFilter:
		return compileExistsFilter(f.Column, false, getColumnScanner)
	case filter.NotExistsFilter:
		return compileExistsFilter(f.Column, true, getColumnScanner)
	case filter.InFilter:
		return compileInFilter(f, getColumnScanner)
	case filter.NotInFilter:
		return compileNotInFilter(f, getColumnScanner)
	case filter.CompareFilter[int64]:
		return compileCompareFilter(f.Type, f.Column, f.Value, getColumnScanner)
	case filter.CompareFilter[float64]:
		return compileCompareFilter(f.Type, f.Column, f.Value, getColumnScanner)
	case filter.CompareFilter[string]:
		return compileCompareFilter(f.Type, f.Column, f.Value, getColumnScanner)
	case filter.AndFilter:
		children := make([]compiledRowFilter, 0, len(f.Exprs))
		for _, expr := range f.Exprs {
			child, err := compilePartitionFilter(expr, getColumnScanner)
			if err != nil {
				return nil, err
			}
			children = append(children, child)
		}
		return compiledAndFilter(children), nil
	case filter.OrFilter:
		children := make([]compiledRowFilter, 0, len(f.Exprs))
		for _, expr := range f.Exprs {
			child, err := compilePartitionFilter(expr, getColumnScanner)
			if err != nil {
				return nil, err
			}
			children = append(children, child)
		}
		return compiledOrFilter(children), nil
	default:
		return nil, fmt.Errorf("unsupported compiled filter %T", f)
	}
}

type comparableValueScanner[T comparable] interface {
	AdvanceTo(target int32) (bool, error)
	Value() T
}

type orderedValueScanner[T filter.Ordered] interface {
	AdvanceTo(target int32) (bool, error)
	Value() T
}

type compiledConstFilter bool

func (f compiledConstFilter) MatchRow(rowID int32) (bool, error) {
	return bool(f), nil
}

type compiledExistsLeaf struct {
	scanner metricValueScanner
	negate  bool
}

func (f compiledExistsLeaf) MatchRow(rowID int32) (bool, error) {
	matched, err := f.scanner.AdvanceTo(rowID)
	if err != nil {
		return false, err
	}
	if f.negate {
		return !matched, nil
	}
	return matched, nil
}

type compiledEqLeaf[T comparable] struct {
	scanner comparableValueScanner[T]
	value   T
}

func (f compiledEqLeaf[T]) MatchRow(rowID int32) (bool, error) {
	matched, err := f.scanner.AdvanceTo(rowID)
	if err != nil || !matched {
		return false, err
	}
	return f.scanner.Value() == f.value, nil
}

type compiledSetLeaf[T comparable] struct {
	scanner comparableValueScanner[T]
	values  map[T]struct{}
	negate  bool
}

func (f compiledSetLeaf[T]) MatchRow(rowID int32) (bool, error) {
	matched, err := f.scanner.AdvanceTo(rowID)
	if err != nil {
		return false, err
	}
	if !matched {
		return f.negate, nil
	}
	_, exists := f.values[f.scanner.Value()]
	if f.negate {
		return !exists, nil
	}
	return exists, nil
}

type compiledCompareLeaf[T filter.Ordered] struct {
	scanner orderedValueScanner[T]
	op      string
	value   T
}

func (f compiledCompareLeaf[T]) MatchRow(rowID int32) (bool, error) {
	matched, err := f.scanner.AdvanceTo(rowID)
	if err != nil || !matched {
		return false, err
	}
	value := f.scanner.Value()
	switch f.op {
	case filter.FilterOpLt:
		return value < f.value, nil
	case filter.FilterOpLte:
		return value <= f.value, nil
	case filter.FilterOpGt:
		return value > f.value, nil
	case filter.FilterOpGte:
		return value >= f.value, nil
	default:
		return false, fmt.Errorf("unsupported compare op %q", f.op)
	}
}

type compiledAndFilter []compiledRowFilter

func (f compiledAndFilter) MatchRow(rowID int32) (bool, error) {
	for _, child := range f {
		matched, err := child.MatchRow(rowID)
		if err != nil {
			return false, err
		}
		if !matched {
			return false, nil
		}
	}
	return true, nil
}

type compiledOrFilter []compiledRowFilter

func (f compiledOrFilter) MatchRow(rowID int32) (bool, error) {
	for _, child := range f {
		matched, err := child.MatchRow(rowID)
		if err != nil {
			return false, err
		}
		if matched {
			return true, nil
		}
	}
	return false, nil
}

func compileExistsFilter(
	column string,
	negate bool,
	getColumnScanner func(column string) (*openedColumnScanner, bool, error),
) (compiledRowFilter, error) {
	entry, exists, err := getColumnScanner(column)
	if err != nil {
		return nil, err
	}
	if !exists {
		return compiledConstFilter(negate), nil
	}
	return compiledExistsLeaf{scanner: entry.scanner, negate: negate}, nil
}

func compileEqFilter(
	f filter.EqFilter,
	getColumnScanner func(column string) (*openedColumnScanner, bool, error),
) (compiledRowFilter, error) {
	entry, exists, err := getColumnScanner(f.Column)
	if err != nil {
		return nil, err
	}
	if !exists {
		return compiledConstFilter(false), nil
	}

	value, ok := filter.NormalizeFilterValueForKind(f.Value, entry.kind)
	if !ok || value == nil {
		return compiledConstFilter(false), nil
	}

	switch entry.kind {
	case colkind.KindInt64:
		return compiledEqLeaf[int64]{scanner: entry.scanner.(*scan.Int64Scanner), value: value.(int64)}, nil
	case colkind.KindFloat64:
		return compiledEqLeaf[float64]{scanner: entry.scanner.(*scan.Float64Scanner), value: value.(float64)}, nil
	case colkind.KindBool:
		return compiledEqLeaf[bool]{scanner: entry.scanner.(*scan.BoolScanner), value: value.(bool)}, nil
	case colkind.KindString:
		return compiledEqLeaf[string]{scanner: entry.scanner.(*scan.StringScanner), value: value.(string)}, nil
	default:
		return nil, fmt.Errorf("unsupported eq column kind %d for %s", entry.kind, f.Column)
	}
}

func compileInFilter(
	f filter.InFilter,
	getColumnScanner func(column string) (*openedColumnScanner, bool, error),
) (compiledRowFilter, error) {
	entry, exists, err := getColumnScanner(f.Column)
	if err != nil {
		return nil, err
	}
	if !exists {
		return compiledConstFilter(false), nil
	}

	values := filter.NormalizeFilterValuesForKind(f.Values, entry.kind)
	if len(values) == 0 {
		return compiledConstFilter(false), nil
	}

	switch entry.kind {
	case colkind.KindInt64:
		set := make(map[int64]struct{}, len(values))
		for _, value := range values {
			set[value.(int64)] = struct{}{}
		}
		return compiledSetLeaf[int64]{scanner: entry.scanner.(*scan.Int64Scanner), values: set}, nil
	case colkind.KindFloat64:
		set := make(map[float64]struct{}, len(values))
		for _, value := range values {
			set[value.(float64)] = struct{}{}
		}
		return compiledSetLeaf[float64]{scanner: entry.scanner.(*scan.Float64Scanner), values: set}, nil
	case colkind.KindBool:
		set := make(map[bool]struct{}, len(values))
		for _, value := range values {
			set[value.(bool)] = struct{}{}
		}
		return compiledSetLeaf[bool]{scanner: entry.scanner.(*scan.BoolScanner), values: set}, nil
	case colkind.KindString:
		set := make(map[string]struct{}, len(values))
		for _, value := range values {
			set[value.(string)] = struct{}{}
		}
		return compiledSetLeaf[string]{scanner: entry.scanner.(*scan.StringScanner), values: set}, nil
	default:
		return nil, fmt.Errorf("unsupported in column kind %d for %s", entry.kind, f.Column)
	}
}

func compileNotInFilter(
	f filter.NotInFilter,
	getColumnScanner func(column string) (*openedColumnScanner, bool, error),
) (compiledRowFilter, error) {
	entry, exists, err := getColumnScanner(f.Column)
	if err != nil {
		return nil, err
	}
	if !exists {
		return compiledConstFilter(true), nil
	}

	values := filter.NormalizeFilterValuesForKind(f.Values, entry.kind)
	if len(values) == 0 {
		return compiledConstFilter(true), nil
	}

	switch entry.kind {
	case colkind.KindInt64:
		set := make(map[int64]struct{}, len(values))
		for _, value := range values {
			set[value.(int64)] = struct{}{}
		}
		return compiledSetLeaf[int64]{scanner: entry.scanner.(*scan.Int64Scanner), values: set, negate: true}, nil
	case colkind.KindFloat64:
		set := make(map[float64]struct{}, len(values))
		for _, value := range values {
			set[value.(float64)] = struct{}{}
		}
		return compiledSetLeaf[float64]{scanner: entry.scanner.(*scan.Float64Scanner), values: set, negate: true}, nil
	case colkind.KindBool:
		set := make(map[bool]struct{}, len(values))
		for _, value := range values {
			set[value.(bool)] = struct{}{}
		}
		return compiledSetLeaf[bool]{scanner: entry.scanner.(*scan.BoolScanner), values: set, negate: true}, nil
	case colkind.KindString:
		set := make(map[string]struct{}, len(values))
		for _, value := range values {
			set[value.(string)] = struct{}{}
		}
		return compiledSetLeaf[string]{scanner: entry.scanner.(*scan.StringScanner), values: set, negate: true}, nil
	default:
		return nil, fmt.Errorf("unsupported not-in column kind %d for %s", entry.kind, f.Column)
	}
}

func compileCompareFilter[T filter.Ordered](
	op string,
	column string,
	value T,
	getColumnScanner func(column string) (*openedColumnScanner, bool, error),
) (compiledRowFilter, error) {
	entry, exists, err := getColumnScanner(column)
	if err != nil {
		return nil, err
	}
	if !exists {
		return compiledConstFilter(false), nil
	}

	normalized, ok := filter.NormalizeFilterValueForKind(value, entry.kind)
	if !ok || normalized == nil {
		return compiledConstFilter(false), nil
	}

	switch entry.kind {
	case colkind.KindInt64:
		return compiledCompareLeaf[int64]{scanner: entry.scanner.(*scan.Int64Scanner), op: op, value: normalized.(int64)}, nil
	case colkind.KindFloat64:
		return compiledCompareLeaf[float64]{scanner: entry.scanner.(*scan.Float64Scanner), op: op, value: normalized.(float64)}, nil
	case colkind.KindString:
		return compiledCompareLeaf[string]{scanner: entry.scanner.(*scan.StringScanner), op: op, value: normalized.(string)}, nil
	default:
		return nil, fmt.Errorf("unsupported compare column kind %d for %s", entry.kind, column)
	}
}
