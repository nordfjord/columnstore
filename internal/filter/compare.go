package filter

import (
	"fmt"

	"github.com/nordfjord/columnstore/internal/colkind"
)

type Ordered interface {
	~int64 | ~float64 | ~string
}

type CompareFilter[T Ordered] struct {
	Type   string
	Column string
	Value  T
	Op     func(T, T) bool
}

func Lt[T Ordered](column string, value T) CompareFilter[T] {
	return CompareFilter[T]{Type: FilterOpLt, Column: column, Value: value, Op: func(a, b T) bool { return a < b }}
}
func Lte[T Ordered](column string, value T) CompareFilter[T] {
	return CompareFilter[T]{Type: FilterOpLte, Column: column, Value: value, Op: func(a, b T) bool { return a <= b }}
}
func Gt[T Ordered](column string, value T) CompareFilter[T] {
	return CompareFilter[T]{Type: FilterOpGt, Column: column, Value: value, Op: func(a, b T) bool { return a > b }}
}
func Gte[T Ordered](column string, value T) CompareFilter[T] {
	return CompareFilter[T]{Type: FilterOpGte, Column: column, Value: value, Op: func(a, b T) bool { return a >= b }}
}

func (f CompareFilter[T]) Match(values ValueLookup) bool {
	if values == nil {
		return false
	}
	actual, exists := values.Lookup(f.Column)
	if !exists {
		return false
	}
	value, ok := actual.(T)
	if !ok {
		return false
	}
	return f.Op(value, f.Value)
}

func (f CompareFilter[T]) Validate() error {
	if f.Column == "" {
		return fmt.Errorf("compare filter requires column")
	}
	if f.Op == nil {
		return fmt.Errorf("compare filter requires op")
	}
	return nil
}

func (f CompareFilter[T]) Columns() []string {
	if f.Column == "" {
		return nil
	}
	return []string{f.Column}
}

func (f CompareFilter[T]) CanMatch(columns map[string]colkind.ColumnKind) bool {
	kind, exists := columns[f.Column]
	if !exists {
		return false
	}
	return buildOptimizedCompareFilter(f.Type, f.Column, f.Value, kind) != nil
}

func (f CompareFilter[T]) Optimize(columns map[string]colkind.ColumnKind) Filter {
	kind, exists := columns[f.Column]
	if !exists {
		return f
	}
	optimized := buildOptimizedCompareFilter(f.Type, f.Column, f.Value, kind)
	if optimized == nil {
		return f
	}
	return optimized
}

func (f CompareFilter[T]) String() string {
	return fmt.Sprintf("(%s %s %v)", f.Type, f.Column, f.Value)
}

func buildOptimizedCompareFilter(op, column string, value any, kind colkind.ColumnKind) Filter {
	normalized, ok := NormalizeFilterValueForKind(value, kind)
	if !ok || normalized == nil {
		return nil
	}

	switch kind {
	case colkind.KindInt64:
		v, ok := normalized.(int64)
		if !ok {
			return nil
		}
		return newCompareFilterForOp(op, column, v)
	case colkind.KindFloat64:
		v, ok := normalized.(float64)
		if !ok {
			return nil
		}
		return newCompareFilterForOp(op, column, v)
	case colkind.KindString:
		v, ok := normalized.(string)
		if !ok {
			return nil
		}
		return newCompareFilterForOp(op, column, v)
	default:
		return nil
	}
}

func newCompareFilterForOp[T Ordered](op, column string, value T) Filter {
	switch op {
	case FilterOpLt:
		return Lt(column, value)
	case FilterOpLte:
		return Lte(column, value)
	case FilterOpGt:
		return Gt(column, value)
	case FilterOpGte:
		return Gte(column, value)
	default:
		return nil
	}
}
