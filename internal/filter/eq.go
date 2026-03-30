package filter

import (
	"fmt"

	"github.com/nordfjord/columnstore/internal/colkind"
)

func equalValues(a, b any) bool {
	switch av := a.(type) {
	case int64:
		bv, ok := b.(int64)
		return ok && av == bv
	case float64:
		bv, ok := b.(float64)
		return ok && av == bv
	case string:
		bv, ok := b.(string)
		return ok && av == bv
	case bool:
		bv, ok := b.(bool)
		return ok && av == bv
	case nil:
		return b == nil
	}

	return false
}

// EqFilter represents Eq(column, value).
type EqFilter struct {
	Column string
	Value  any
}

// Eq constructs an Eq filter.
func Eq(column string, value any) EqFilter {
	return EqFilter{Column: column, Value: value}
}

// Match evaluates Eq semantics for present, non-nil equality values.
func (f EqFilter) Match(values ValueLookup) bool {
	if values == nil {
		return false
	}

	actual, exists := values.Lookup(f.Column)
	if !exists {
		return false
	}

	if f.Value == nil {
		return false
	}

	return equalValues(actual, f.Value)
}

// Columns returns referenced columns for this filter.
func (f EqFilter) Columns() []string {
	if f.Column == "" {
		return nil
	}
	return []string{f.Column}
}

// Validate validates the Eq filter.
func (f EqFilter) Validate() error {
	if f.Column == "" {
		return fmt.Errorf("eq filter requires column")
	}
	if f.Value == nil {
		return fmt.Errorf("eq filter requires non-nil value")
	}
	return nil
}

func (f EqFilter) CanMatch(columns map[string]colkind.ColumnKind) bool {
	if f.Value == nil {
		return false
	}
	kind, exists := columns[f.Column]
	if !exists {
		return false
	}
	_, ok := NormalizeFilterValueForKind(f.Value, kind)
	return ok
}

func (f EqFilter) Optimize(columns map[string]colkind.ColumnKind) Filter {
	kind, exists := columns[f.Column]
	if !exists {
		return f
	}
	value, _ := NormalizeFilterValueForKind(f.Value, kind)
	return Eq(f.Column, value)
}

func (f EqFilter) String() string {
	return fmt.Sprintf("(Eq %s %v)", f.Column, f.Value)
}
