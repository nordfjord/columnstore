package filter

import (
	"fmt"

	"github.com/nordfjord/columnstore/internal/colkind"
)

type InFilter struct {
	Column string
	Values []any
}

func In(column string, values ...any) InFilter {
	return InFilter{Column: column, Values: values}
}

func (f InFilter) Match(values ValueLookup) bool {
	actual, exists := values.Lookup(f.Column)
	if !exists {
		return false
	}
	for _, v := range f.Values {
		if equalValues(actual, v) {
			return true
		}
	}
	return false
}

func (f InFilter) Columns() []string {
	if f.Column == "" {
		return nil
	}
	return []string{f.Column}
}

func (f InFilter) Validate() error {
	if f.Column == "" {
		return fmt.Errorf("in filter requires column")
	}
	return nil
}

func (f InFilter) CanMatch(columns map[string]colkind.ColumnKind) bool {
	kind, exists := columns[f.Column]
	if !exists {
		return false
	}
	values := NormalizeFilterValuesForKind(f.Values, kind)
	return len(values) > 0
}

func (f InFilter) Optimize(columns map[string]colkind.ColumnKind) Filter {
	kind, exists := columns[f.Column]
	if !exists {
		return f
	}
	return In(f.Column, NormalizeFilterValuesForKind(f.Values, kind)...)
}

func (f InFilter) String() string {
	return fmt.Sprintf("(In %s %v)", f.Column, f.Values)
}

type NotInFilter struct {
	Column string
	Values []any
}

func NotIn(column string, values ...any) NotInFilter {
	return NotInFilter{Column: column, Values: values}
}

func (f NotInFilter) Match(values ValueLookup) bool {
	actual, exists := values.Lookup(f.Column)
	if !exists {
		return true
	}
	for _, v := range f.Values {
		if equalValues(actual, v) {
			return false
		}
	}
	return true
}

func (f NotInFilter) Columns() []string {
	if f.Column == "" {
		return nil
	}
	return []string{f.Column}
}

func (f NotInFilter) Validate() error {
	if f.Column == "" {
		return fmt.Errorf("not-in filter requires column")
	}
	return nil
}

func (f NotInFilter) CanMatch(columns map[string]colkind.ColumnKind) bool {
	return true
}

func (f NotInFilter) Optimize(columns map[string]colkind.ColumnKind) Filter {
	kind, exists := columns[f.Column]
	if !exists {
		return f
	}
	return NotIn(f.Column, NormalizeFilterValuesForKind(f.Values, kind)...)
}

func (f NotInFilter) String() string {
	return fmt.Sprintf("(NotIn %s %v)", f.Column, f.Values)
}
