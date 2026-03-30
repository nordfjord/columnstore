package filter

import (
	"fmt"

	"github.com/nordfjord/columnstore/internal/colkind"
)

type ExistsFilter struct {
	Column string
}

func Exists(column string) ExistsFilter {
	return ExistsFilter{Column: column}
}

func (f ExistsFilter) Match(values ValueLookup) bool {
	actual, exists := values.Lookup(f.Column)
	return exists && actual != nil
}

func (f ExistsFilter) Columns() []string {
	if f.Column == "" {
		return nil
	}
	return []string{f.Column}
}

func (f ExistsFilter) Validate() error {
	if f.Column == "" {
		return fmt.Errorf("exists filter requires column")
	}
	return nil
}

func (f ExistsFilter) CanMatch(columns map[string]colkind.ColumnKind) bool {
	_, exists := columns[f.Column]
	return exists
}

func (f ExistsFilter) Optimize(columns map[string]colkind.ColumnKind) Filter {
	return f
}

func (f ExistsFilter) String() string {
	return fmt.Sprintf("(Exists %s)", f.Column)
}

type NotExistsFilter struct {
	Column string
}

func NotExists(column string) NotExistsFilter {
	return NotExistsFilter{Column: column}
}

func (f NotExistsFilter) Match(values ValueLookup) bool {
	actual, exists := values.Lookup(f.Column)
	return !exists || actual == nil
}

func (f NotExistsFilter) Columns() []string {
	if f.Column == "" {
		return nil
	}
	return []string{f.Column}
}

func (f NotExistsFilter) Validate() error {
	if f.Column == "" {
		return fmt.Errorf("not-exists filter requires column")
	}
	return nil
}

func (f NotExistsFilter) CanMatch(columns map[string]colkind.ColumnKind) bool {
	return true
}

func (f NotExistsFilter) Optimize(columns map[string]colkind.ColumnKind) Filter {
	return f
}

func (f NotExistsFilter) String() string {
	return fmt.Sprintf("(NotExists %s)", f.Column)
}
