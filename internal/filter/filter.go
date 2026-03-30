// Package filter provides internal filter types and helpers for columnstore.
package filter

import colkind "github.com/nordfjord/columnstore/internal/colkind"

const (
	FilterOpEq        = "eq"
	FilterOpExists    = "exists"
	FilterOpNotExists = "not-exists"
	FilterOpIn        = "in"
	FilterOpNotIn     = "not-in"
	FilterOpLt        = "lt"
	FilterOpLte       = "lte"
	FilterOpGt        = "gt"
	FilterOpGte       = "gte"
	FilterOpAnd       = "and"
	FilterOpOr        = "or"
)

// Filter is the typed query filter AST used by columnstore.
type Filter interface {
	Match(values ValueLookup) bool
	Columns() []string
	Validate() error
	CanMatch(columns map[string]colkind.ColumnKind) bool
	Optimize(columns map[string]colkind.ColumnKind) Filter
	String() string
}

// ValueLookup resolves values for a logical column.
// The bool return indicates whether the column is present for the current row.
type ValueLookup interface {
	Lookup(column string) (any, bool)
}

// MapLookup is a simple ValueLookup backed by a map.
type MapLookup map[string]any

// Lookup returns the value for a column and whether it exists.
func (m MapLookup) Lookup(column string) (any, bool) {
	v, ok := m[column]
	return v, ok
}
