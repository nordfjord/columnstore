package filter

import (
	"fmt"
	"strings"

	"github.com/nordfjord/columnstore/internal/colkind"
)

type AndFilter struct {
	Exprs []Filter
}

func And(exprs ...Filter) AndFilter {
	return AndFilter{Exprs: exprs}
}

func (f AndFilter) Match(values ValueLookup) bool {
	for _, expr := range f.Exprs {
		if !expr.Match(values) {
			return false
		}
	}
	return true
}

func (f AndFilter) Columns() []string {
	set := map[string]struct{}{}
	for _, expr := range f.Exprs {
		for _, c := range expr.Columns() {
			set[c] = struct{}{}
		}
	}
	cols := make([]string, 0, len(set))
	for c := range set {
		cols = append(cols, c)
	}
	return cols
}

func (f AndFilter) Validate() error {
	if len(f.Exprs) == 0 {
		return fmt.Errorf("and filter requires exprs")
	}
	for i, expr := range f.Exprs {
		if expr == nil {
			return fmt.Errorf("and filter expr %d is nil", i)
		}
		if err := expr.Validate(); err != nil {
			return err
		}
	}
	return nil
}

func (f AndFilter) CanMatch(columns map[string]colkind.ColumnKind) bool {
	for _, expr := range f.Exprs {
		if !expr.CanMatch(columns) {
			return false
		}
	}
	return true
}

func (f AndFilter) Optimize(columns map[string]colkind.ColumnKind) Filter {
	exprs := make([]Filter, 0, len(f.Exprs))
	for _, expr := range f.Exprs {
		exprs = append(exprs, expr.Optimize(columns))
	}
	return And(exprs...)
}

func (f AndFilter) String() string {
	var parts []string
	for _, expr := range f.Exprs {
		parts = append(parts, expr.String())
	}
	return fmt.Sprintf("(And %s)", strings.Join(parts, " "))
}

type OrFilter struct {
	Exprs []Filter
}

func Or(exprs ...Filter) OrFilter {
	return OrFilter{Exprs: exprs}
}
func (f OrFilter) Match(values ValueLookup) bool {
	for _, expr := range f.Exprs {
		if expr.Match(values) {
			return true
		}
	}
	return false
}

func (f OrFilter) Columns() []string {
	set := map[string]struct{}{}
	for _, expr := range f.Exprs {
		for _, c := range expr.Columns() {
			set[c] = struct{}{}
		}
	}
	cols := make([]string, 0, len(set))
	for c := range set {
		cols = append(cols, c)
	}
	return cols
}

func (f OrFilter) Validate() error {
	if len(f.Exprs) == 0 {
		return fmt.Errorf("or filter requires exprs")
	}
	for i, expr := range f.Exprs {
		if expr == nil {
			return fmt.Errorf("or filter expr %d is nil", i)
		}
		if err := expr.Validate(); err != nil {
			return err
		}
	}
	return nil
}

func (f OrFilter) CanMatch(columns map[string]colkind.ColumnKind) bool {
	for _, expr := range f.Exprs {
		if expr.CanMatch(columns) {
			return true
		}
	}
	return false
}

func (f OrFilter) Optimize(columns map[string]colkind.ColumnKind) Filter {
	exprs := make([]Filter, 0, len(f.Exprs))
	for _, expr := range f.Exprs {
		if !expr.CanMatch(columns) {
			continue
		}
		exprs = append(exprs, expr.Optimize(columns))
	}
	return Or(exprs...)
}

func (f OrFilter) String() string {
	var parts []string
	for _, expr := range f.Exprs {
		parts = append(parts, expr.String())
	}
	return fmt.Sprintf("(Or %s)", strings.Join(parts, " "))
}
