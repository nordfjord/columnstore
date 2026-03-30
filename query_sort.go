package columnstore

import (
	"fmt"
	"slices"
	"time"
)

type rowOrderAccessor struct {
	desc bool
	get  func(row QueryResultRow) any
}

func sortQueryRows(rows []QueryResultRow, dimensions []string, metricNames []string, orderBy []OrderBy) error {
	if len(rows) < 2 {
		return nil
	}

	accessors, err := buildRowOrderAccessors(dimensions, metricNames, orderBy)
	if err != nil {
		return err
	}

	slices.SortStableFunc(rows, func(a, b QueryResultRow) int {
		for _, accessor := range accessors {
			cmp := compareDimensionValue(accessor.get(a), accessor.get(b))
			if accessor.desc {
				cmp = -cmp
			}
			if cmp != 0 {
				return cmp
			}
		}
		return 0
	})

	return nil
}

func buildRowOrderAccessors(dimensions []string, metricNames []string, orderBy []OrderBy) ([]rowOrderAccessor, error) {
	if len(orderBy) == 0 {
		accessors := make([]rowOrderAccessor, 0, len(dimensions))
		for i := range dimensions {
			fieldName := dimensions[i]
			accessors = append(accessors, rowOrderAccessor{
				desc: false,
				get: func(row QueryResultRow) any {
					value, _ := row.Any(fieldName)
					return value
				},
			})
		}
		return accessors, nil
	}

	dimIndexByName := make(map[string]int, len(dimensions))
	for i, name := range dimensions {
		dimIndexByName[name] = i
	}
	metricSet := make(map[string]struct{}, len(metricNames))
	for _, name := range metricNames {
		metricSet[name] = struct{}{}
	}

	accessors := make([]rowOrderAccessor, 0, len(orderBy))
	for _, order := range orderBy {
		if order.Field == "" {
			return nil, fmt.Errorf("order field is required")
		}
		if order.Field == CountField {
			accessors = append(accessors, rowOrderAccessor{desc: order.Desc, get: func(row QueryResultRow) any {
				value, _ := row.Any(CountField)
				return value
			}})
			continue
		}
		if idx, ok := dimIndexByName[order.Field]; ok {
			fieldName := dimensions[idx]
			accessors = append(accessors, rowOrderAccessor{
				desc: order.Desc,
				get: func(row QueryResultRow) any {
					value, _ := row.Any(fieldName)
					return value
				},
			})
			continue
		}
		if _, ok := metricSet[order.Field]; ok {
			metricName := order.Field
			accessors = append(accessors, rowOrderAccessor{
				desc: order.Desc,
				get: func(row QueryResultRow) any {
					value, _ := row.Any(metricName)
					return value
				},
			})
			continue
		}

		return nil, fmt.Errorf("unsupported order field %q", order.Field)
	}

	return accessors, nil
}

func compareDimensionValue(a, b any) int {
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		return -1
	}
	if b == nil {
		return 1
	}

	switch av := a.(type) {
	case time.Time:
		bv, ok := b.(time.Time)
		if !ok {
			break
		}
		if av.Before(bv) {
			return -1
		}
		if av.After(bv) {
			return 1
		}
		return 0
	case int64:
		bv, ok := b.(int64)
		if !ok {
			break
		}
		if av < bv {
			return -1
		}
		if av > bv {
			return 1
		}
		return 0
	case float64:
		bv, ok := b.(float64)
		if !ok {
			break
		}
		if av < bv {
			return -1
		}
		if av > bv {
			return 1
		}
		return 0
	case string:
		bv, ok := b.(string)
		if !ok {
			break
		}
		if av < bv {
			return -1
		}
		if av > bv {
			return 1
		}
		return 0
	case bool:
		bv, ok := b.(bool)
		if !ok {
			break
		}
		if av == bv {
			return 0
		}
		if !av {
			return -1
		}
		return 1
	}

	as := fmt.Sprint(a)
	bs := fmt.Sprint(b)
	if as < bs {
		return -1
	}
	if as > bs {
		return 1
	}
	return 0
}
