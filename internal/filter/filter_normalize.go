package filter

import (
	"math"
	"reflect"

	"github.com/nordfjord/columnstore/internal/colkind"
)

func NormalizeFilterValueForKind(value any, kind colkind.ColumnKind) (any, bool) {
	if value == nil {
		return nil, true
	}

	rv := reflect.ValueOf(value)

	switch kind {
	case colkind.KindInt64:
		switch rv.Kind() {
		case reflect.Int64:
			return rv.Int(), true
		case reflect.Int:
			return int64(rv.Int()), true
		case reflect.Float64, reflect.Float32:
			fv := rv.Convert(reflect.TypeFor[float64]()).Float()
			if math.Trunc(fv) != fv || fv < math.MinInt64 || fv > math.MaxInt64 {
				return nil, false
			}
			return int64(fv), true
		default:
			return nil, false
		}
	case colkind.KindFloat64:
		switch rv.Kind() {
		case reflect.Float64, reflect.Float32:
			return rv.Convert(reflect.TypeFor[float64]()).Float(), true
		case reflect.Int64, reflect.Int:
			return float64(rv.Int()), true
		default:
			return nil, false
		}
	case colkind.KindBool:
		if rv.Kind() != reflect.Bool {
			return nil, false
		}
		return rv.Bool(), true
	case colkind.KindString:
		if rv.Kind() != reflect.String {
			return nil, false
		}
		return rv.String(), true
	default:
		return nil, false
	}
}

func NormalizeFilterValuesForKind(values []any, kind colkind.ColumnKind) []any {
	normalized := make([]any, 0, len(values))
	for _, value := range values {
		coerced, ok := NormalizeFilterValueForKind(value, kind)
		if !ok {
			continue
		}
		normalized = append(normalized, coerced)
	}
	return normalized
}
