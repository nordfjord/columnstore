package metricconv

import "strconv"

func CoerceToFloat64(v any) (float64, bool) {
	switch value := v.(type) {
	case int64:
		return float64(value), true
	case float64:
		return value, true
	case bool:
		if value {
			return 1, true
		}
		return 0, true
	case string:
		if value == "" {
			return 0, true
		}
		parsed, err := strconv.ParseFloat(value, 64)
		if err != nil {
			return 0, false
		}
		return parsed, true
	default:
		return 0, false
	}
}
