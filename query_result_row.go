package columnstore

import (
	"encoding/json"
	"maps"
	"strconv"
	"time"
)

const CountField = "count"

func dimensionFieldName(index int) string {
	return "dimension_" + strconv.Itoa(index)
}

type QueryResultRow struct {
	int64s   map[string]int64
	float64s map[string]float64
	strings  map[string]string
	bools    map[string]bool
	times    map[string]time.Time
	nulls    map[string]struct{}
}

func NewQueryResultRow(values map[string]any) QueryResultRow {
	row := QueryResultRow{}
	for name, value := range values {
		switch v := value.(type) {
		case int64:
			row.SetInt64(name, v)
		case int:
			row.SetInt64(name, int64(v))
		case float64:
			row.SetFloat64(name, v)
		case string:
			row.SetString(name, v)
		case bool:
			row.SetBool(name, v)
		case time.Time:
			row.SetTime(name, v)
		default:
			panic("unsupported query dimension type")
		}
	}
	return row
}

func (r QueryResultRow) Int64(name string) int64 {
	return r.int64s[name]
}

func (r QueryResultRow) Float64(name string) float64 {
	f, ok := r.float64s[name]
	if !ok {
		i, ok := r.int64s[name]
		if ok {
			return float64(i)
		}
	}
	return f
}

func (r QueryResultRow) String(name string) string {
	return r.strings[name]
}

func (r QueryResultRow) Bool(name string) bool {
	return r.bools[name]
}

// Time returns the field as time.Time, coercing from stored time values,
// Unix-second numeric values, and RFC3339/date-only strings.
func (r QueryResultRow) Time(name string) time.Time {
	timeValue, ok := r.times[name]
	if ok {
		return timeValue
	}

	int64Value, ok := r.int64s[name]
	if ok {
		return time.Unix(int64Value, 0)
	}

	float64Value, ok := r.float64s[name]
	if ok {
		return time.Unix(int64(float64Value), 0)
	}

	stringValue, ok := r.strings[name]
	if ok {
		t, err := time.Parse(time.RFC3339, stringValue)
		if err == nil {
			return t
		}
		t, err = time.Parse(time.DateOnly, stringValue)
		if err == nil {
			return t
		}
	}

	return time.Time{}
}

func (r QueryResultRow) Null(name string) bool {
	_, ok := r.nulls[name]
	return ok
}

func (r QueryResultRow) Any(name string) (any, bool) {
	int64Value, ok := r.int64s[name]
	if ok {
		return int64Value, true
	}
	float64Value, ok := r.float64s[name]
	if ok {
		return float64Value, true
	}
	stringValue, ok := r.strings[name]
	if ok {
		return stringValue, true
	}
	boolValue, ok := r.bools[name]
	if ok {
		return boolValue, true
	}
	timeValue, ok := r.times[name]
	if ok {
		return timeValue, true
	}
	if r.Null(name) {
		return nil, true
	}
	return nil, false
}

func (r QueryResultRow) AsMap() map[string]any {
	result := make(map[string]any, len(r.int64s)+len(r.float64s)+len(r.strings)+len(r.bools)+len(r.times)+len(r.nulls))
	for name, value := range r.int64s {
		result[name] = value
	}
	for name, value := range r.float64s {
		result[name] = value
	}
	for name, value := range r.strings {
		result[name] = value
	}
	for name, value := range r.bools {
		result[name] = value
	}
	for name, value := range r.times {
		result[name] = value
	}
	for name := range r.nulls {
		result[name] = nil
	}
	return result
}

func (r QueryResultRow) clone() QueryResultRow {
	return QueryResultRow{
		int64s:   maps.Clone(r.int64s),
		float64s: maps.Clone(r.float64s),
		strings:  maps.Clone(r.strings),
		bools:    maps.Clone(r.bools),
		times:    maps.Clone(r.times),
		nulls:    maps.Clone(r.nulls),
	}
}

func (r *QueryResultRow) SetInt64(name string, value int64) {
	if r == nil || name == "" {
		return
	}
	if r.int64s == nil {
		r.int64s = make(map[string]int64, 4)
	}
	r.int64s[name] = value
}

func (r *QueryResultRow) SetFloat64(name string, value float64) {
	if r == nil || name == "" {
		return
	}
	if r.float64s == nil {
		r.float64s = make(map[string]float64, 4)
	}
	r.float64s[name] = value
}

func (r *QueryResultRow) SetString(name, value string) {
	if r == nil || name == "" {
		return
	}
	if r.strings == nil {
		r.strings = make(map[string]string, 4)
	}
	r.strings[name] = value
}

func (r *QueryResultRow) SetBool(name string, value bool) {
	if r == nil || name == "" {
		return
	}
	if r.bools == nil {
		r.bools = make(map[string]bool, 4)
	}
	r.bools[name] = value
}

func (r *QueryResultRow) SetTime(name string, value time.Time) {
	if r == nil || name == "" {
		return
	}
	if r.times == nil {
		r.times = make(map[string]time.Time, 4)
	}
	r.times[name] = value
}

func (r *QueryResultRow) SetNull(name string) {
	if r == nil || name == "" {
		return
	}
	if r.nulls == nil {
		r.nulls = make(map[string]struct{}, 4)
	}
	r.nulls[name] = struct{}{}
}

func (r *QueryResultRow) MarshalJSON() ([]byte, error) {
	return json.Marshal(r.AsMap())
}
