package columnstore

import (
	"math"
	"slices"
	"time"

	"github.com/nordfjord/columnstore/internal/timecalc"
)

// Rows returns grouped rows sorted by dimensions for deterministic output.
func (a *MetricAccumulator) Rows() []QueryResultRow {
	return a.RowsNamedInLocation(nil, time.UTC)
}

// RowsInLocation returns grouped rows with bucketed time dimensions materialized in loc.
func (a *MetricAccumulator) RowsInLocation(loc *time.Location) []QueryResultRow {
	return a.RowsNamedInLocation(nil, loc)
}

// RowsNamedInLocation returns grouped rows with named dimensions and bucketed times materialized in loc.
func (a *MetricAccumulator) RowsNamedInLocation(dimensionNames []string, loc *time.Location) []QueryResultRow {
	if a == nil || len(a.groups) == 0 {
		return nil
	}

	rows := make([]QueryResultRow, 0, len(a.groups))
	for i := range a.groups {
		group := &a.groups[i]
		dimensions := a.materializeDimensions(group.dimensions, loc)
		row := QueryResultRow{}
		for idx := range dimensions {
			name := dimensionFieldName(idx)
			if idx < len(dimensionNames) && dimensionNames[idx] != "" {
				name = dimensionNames[idx]
			}
			switch value := dimensions[idx].(type) {
			case nil:
				row.SetNull(name)
			case int64:
				row.SetInt64(name, value)
			case float64:
				row.SetFloat64(name, value)
			case string:
				row.SetString(name, value)
			case bool:
				row.SetBool(name, value)
			case time.Time:
				row.SetTime(name, value)
			default:
				panic("unsupported query dimension type")
			}
		}

		for i := range a.specs {
			spec := a.specs[i]
			state := &group.metrics[i]
			switch spec.op {
			case MetricOpCount:
				row.SetInt64(spec.name, state.count)
			case MetricOpSum:
				if state.numericKind == metricKindInt {
					row.SetInt64(spec.name, state.sumInt)
				} else {
					row.SetFloat64(spec.name, state.sumFloat)
				}
			case MetricOpAvg:
				var avg float64
				if state.count > 0 {
					avg = state.avgSum / float64(state.count)
				}
				row.SetFloat64(spec.name, avg)
			case MetricOpMax:
				if !state.hasValue {
					if spec.kind == metricKindInt {
						row.SetInt64(spec.name, 0)
					} else {
						row.SetFloat64(spec.name, 0)
					}
					continue
				}
				if state.numericKind == metricKindInt {
					row.SetInt64(spec.name, state.maxInt)
				} else {
					row.SetFloat64(spec.name, state.maxFloat)
				}
			case MetricOpMin:
				if !state.hasValue {
					if spec.kind == metricKindInt {
						row.SetInt64(spec.name, 0)
					} else {
						row.SetFloat64(spec.name, 0)
					}
					continue
				}
				if state.numericKind == metricKindInt {
					row.SetInt64(spec.name, state.minInt)
				} else {
					row.SetFloat64(spec.name, state.minFloat)
				}
			case MetricOpCountDistinct:
				row.SetInt64(spec.name, distinctEstimate(state))
			default:
				if !isPercentileOp(spec.op) {
					continue
				}
				source := spec.percentileSource
				if source < 0 || source >= len(group.metrics) {
					source = i
				}
				row.SetFloat64(spec.name, percentileValue(group.metrics[source].percentileTDigest, spec.percentile))
			}
		}

		rows = append(rows, row)
	}

	slices.SortFunc(rows, func(left, right QueryResultRow) int {
		for _, name := range dimensionNames {
			leftValue, ok := left.Any(name)
			if !ok {
				leftValue = ""
			}
			rightValue, ok := right.Any(name)
			if !ok {
				rightValue = ""
			}
			if leftValue != rightValue {
				return compareDimensionValue(leftValue, rightValue)
			}
		}
		return 0
	})

	return rows
}

func (a *MetricAccumulator) materializeDimensions(dimensions []dimAtom, loc *time.Location) []any {
	out := make([]any, len(dimensions))
	for i := range dimensions {
		switch dimensions[i].kind {
		case dimKindNil:
			out[i] = nil
		case dimKindInt64:
			out[i] = int64(dimensions[i].bits)
		case dimKindFloat64:
			out[i] = math.Float64frombits(dimensions[i].bits)
		case dimKindString:
			if value, ok := a.stringByID[dimensions[i].bits]; ok {
				out[i] = value
			} else {
				out[i] = ""
			}
		case dimKindBool:
			out[i] = dimensions[i].bits != 0
		case dimKindTime:
			out[i] = timecalc.MaterializeTimeBucket(timecalc.TimeBucket(dimensions[i].bits), loc)
		case dimKindFallback:
			out[i] = a.fallbackByID[dimensions[i].bits]
		default:
			out[i] = nil
		}
	}
	return out
}
