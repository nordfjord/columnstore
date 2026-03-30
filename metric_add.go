package columnstore

import (
	"github.com/nordfjord/columnstore/internal/colkind"
	"github.com/nordfjord/columnstore/internal/metricconv"
)

// Add accumulates one matching row for a dimension tuple.
// values contains only columns present for the current row.
func (a *MetricAccumulator) Add(dimensions []any, values map[string]any) {
	if a == nil {
		return
	}

	dimAtoms, _ := a.internDimsAndHash(dimensions)
	group := a.getOrCreateGroup(dimAtoms)
	a.addToGroup(group, values)
}

func (a *MetricAccumulator) addToGroup(group *metricGroup, values map[string]any) {
	if group == nil {
		return
	}

	for i := range a.specs {
		spec := &a.specs[i]
		state := &group.metrics[i]

		switch spec.op {
		case MetricOpCount:
			state.count++
		case MetricOpSum:
			raw, ok := values[spec.column]
			if !ok {
				continue
			}
			sumRawIntoState(state, raw)
			updateSpecKind(spec, raw)
		case MetricOpAvg:
			raw, ok := values[spec.column]
			if !ok {
				continue
			}
			n, ok := metricconv.CoerceToFloat64(raw)
			if !ok {
				continue
			}
			state.avgSum += n
			state.count++
		case MetricOpMax:
			raw, ok := values[spec.column]
			if !ok {
				continue
			}
			setMaxIntoState(state, raw)
			updateSpecKind(spec, raw)
		case MetricOpMin:
			raw, ok := values[spec.column]
			if !ok {
				continue
			}
			setMinIntoState(state, raw)
			updateSpecKind(spec, raw)
		case MetricOpCountDistinct:
			raw, ok := values[spec.column]
			if !ok {
				continue
			}
			addDistinctHash(state, hashDistinctAny(raw))
		default:
			if !isPercentileOp(spec.op) {
				continue
			}
			if spec.percentileSource != i {
				continue
			}
			raw, ok := values[spec.column]
			if !ok {
				continue
			}
			n, ok := metricconv.CoerceToFloat64(raw)
			if !ok {
				continue
			}
			if state.percentileTDigest == nil {
				state.percentileTDigest = newPercentileTDigest()
				if state.percentileTDigest == nil {
					continue
				}
			}
			state.percentileTDigest.Add(n, 1)
		}
	}
}

// AddResolved accumulates one matching row using pre-resolved metric values.
// values/present are aligned to the metric scanner column order and specValueIndex
// maps each spec to an index in values (or -1 when no value source is required).
func (a *MetricAccumulator) AddResolved(dimensions []any, values []any, present []bool, specValueIndex []int) {
	if a == nil {
		return
	}
	if len(specValueIndex) != len(a.specs) {
		return
	}

	dimAtoms, _ := a.internDimsAndHash(dimensions)
	group := a.getOrCreateGroup(dimAtoms)

	for i := range a.specs {
		spec := &a.specs[i]
		state := &group.metrics[i]

		valueIndex := specValueIndex[i]
		var (
			raw any
			ok  bool
		)
		if valueIndex >= 0 {
			if valueIndex >= len(values) || valueIndex >= len(present) || !present[valueIndex] {
				ok = false
			} else {
				raw = values[valueIndex]
				ok = true
			}
		}

		switch spec.op {
		case MetricOpCount:
			state.count++
		case MetricOpSum:
			if !ok {
				continue
			}
			sumRawIntoState(state, raw)
			updateSpecKind(spec, raw)
		case MetricOpAvg:
			if !ok {
				continue
			}
			n, ok := metricconv.CoerceToFloat64(raw)
			if !ok {
				continue
			}
			state.avgSum += n
			state.count++
		case MetricOpMax:
			if !ok {
				continue
			}
			setMaxIntoState(state, raw)
			updateSpecKind(spec, raw)
		case MetricOpMin:
			if !ok {
				continue
			}
			setMinIntoState(state, raw)
			updateSpecKind(spec, raw)
		case MetricOpCountDistinct:
			if !ok {
				continue
			}
			addDistinctHash(state, hashDistinctAny(raw))
		default:
			if !isPercentileOp(spec.op) || !ok {
				continue
			}
			if spec.percentileSource != i {
				continue
			}
			n, ok := metricconv.CoerceToFloat64(raw)
			if !ok {
				continue
			}
			if state.percentileTDigest == nil {
				state.percentileTDigest = newPercentileTDigest()
				if state.percentileTDigest == nil {
					continue
				}
			}
			state.percentileTDigest.Add(n, 1)
		}
	}
}

// AddResolvedTyped accumulates one matching row using typed metric vectors.
// intValues/floatValues/anyValues/present are aligned by metric column index.
func (a *MetricAccumulator) AddResolvedTyped(
	dimensions []dimAtom,
	columnKinds []colkind.ColumnKind,
	intValues []int64,
	floatValues []float64,
	anyValues []any,
	present []bool,
	specValueIndex []int,
) {
	if a == nil {
		return
	}
	if len(specValueIndex) != len(a.specs) {
		return
	}

	group := a.getOrCreateGroup(dimensions)

	for i := range a.specs {
		spec := &a.specs[i]
		state := &group.metrics[i]

		valueIndex := specValueIndex[i]
		if valueIndex < 0 || valueIndex >= len(present) || !present[valueIndex] {
			if spec.op == MetricOpCount {
				state.count++
			}
			continue
		}

		var kind colkind.ColumnKind
		if valueIndex < len(columnKinds) {
			kind = columnKinds[valueIndex]
		}

		switch spec.op {
		case MetricOpCount:
			state.count++
		case MetricOpSum:
			switch kind {
			case colkind.KindInt64:
				v := intValues[valueIndex]
				if state.numericKind == metricKindFloat {
					state.sumFloat += float64(v)
				} else {
					state.numericKind = metricKindInt
					state.sumInt += v
				}
				if spec.kind == metricKindUnknown {
					spec.kind = metricKindInt
				}
			case colkind.KindFloat64:
				v := floatValues[valueIndex]
				if state.numericKind == metricKindInt {
					state.sumFloat = float64(state.sumInt)
					state.sumInt = 0
				}
				state.numericKind = metricKindFloat
				state.sumFloat += v
				spec.kind = metricKindFloat
			default:
				raw := anyValues[valueIndex]
				sumRawIntoState(state, raw)
				updateSpecKind(spec, raw)
			}
		case MetricOpAvg:
			switch kind {
			case colkind.KindInt64:
				state.avgSum += float64(intValues[valueIndex])
				state.count++
			case colkind.KindFloat64:
				state.avgSum += floatValues[valueIndex]
				state.count++
			default:
				raw := anyValues[valueIndex]
				n, ok := metricconv.CoerceToFloat64(raw)
				if !ok {
					continue
				}
				state.avgSum += n
				state.count++
			}
		case MetricOpMax:
			switch kind {
			case colkind.KindInt64:
				v := intValues[valueIndex]
				if state.numericKind == metricKindFloat {
					setFloatExtreme(state, float64(v), true)
				} else {
					if state.numericKind == metricKindUnknown {
						state.numericKind = metricKindInt
					}
					setIntExtreme(state, v, true)
				}
				if spec.kind == metricKindUnknown {
					spec.kind = metricKindInt
				}
			case colkind.KindFloat64:
				v := floatValues[valueIndex]
				if state.numericKind == metricKindInt {
					state.minFloat = float64(state.minInt)
					state.maxFloat = float64(state.maxInt)
				}
				state.numericKind = metricKindFloat
				setFloatExtreme(state, v, true)
				spec.kind = metricKindFloat
			default:
				raw := anyValues[valueIndex]
				setMaxIntoState(state, raw)
				updateSpecKind(spec, raw)
			}
		case MetricOpMin:
			switch kind {
			case colkind.KindInt64:
				v := intValues[valueIndex]
				if state.numericKind == metricKindFloat {
					setFloatExtreme(state, float64(v), false)
				} else {
					if state.numericKind == metricKindUnknown {
						state.numericKind = metricKindInt
					}
					setIntExtreme(state, v, false)
				}
				if spec.kind == metricKindUnknown {
					spec.kind = metricKindInt
				}
			case colkind.KindFloat64:
				v := floatValues[valueIndex]
				if state.numericKind == metricKindInt {
					state.minFloat = float64(state.minInt)
					state.maxFloat = float64(state.maxInt)
				}
				state.numericKind = metricKindFloat
				setFloatExtreme(state, v, false)
				spec.kind = metricKindFloat
			default:
				raw := anyValues[valueIndex]
				setMinIntoState(state, raw)
				updateSpecKind(spec, raw)
			}
		case MetricOpCountDistinct:
			switch kind {
			case colkind.KindInt64:
				addDistinctHash(state, hashDistinctInt64(intValues[valueIndex]))
			case colkind.KindFloat64:
				addDistinctHash(state, hashDistinctFloat64(floatValues[valueIndex]))
			case colkind.KindBool:
				raw, _ := anyValues[valueIndex].(bool)
				addDistinctHash(state, hashDistinctBool(raw))
			case colkind.KindString:
				addDistinctHash(state, hashDistinctAny(anyValues[valueIndex]))
			default:
				raw := anyValues[valueIndex]
				addDistinctHash(state, hashDistinctAny(raw))
			}
		default:
			if !isPercentileOp(spec.op) {
				continue
			}
			if spec.percentileSource != i {
				continue
			}
			if state.percentileTDigest == nil {
				state.percentileTDigest = newPercentileTDigest()
				if state.percentileTDigest == nil {
					continue
				}
			}
			switch kind {
			case colkind.KindInt64:
				state.percentileTDigest.Add(float64(intValues[valueIndex]), 1)
			case colkind.KindFloat64:
				state.percentileTDigest.Add(floatValues[valueIndex], 1)
			default:
				raw := anyValues[valueIndex]
				n, ok := metricconv.CoerceToFloat64(raw)
				if !ok {
					continue
				}
				state.percentileTDigest.Add(n, 1)
			}
		}
	}
}
