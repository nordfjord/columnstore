package columnstore

import "fmt"

// Merge merges grouped metric states from another accumulator.
func (a *MetricAccumulator) Merge(other *MetricAccumulator) error {
	if a == nil || other == nil {
		return nil
	}
	if len(a.specs) != len(other.specs) {
		return fmt.Errorf("metric shape mismatch")
	}
	for i := range a.specs {
		if !sameMetricShape(a.specs[i], other.specs[i]) {
			return fmt.Errorf("metric shape mismatch")
		}
		if a.specs[i].kind == metricKindUnknown {
			a.specs[i].kind = other.specs[i].kind
		} else if other.specs[i].kind == metricKindFloat {
			a.specs[i].kind = metricKindFloat
		}
	}

	for id, value := range other.stringByID {
		if _, exists := a.stringByID[id]; !exists {
			a.stringByID[id] = value
		}
	}
	for id, value := range other.fallbackByID {
		if _, exists := a.fallbackByID[id]; !exists {
			a.fallbackByID[id] = value
		}
	}

	for i := range other.groups {
		otherGroup := &other.groups[i]
		group := a.getOrCreateGroup(a.remapMergedDimensions(otherGroup.dimensions, other.stringByID))

		for i := range a.specs {
			spec := a.specs[i]
			state := &group.metrics[i]
			otherState := &otherGroup.metrics[i]

			state.count += otherState.count
			state.avgSum += otherState.avgSum
			mergeNumericStateForOp(state, otherState, spec.op)

			if otherState.distinct != nil || len(otherState.distinctExact) > 0 {
				if err := mergeDistinctState(state, otherState); err != nil {
					return err
				}
			}
			if isPercentileOp(spec.op) && spec.percentileSource != i {
				continue
			}
			if otherState.percentileTDigest != nil {
				if state.percentileTDigest == nil {
					state.percentileTDigest = clonePercentileTDigest(otherState.percentileTDigest)
				} else {
					mergePercentileTDigest(state.percentileTDigest, otherState.percentileTDigest)
				}
			}
		}
	}

	return nil
}

func sameMetricShape(a, b metricSpec) bool {
	return a.name == b.name && a.op == b.op && a.column == b.column
}

func (a *MetricAccumulator) remapMergedDimensions(dimensions []dimAtom, sourceStrings map[uint64]string) []dimAtom {
	if len(dimensions) == 0 {
		return dimensions
	}
	out := make([]dimAtom, len(dimensions))
	for i := range dimensions {
		switch dimensions[i].kind {
		case dimKindString:
			value, ok := sourceStrings[dimensions[i].bits]
			if !ok {
				out[i] = dimAtom{kind: dimKindString}
				continue
			}
			id, exists := a.stringIDs[value]
			if !exists {
				id = a.nextString
				a.nextString++
				a.stringIDs[value] = id
				a.stringByID[id] = value
			}
			out[i] = dimAtom{kind: dimKindString, bits: id}
		default:
			out[i] = dimensions[i]
		}
	}
	return out
}
