package columnstore

import (
	"math"

	hashutil "github.com/nordfjord/columnstore/internal/hashutil"
	hll "github.com/nordfjord/columnstore/internal/hll"
	"github.com/nordfjord/columnstore/internal/metricconv"
	tdigest "github.com/nordfjord/columnstore/internal/tdigest"
)

type metricNumericKind uint8

const (
	metricKindUnknown metricNumericKind = iota
	metricKindInt
	metricKindFloat
)

type metricState struct {
	count             int64
	avgSum            float64
	sumInt            int64
	sumFloat          float64
	minInt            int64
	maxInt            int64
	minFloat          float64
	maxFloat          float64
	numericKind       metricNumericKind
	hasValue          bool
	distinctExact     map[uint64]struct{}
	distinct          *hll.Sketch
	percentileTDigest *tdigest.Histogram
}

const distinctExactThreshold = 256

func newDistinctSketch() *hll.Sketch {
	return hll.New()
}

func addDistinctHash(state *metricState, hash uint64) {
	if state == nil {
		return
	}
	if state.distinct != nil {
		state.distinct.AddHash(hash)
		return
	}
	if state.distinctExact == nil {
		state.distinctExact = make(map[uint64]struct{}, distinctExactThreshold)
	}
	state.distinctExact[hash] = struct{}{}
	if len(state.distinctExact) <= distinctExactThreshold {
		return
	}

	state.distinct = newDistinctSketch()
	for valueHash := range state.distinctExact {
		state.distinct.AddHash(valueHash)
	}
	state.distinctExact = nil
}

func mergeDistinctState(state, other *metricState) error {
	if state == nil || other == nil {
		return nil
	}
	if other.distinct != nil {
		if state.distinct == nil {
			state.distinct = newDistinctSketch()
			for valueHash := range state.distinctExact {
				state.distinct.AddHash(valueHash)
			}
			state.distinctExact = nil
		}
		return state.distinct.Merge(other.distinct)
	}
	for valueHash := range other.distinctExact {
		addDistinctHash(state, valueHash)
	}
	return nil
}

func distinctEstimate(state *metricState) int64 {
	if state == nil {
		return 0
	}
	if state.distinct != nil {
		return int64(math.Round(state.distinct.Estimate()))
	}
	return int64(len(state.distinctExact))
}

func hashDistinctInt64(v int64) uint64 {
	return hashutil.XXH64TaggedUint64('i', uint64(v))
}

func hashDistinctFloat64(v float64) uint64 {
	return hashutil.XXH64TaggedUint64('f', math.Float64bits(v))
}

func hashDistinctString(v string) uint64 {
	return hashutil.XXH64TaggedString('s', v)
}

func hashDistinctBool(v bool) uint64 {
	return hashutil.XXH64TaggedBool('b', v)
}

func hashDistinctAny(v any) uint64 {
	switch x := v.(type) {
	case int64:
		return hashDistinctInt64(x)
	case float64:
		return hashDistinctFloat64(x)
	case string:
		return hashDistinctString(x)
	case bool:
		return hashDistinctBool(x)
	case uint64:
		return x
	default:
		key := BuildGroupKey([]any{v})
		return hashDistinctString(key)
	}
}

func updateSpecKind(spec *metricSpec, raw any) {
	if spec == nil {
		return
	}
	switch raw.(type) {
	case int64:
		if spec.kind == metricKindUnknown {
			spec.kind = metricKindInt
		}
	case float64:
		spec.kind = metricKindFloat
	default:
		if _, ok := metricconv.CoerceToFloat64(raw); ok {
			spec.kind = metricKindFloat
		}
	}
}

func sumRawIntoState(state *metricState, raw any) {
	if state == nil {
		return
	}
	switch v := raw.(type) {
	case int64:
		if state.numericKind == metricKindFloat {
			state.sumFloat += float64(v)
			return
		}
		state.numericKind = metricKindInt
		state.sumInt += v
	case float64:
		if state.numericKind == metricKindInt {
			state.sumFloat = float64(state.sumInt)
			state.sumInt = 0
		}
		state.numericKind = metricKindFloat
		state.sumFloat += v
	default:
		n, ok := metricconv.CoerceToFloat64(raw)
		if !ok {
			return
		}
		if state.numericKind == metricKindInt {
			state.sumFloat = float64(state.sumInt)
			state.sumInt = 0
		}
		state.numericKind = metricKindFloat
		state.sumFloat += n
	}
}

func setMinIntoState(state *metricState, raw any) {
	setExtremeIntoState(state, raw, false)
}

func setMaxIntoState(state *metricState, raw any) {
	setExtremeIntoState(state, raw, true)
}

func setExtremeIntoState(state *metricState, raw any, isMax bool) {
	if state == nil {
		return
	}

	switch v := raw.(type) {
	case int64:
		if state.numericKind == metricKindFloat {
			setFloatExtreme(state, float64(v), isMax)
			return
		}
		if state.numericKind == metricKindUnknown {
			state.numericKind = metricKindInt
		}
		setIntExtreme(state, v, isMax)
	case float64:
		if state.numericKind == metricKindInt {
			state.minFloat = float64(state.minInt)
			state.maxFloat = float64(state.maxInt)
		}
		state.numericKind = metricKindFloat
		setFloatExtreme(state, v, isMax)
	default:
		n, ok := metricconv.CoerceToFloat64(raw)
		if !ok {
			return
		}
		if state.numericKind == metricKindInt {
			state.minFloat = float64(state.minInt)
			state.maxFloat = float64(state.maxInt)
		}
		state.numericKind = metricKindFloat
		setFloatExtreme(state, n, isMax)
	}
}

func setIntExtreme(state *metricState, value int64, isMax bool) {
	if !state.hasValue {
		state.minInt = value
		state.maxInt = value
		state.hasValue = true
		return
	}
	if isMax {
		if value > state.maxInt {
			state.maxInt = value
		}
		return
	}
	if value < state.minInt {
		state.minInt = value
	}
}

func setFloatExtreme(state *metricState, value float64, isMax bool) {
	if !state.hasValue {
		state.minFloat = value
		state.maxFloat = value
		state.hasValue = true
		return
	}
	if isMax {
		if value > state.maxFloat {
			state.maxFloat = value
		}
		return
	}
	if value < state.minFloat {
		state.minFloat = value
	}
}

func mergeNumericStateForOp(state, other *metricState, op string) {
	if state == nil || other == nil {
		return
	}

	switch op {
	case MetricOpSum:
		mergeSumState(state, other)
	case MetricOpMin:
		if !other.hasValue {
			return
		}
		if other.numericKind == metricKindInt {
			setMinIntoState(state, other.minInt)
			return
		}
		setMinIntoState(state, other.minFloat)
	case MetricOpMax:
		if !other.hasValue {
			return
		}
		if other.numericKind == metricKindInt {
			setMaxIntoState(state, other.maxInt)
			return
		}
		setMaxIntoState(state, other.maxFloat)
	}
}

func mergeSumState(state, other *metricState) {
	if state == nil || other == nil || other.numericKind == metricKindUnknown {
		return
	}
	if state.numericKind == metricKindUnknown {
		state.numericKind = other.numericKind
		state.sumInt = other.sumInt
		state.sumFloat = other.sumFloat
		return
	}
	if state.numericKind == metricKindInt && other.numericKind == metricKindInt {
		state.sumInt += other.sumInt
		return
	}
	if state.numericKind == metricKindInt {
		state.sumFloat = float64(state.sumInt)
		state.sumInt = 0
		state.numericKind = metricKindFloat
	}
	if other.numericKind == metricKindInt {
		state.sumFloat += float64(other.sumInt)
		return
	}
	state.sumFloat += other.sumFloat
}
