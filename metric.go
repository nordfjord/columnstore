package columnstore

import (
	"fmt"
	"math/bits"
)

type metricSpec struct {
	name             string
	op               string
	column           string
	kind             metricNumericKind
	percentile       float64
	percentileSource int
}

type metricGroup struct {
	dimensions []dimAtom
	metrics    []metricState
}

// MetricAccumulator accumulates grouped metric values for one or more metrics.
type MetricAccumulator struct {
	specs  []metricSpec
	groups []metricGroup
	slots  []int

	stringIDs    map[string]uint64
	stringByID   map[uint64]string
	nextString   uint64
	fallbackIDs  map[string]uint64
	fallbackByID map[uint64]any
	nextFallback uint64
	scratchDims  []dimAtom
}

// NewMetricAccumulator creates a grouped accumulator for the provided metrics.
func NewMetricAccumulator(metrics []Metric) (*MetricAccumulator, error) {
	if len(metrics) == 0 {
		return nil, fmt.Errorf("at least one metric is required")
	}

	specs := make([]metricSpec, 0, len(metrics))
	builder := newMetricSpecBuilder()
	for _, metric := range metrics {
		specs = metric.appendSpecs(specs, builder)
	}

	return &MetricAccumulator{
		specs:        specs,
		groups:       make([]metricGroup, 0, initialGroupMapHint),
		slots:        make([]int, groupTableSizeHint(initialGroupMapHint)),
		stringIDs:    make(map[string]uint64, 128),
		stringByID:   make(map[uint64]string, 128),
		fallbackIDs:  make(map[string]uint64, 32),
		fallbackByID: make(map[uint64]any, 32),
		nextString:   1,
		nextFallback: 1,
	}, nil
}

func (a *MetricAccumulator) RegisterInternedStrings(values map[uint64]string) {
	if a == nil || len(values) == 0 {
		return
	}
	for id, value := range values {
		if id == 0 {
			continue
		}
		if _, exists := a.stringByID[id]; !exists {
			a.stringByID[id] = value
		}
	}
}

func (a *MetricAccumulator) getOrCreateGroup(dimensions []dimAtom) *metricGroup {
	if a == nil {
		return nil
	}
	if len(a.slots) == 0 {
		a.slots = make([]int, groupTableSizeHint(initialGroupMapHint))
	}

	dimAtoms, hash := a.hashDimensionAtoms(dimensions)
	mask := uint64(len(a.slots) - 1)
	for slot := int(hash & mask); ; slot = (slot + 1) & int(mask) {
		groupIndex := a.slots[slot]
		if groupIndex == 0 {
			if len(a.groups)+1 >= (len(a.slots)*7)/10 {
				a.growGroupTable()
				return a.getOrCreateGroup(dimAtoms)
			}
			idx := len(a.groups)
			a.groups = append(a.groups, metricGroup{
				dimensions: append([]dimAtom(nil), dimAtoms...),
				metrics:    make([]metricState, len(a.specs)),
			})
			a.slots[slot] = idx + 1
			return &a.groups[idx]
		}
		idx := groupIndex - 1
		if dimAtomsEqual(a.groups[idx].dimensions, dimAtoms) {
			return &a.groups[idx]
		}
	}
}

func (a *MetricAccumulator) internDimsAndHash(dimensions []any) ([]dimAtom, uint64) {
	if cap(a.scratchDims) < len(dimensions) {
		a.scratchDims = make([]dimAtom, len(dimensions))
	} else {
		a.scratchDims = a.scratchDims[:len(dimensions)]
	}

	for i := range dimensions {
		atom := internOneDimension(a, dimensions[i])
		a.scratchDims[i] = atom
	}

	return a.scratchDims, hashDimensionAtoms(a.scratchDims)
}

func (a *MetricAccumulator) hashDimensionAtoms(dimensions []dimAtom) ([]dimAtom, uint64) {
	return dimensions, hashDimensionAtoms(dimensions)
}

func hashDimensionAtoms(dimensions []dimAtom) uint64 {
	h := fnv64OffsetBasis
	for i := range dimensions {
		if i > 0 {
			h = fnv1aAddByte(h, 0x1f)
		}
		h = fnv1aAddByte(h, dimensions[i].kind)
		h = fnv1aAddU64(h, dimensions[i].bits)
	}
	return h
}

func (a *MetricAccumulator) growGroupTable() {
	newSlots := make([]int, groupTableSizeHint(len(a.groups)*2))
	for idx := range a.groups {
		hash := hashDimensionAtoms(a.groups[idx].dimensions)
		insertGroupSlot(newSlots, hash, idx)
	}
	a.slots = newSlots
}

func insertGroupSlot(slots []int, hash uint64, idx int) {
	mask := uint64(len(slots) - 1)
	for slot := int(hash & mask); ; slot = (slot + 1) & int(mask) {
		if slots[slot] == 0 {
			slots[slot] = idx + 1
			return
		}
	}
}

func groupTableSizeHint(n int) int {
	if n < 1 {
		n = 1
	}
	needed := max(n*2, 16)
	return 1 << bits.Len(uint(needed-1))
}
