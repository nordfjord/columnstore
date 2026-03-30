package tdigest

import (
	"math"
)

type node struct {
	mean  float64
	count float64
}

type Histogram struct {
	compression float64

	cap int

	mergedNodes   int
	unmergedNodes int

	mergedCount   float64
	unmergedCount float64

	nodes   []node
	scratch []node
}

func New(compression int) *Histogram {
	if compression <= 0 {
		compression = 1
	}
	cap := capFromCompression(compression)
	return &Histogram{
		compression: float64(compression),
		cap:         cap,
		nodes:       make([]node, cap),
		scratch:     make([]node, cap),
	}
}

func (h *Histogram) Add(val, count float64) {
	if h == nil {
		return
	}
	if h.shouldMerge() {
		h.merge()
		if h.shouldMerge() {
			h.nodes = append(h.nodes, node{})
			h.scratch = append(h.scratch, node{})
			h.cap = len(h.nodes)
		}
	}
	idx := h.nextNode()
	h.nodes[idx] = node{mean: val, count: count}
	h.unmergedNodes++
	h.unmergedCount += count
}

func (h *Histogram) Merge(other *Histogram) {
	if h == nil || other == nil {
		return
	}
	h.merge()
	other.merge()
	for i := 0; i < other.mergedNodes; i++ {
		n := other.nodes[i]
		h.Add(n.mean, n.count)
	}
}

func (h *Histogram) TotalCount() float64 {
	if h == nil {
		return 0
	}
	return h.mergedCount + h.unmergedCount
}

func (h *Histogram) ValueAt(q float64) float64 {
	if h == nil {
		return 0
	}
	h.merge()
	if q < 0 || q > 1 || h.mergedNodes == 0 {
		return math.NaN()
	}
	goal := q * h.mergedCount
	k := 0.0
	i := 0
	var n *node
	for ; i < h.mergedNodes; i++ {
		n = &h.nodes[i]
		if k+n.count > goal {
			break
		}
		k += n.count
	}

	deltaK := goal - k - (n.count / 2)
	if isVerySmall(deltaK) {
		return n.mean
	}

	right := deltaK > 0
	if (right && (i+1) == h.mergedNodes) || (!right && i == 0) {
		return n.mean
	}

	var nl, nr *node
	if right {
		nl = n
		nr = &h.nodes[i+1]
		k += nl.count / 2
	} else {
		nl = &h.nodes[i-1]
		nr = n
		k -= nl.count / 2
	}

	x := goal - k
	m := (nr.mean - nl.mean) / (nl.count/2 + nr.count/2)
	return m*x + nl.mean
}

func (h *Histogram) merge() {
	if h == nil || h.unmergedNodes == 0 {
		return
	}

	n := h.mergedNodes + h.unmergedNodes
	h.ensureScratch(n)
	stableMergeSortNodes(h.nodes[:n], h.scratch[:n])

	totalCount := h.mergedCount + h.unmergedCount
	denom := 2 * math.Pi * totalCount * math.Log(totalCount)
	normalizer := h.compression / denom

	cur := 0
	countSoFar := 0.0
	for i := 1; i < n; i++ {
		proposedCount := h.nodes[cur].count + h.nodes[i].count
		z := proposedCount * normalizer
		q0 := countSoFar / totalCount
		q2 := (countSoFar + proposedCount) / totalCount
		shouldAdd := (z <= (q0 * (1 - q0))) && (z <= (q2 * (1 - q2)))

		if shouldAdd {
			h.nodes[cur].count += h.nodes[i].count
			delta := h.nodes[i].mean - h.nodes[cur].mean
			weightedDelta := (delta * h.nodes[i].count) / h.nodes[cur].count
			h.nodes[cur].mean += weightedDelta
		} else {
			countSoFar += h.nodes[cur].count
			cur++
			h.nodes[cur] = h.nodes[i]
		}
	}

	for i := cur + 1; i < n; i++ {
		h.nodes[i] = node{}
	}

	h.mergedNodes = cur + 1
	h.mergedCount = totalCount
	h.unmergedNodes = 0
	h.unmergedCount = 0
}

func (h *Histogram) shouldMerge() bool {
	return (h.mergedNodes + h.unmergedNodes) == h.cap
}

func (h *Histogram) nextNode() int {
	return h.mergedNodes + h.unmergedNodes
}

func capFromCompression(compression int) int {
	return 6*compression + 10
}

func isVerySmall(val float64) bool {
	return !(val > 1e-9 || val < -1e-9)
}

func (h *Histogram) ensureScratch(n int) {
	if cap(h.scratch) < n {
		h.scratch = make([]node, n)
		return
	}
	h.scratch = h.scratch[:n]
}

func stableMergeSortNodes(values, scratch []node) {
	n := len(values)
	if n < 2 {
		return
	}

	src := values
	dst := scratch[:n]

	for width := 1; width < n; width <<= 1 {
		for i := 0; i < n; i += 2 * width {
			mid := min(i+width, n)
			end := min(i+2*width, n)
			mergeRuns(src, dst, i, mid, end)
		}
		src, dst = dst, src
	}

	if &src[0] != &values[0] {
		copy(values, src)
	}
}

func mergeRuns(src, dst []node, left, mid, right int) {
	i := left
	j := mid
	k := left

	for i < mid && j < right {
		if src[i].mean <= src[j].mean {
			dst[k] = src[i]
			i++
		} else {
			dst[k] = src[j]
			j++
		}
		k++
	}

	for i < mid {
		dst[k] = src[i]
		i++
		k++
	}

	for j < right {
		dst[k] = src[j]
		j++
		k++
	}
}
