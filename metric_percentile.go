package columnstore

import (
	tdigest "github.com/nordfjord/columnstore/internal/tdigest"
)

const defaultTDigestCompression = 100

var percentileOpToQuantile = map[string]float64{
	MetricOpP50:  0.50,
	MetricOpP75:  0.75,
	MetricOpP90:  0.90,
	MetricOpP95:  0.95,
	MetricOpP99:  0.99,
	MetricOpP999: 0.999,
}

func newPercentileTDigest() *tdigest.Histogram {
	return tdigest.New(defaultTDigestCompression)
}

func percentileValue(digest *tdigest.Histogram, quantile float64) float64 {
	if digest == nil || digest.TotalCount() == 0 {
		return 0
	}
	return digest.ValueAt(quantile)
}

func clonePercentileTDigest(src *tdigest.Histogram) *tdigest.Histogram {
	if src == nil {
		return nil
	}
	dst := tdigest.New(defaultTDigestCompression)
	dst.Merge(src)
	return dst
}

func mergePercentileTDigest(dst, src *tdigest.Histogram) {
	if dst == nil || src == nil {
		return
	}
	dst.Merge(src)
}

func isPercentileOp(op string) bool {
	_, ok := percentileOpToQuantile[op]
	return ok
}
