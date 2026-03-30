package columnstore

import (
	"testing"

	"github.com/nordfjord/columnstore/internal/colkind"
)

var benchmarkCountDistinctSink int64

func BenchmarkMetricAccumulatorCountDistinctStringIDs(b *testing.B) {
	benchmarks := []struct {
		name     string
		rows     int
		distinct int
	}{
		{name: "repeated_4096", rows: 4096, distinct: 16},
		{name: "unique_4096", rows: 4096, distinct: 4096},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			ids := make([]uint64, bm.rows)
			for i := range ids {
				ids[i] = uint64((i % bm.distinct) + 1)
			}

			columnKinds := []colkind.ColumnKind{colkind.KindString}
			intValues := []int64{0}
			floatValues := []float64{0}
			anyValues := make([]any, 1)
			present := []bool{true}
			specValueIndex := []int{0}

			b.ReportAllocs()
			b.ResetTimer()

			for b.Loop() {
				agg, err := NewMetricAccumulator([]Metric{CountDistinct("customer")})
				if err != nil {
					b.Fatal(err)
				}
				for _, id := range ids {
					anyValues[0] = id
					agg.AddResolvedTyped(nil, columnKinds, intValues, floatValues, anyValues, present, specValueIndex)
				}
				rows := agg.Rows()
				if len(rows) != 1 {
					b.Fatalf("rows=%d, want 1", len(rows))
				}
				value := rows[0].Int64("count:customer")
				benchmarkCountDistinctSink = value
			}
		})
	}
}
