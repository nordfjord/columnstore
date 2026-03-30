package columnstore

import (
	"context"
	"fmt"
	"testing"
	"time"
)

var benchmarkQueryResultRows int

func BenchmarkStoreQueryDateStringSumCount(b *testing.B) {
	store := NewStore(b.TempDir())

	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	rowCount := 100_000
	stringCardinality := 100
	stringValues := make([]string, stringCardinality)
	for i := range stringValues {
		stringValues[i] = fmt.Sprintf("group-%03d", i)
	}

	rows := make([]RowInput, 0, rowCount)
	for i := range rowCount {
		ts := base.Add(time.Duration(i%31) * 24 * time.Hour).Add(time.Duration(i%1440) * time.Minute)
		rows = append(rows, RowInput{
			Timestamp: ts,
			Values: map[string]any{
				"some_string": stringValues[i%stringCardinality],
				"some_int":    int64((i % 1000) + 1),
			},
		})
	}

	if _, err := store.IngestBatch("bench_dataset", rows); err != nil {
		b.Fatalf("ingest benchmark rows: %v", err)
	}

	query := Query{
		Start:       base,
		End:         base.AddDate(0, 1, 0),
		Dataset:     "bench_dataset",
		Dimensions:  []string{"date", "some_string"},
		Granularity: "day",
		Timezone:    "UTC",
		Metrics: []Metric{
			Sum("some_int"),
			Count(),
		},
	}

	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		res, err := store.Query(context.Background(), query)
		if err != nil {
			b.Fatalf("query benchmark: %v", err)
		}
		benchmarkQueryResultRows = len(res.Rows)
	}
}
