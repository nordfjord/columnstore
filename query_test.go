package columnstore

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/nordfjord/columnstore/internal/timecalc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func requireRowInt64(t *testing.T, row QueryResultRow, field string) int64 {
	t.Helper()
	require.False(t, row.Null(field), "missing int64 field %q", field)
	return row.Int64(field)
}

func requireRowFloat64(t *testing.T, row QueryResultRow, field string) float64 {
	t.Helper()
	require.False(t, row.Null(field), "missing float64 field %q", field)
	return row.Float64(field)
}

func requireRowString(t *testing.T, row QueryResultRow, field string) string {
	t.Helper()
	require.False(t, row.Null(field), "missing string field %q", field)
	return row.String(field)
}

func requireRowTime(t *testing.T, row QueryResultRow, field string) time.Time {
	t.Helper()
	require.False(t, row.Null(field), "missing time field %q", field)
	return row.Time(field)
}

func TestStoreQueryCountByDateAndEntityTypeAcrossPartitions(t *testing.T) {
	store := NewStore(t.TempDir())

	mustIngest := func(ts int64, row map[string]any) {
		_, _, err := store.IngestRow("entities", time.Unix(ts, 0).UTC(), row)
		require.NoError(t, err)
	}

	mustIngest(1704067200, map[string]any{"entity_type": "SalesTransaction", "value": int64(10)}) // 2024-01-01
	mustIngest(1704070800, map[string]any{"entity_type": "SalesTransaction", "value": int64(20)}) // 2024-01-01
	mustIngest(1704153600, map[string]any{"entity_type": "Invoice", "value": int64(30)})          // 2024-01-02
	mustIngest(1706745600, map[string]any{"entity_type": "SalesTransaction", "value": int64(40)}) // 2024-02-01

	res, err := store.Query(t.Context(), Query{
		Start:       time.Unix(1704067200, 0).UTC(),
		End:         time.Unix(1709251200, 0).UTC(),
		Dataset:     "entities",
		Dimensions:  []string{"date", "entity_type"},
		Granularity: "day",
		Timezone:    "UTC",
		Metric:      Count(),
		Filter:      Eq("entity_type", "SalesTransaction"),
	})
	require.NoError(t, err)

	got := make(map[string]int64)
	for _, row := range res.Rows {
		date := requireRowTime(t, row, "date")
		typeVal := requireRowString(t, row, "entity_type")
		got[date.Format("2006-01-02")+"|"+typeVal] = requireRowInt64(t, row, CountField)
	}

	assert.Equal(t, map[string]int64{
		"2024-01-01|SalesTransaction": 2,
		"2024-02-01|SalesTransaction": 1,
	}, got)
}

func TestStoreQueryAllowsMissingDimensionAsNil(t *testing.T) {
	store := NewStore(t.TempDir())
	_, _, err := store.IngestRow("entities", time.Unix(1704067200, 0).UTC(), map[string]any{"entity_type": "SalesTransaction"})
	require.NoError(t, err)

	res, err := store.Query(t.Context(), Query{
		Start:       time.Unix(1704067200, 0).UTC(),
		End:         time.Unix(1704153600, 0).UTC(),
		Dataset:     "entities",
		Dimensions:  []string{"date", "non_existent_dimension"},
		Granularity: "day",
		Timezone:    "UTC",
		Metric:      Count(),
		Filter:      Eq("entity_type", "SalesTransaction"),
	})
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	require.Equal(t, time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), requireRowTime(t, res.Rows[0], "date"))
	require.True(t, res.Rows[0].Null("non_existent_dimension"))
	require.Equal(t, int64(1), requireRowInt64(t, res.Rows[0], CountField))
}

func TestStoreQueryGroupsByArbitraryStringDimension(t *testing.T) {
	store := NewStore(t.TempDir())
	mustIngest := func(ts int64, row map[string]any) {
		_, _, err := store.IngestRow("test_dataset", time.Unix(ts, 0).UTC(), row)
		require.NoError(t, err)
	}

	mustIngest(1704067200, map[string]any{"entity_type": "SalesTransaction", "product_name": "Tea"})
	mustIngest(1704067300, map[string]any{"entity_type": "SalesTransaction", "product_name": "Tea"})
	mustIngest(1704067400, map[string]any{"entity_type": "SalesTransaction", "product_name": "Coffee"})
	mustIngest(1704067500, map[string]any{"entity_type": "Invoice", "product_name": "Tea"})

	res, err := store.Query(t.Context(), Query{
		Start:       time.Unix(1704067200, 0).UTC(),
		End:         time.Unix(1704153600, 0).UTC(),
		Dataset:     "test_dataset",
		Dimensions:  []string{"date", "product_name"},
		Granularity: "day",
		Timezone:    "UTC",
		Metric:      Count(),
		Filter:      Eq("entity_type", "SalesTransaction"),
	})
	require.NoError(t, err)

	got := make(map[string]int64)
	for _, row := range res.Rows {
		date := requireRowTime(t, row, "date")
		name := requireRowString(t, row, "product_name")
		got[date.Format("2006-01-02")+"|"+name] = requireRowInt64(t, row, CountField)
	}

	assert.Equal(t, map[string]int64{
		"2024-01-01|Tea":    2,
		"2024-01-01|Coffee": 1,
	}, got)
}

func TestStoreQueryGroupsByDateAndIntDimension(t *testing.T) {
	store := NewStore(t.TempDir())
	mustIngest := func(ts int64, row map[string]any) {
		_, _, err := store.IngestRow("entities", time.Unix(ts, 0).UTC(), row)
		require.NoError(t, err)
	}

	mustIngest(1704067200, map[string]any{"entity_type": "SalesTransaction", "family_size": int64(2)})
	mustIngest(1704067300, map[string]any{"entity_type": "SalesTransaction", "family_size": int64(2)})
	mustIngest(1704067400, map[string]any{"entity_type": "SalesTransaction", "family_size": int64(4)})

	res, err := store.Query(t.Context(), Query{
		Start:       time.Unix(1704067200, 0).UTC(),
		End:         time.Unix(1704153600, 0).UTC(),
		Dataset:     "entities",
		Dimensions:  []string{"date", "family_size"},
		Granularity: "day",
		Timezone:    "UTC",
		Metric:      Count(),
		Filter:      Eq("entity_type", "SalesTransaction"),
	})
	require.NoError(t, err)

	got := make(map[int64]int64)
	for _, row := range res.Rows {
		date := requireRowTime(t, row, "date")
		assert.Equal(t, time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), date)
		familySize := requireRowInt64(t, row, "family_size")
		got[familySize] = requireRowInt64(t, row, CountField)
	}

	assert.Equal(t, map[int64]int64{
		2: 2,
		4: 1,
	}, got)
}

func TestStoreQueryRowsAreSortedByDimensions(t *testing.T) {
	store := NewStore(t.TempDir())
	mustIngest := func(ts int64, row map[string]any) {
		_, _, err := store.IngestRow("entities", time.Unix(ts, 0).UTC(), row)
		require.NoError(t, err)
	}

	mustIngest(1704067400, map[string]any{"entity_type": "SalesTransaction", "family_size": int64(4)})
	mustIngest(1704067300, map[string]any{"entity_type": "SalesTransaction", "family_size": int64(2)})
	mustIngest(1704067200, map[string]any{"entity_type": "SalesTransaction", "family_size": int64(3)})

	res, err := store.Query(t.Context(), Query{
		Start:       time.Unix(1704067200, 0).UTC(),
		End:         time.Unix(1704153600, 0).UTC(),
		Dataset:     "entities",
		Dimensions:  []string{"date", "family_size"},
		Granularity: "day",
		Timezone:    "UTC",
		Metric:      Count(),
		Filter:      Eq("entity_type", "SalesTransaction"),
	})
	require.NoError(t, err)
	require.Len(t, res.Rows, 3)

	assert.Equal(t, int64(2), requireRowInt64(t, res.Rows[0], "family_size"))
	assert.Equal(t, int64(3), requireRowInt64(t, res.Rows[1], "family_size"))
	assert.Equal(t, int64(4), requireRowInt64(t, res.Rows[2], "family_size"))
}

func TestStoreQuerySupportsOrderByCountDescending(t *testing.T) {
	store := NewStore(t.TempDir())
	mustIngest := func(ts int64, row map[string]any) {
		_, _, err := store.IngestRow("entities", time.Unix(ts, 0).UTC(), row)
		require.NoError(t, err)
	}

	mustIngest(1704067200, map[string]any{"entity_type": "SalesTransaction", "family_size": int64(2)})
	mustIngest(1704067300, map[string]any{"entity_type": "SalesTransaction", "family_size": int64(2)})
	mustIngest(1704067400, map[string]any{"entity_type": "SalesTransaction", "family_size": int64(4)})

	res, err := store.Query(t.Context(), Query{
		Start:       time.Unix(1704067200, 0).UTC(),
		End:         time.Unix(1704153600, 0).UTC(),
		Dataset:     "entities",
		Dimensions:  []string{"family_size"},
		Granularity: "day",
		Timezone:    "UTC",
		Metrics:     []Metric{Count()},
		OrderBy:     []OrderBy{{Field: "count", Desc: true}},
	})
	require.NoError(t, err)
	require.Len(t, res.Rows, 2)

	assert.Equal(t, int64(2), requireRowInt64(t, res.Rows[0], CountField))
	assert.Equal(t, int64(2), requireRowInt64(t, res.Rows[0], "family_size"))
	assert.Equal(t, int64(1), requireRowInt64(t, res.Rows[1], CountField))
	assert.Equal(t, int64(4), requireRowInt64(t, res.Rows[1], "family_size"))
}

func TestStoreQuerySupportsOrderByDateAscending(t *testing.T) {
	store := NewStore(t.TempDir())
	mustIngest := func(ts int64, row map[string]any) {
		_, _, err := store.IngestRow("entities", time.Unix(ts, 0).UTC(), row)
		require.NoError(t, err)
	}

	// ingest reverse day order intentionally
	mustIngest(1704153600, map[string]any{"entity_type": "SalesTransaction"}) // 2024-01-02
	mustIngest(1704067200, map[string]any{"entity_type": "SalesTransaction"}) // 2024-01-01

	res, err := store.Query(t.Context(), Query{
		Start:       time.Unix(1704067200, 0).UTC(),
		End:         time.Unix(1704240000, 0).UTC(),
		Dataset:     "entities",
		Dimensions:  []string{"date"},
		Granularity: "day",
		Timezone:    "UTC",
		Metric:      Count(),
		OrderBy:     []OrderBy{{Field: "date", Desc: false}},
	})
	require.NoError(t, err)
	require.Len(t, res.Rows, 2)

	assert.Equal(t, time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), requireRowTime(t, res.Rows[0], "date"))
	assert.Equal(t, time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC), requireRowTime(t, res.Rows[1], "date"))
}

func TestStoreQueryGroupsByTwoStringDimensionsOverTime(t *testing.T) {
	store := NewStore(t.TempDir())
	mustIngest := func(ts int64, row map[string]any) {
		_, _, err := store.IngestRow("test_dataset", time.Unix(ts, 0).UTC(), row)
		require.NoError(t, err)
	}

	mustIngest(1704067200, map[string]any{"entity_type": "SalesTransaction", "product_name": "Tea", "product_description": "Green"})
	mustIngest(1704067300, map[string]any{"entity_type": "SalesTransaction", "product_name": "Tea", "product_description": "Black"})
	mustIngest(1704067400, map[string]any{"entity_type": "SalesTransaction", "product_name": "Tea", "product_description": "Green"})
	mustIngest(1706745600, map[string]any{"entity_type": "SalesTransaction", "product_name": "Tea", "product_description": "Green"})

	res, err := store.Query(t.Context(), Query{
		Start:       time.Unix(1704067200, 0).UTC(),
		End:         time.Unix(1709251200, 0).UTC(),
		Dataset:     "test_dataset",
		Dimensions:  []string{"date", "product_name", "product_description"},
		Granularity: "day",
		Timezone:    "UTC",
		Metric:      Count(),
		Filter:      Eq("entity_type", "SalesTransaction"),
	})
	require.NoError(t, err)

	got := make(map[string]int64)
	for _, row := range res.Rows {
		date := requireRowTime(t, row, "date")
		name := requireRowString(t, row, "product_name")
		desc := requireRowString(t, row, "product_description")
		got[date.Format("2006-01-02")+"|"+name+"|"+desc] = requireRowInt64(t, row, CountField)
	}

	assert.Equal(t, map[string]int64{
		"2024-01-01|Tea|Green": 2,
		"2024-01-01|Tea|Black": 1,
		"2024-02-01|Tea|Green": 1,
	}, got)
}

func TestStoreQueryWithoutFilterCountsAllRows(t *testing.T) {
	store := NewStore(t.TempDir())
	for _, ts := range []int64{1704067200, 1704067300, 1704067400} {
		_, _, err := store.IngestRow("entities", time.Unix(ts, 0).UTC(), map[string]any{"entity_type": "SalesTransaction"})
		require.NoError(t, err)
	}

	res, err := store.Query(t.Context(), Query{
		Start:       time.Unix(1704067200, 0).UTC(),
		End:         time.Unix(1704153600, 0).UTC(),
		Dataset:     "entities",
		Dimensions:  []string{"date"},
		Granularity: "day",
		Timezone:    "UTC",
		Metric:      Count(),
	})
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	assert.Equal(t, int64(3), requireRowInt64(t, res.Rows[0], CountField))
}

func TestStoreQueryTimeDimensionsTimezoneAndGranularity(t *testing.T) {
	store := NewStore(t.TempDir())
	// 2024-01-01T00:30:00Z => 2023-12-31 in America/New_York
	_, _, err := store.IngestRow("entities", time.Unix(1704069000, 0).UTC(), map[string]any{"entity_type": "SalesTransaction"})
	require.NoError(t, err)

	res, err := store.Query(t.Context(), Query{
		Start:       time.Unix(1704067200, 0).UTC(),
		End:         time.Unix(1704153600, 0).UTC(),
		Dataset:     "entities",
		Dimensions:  []string{"date", "day_of_week"},
		Granularity: "day",
		Timezone:    "America/New_York",
		Metric:      Count(),
		Filter:      Eq("entity_type", "SalesTransaction"),
	})
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	assert.Equal(t, time.Date(2023, 12, 31, 0, 0, 0, 0, timecalc.ResolveTimeZone("America/New_York")), requireRowTime(t, res.Rows[0], "date"))
	assert.Equal(t, "Sun", requireRowString(t, res.Rows[0], "day_of_week"))
}

func TestStoreQueryWithoutEntityTypeColumnStillCounts(t *testing.T) {
	store := NewStore(t.TempDir())
	_, _, err := store.IngestRow("entities", time.Unix(1704067200, 0).UTC(), map[string]any{
		"product_name": "Tea",
	})
	require.NoError(t, err)

	res, err := store.Query(t.Context(), Query{
		Start:       time.Unix(1704067200, 0).UTC(),
		End:         time.Unix(1704153600, 0).UTC(),
		Dataset:     "entities",
		Dimensions:  []string{"date"},
		Granularity: "day",
		Timezone:    "UTC",
		Metric:      Count(),
	})
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	assert.Equal(t, int64(1), requireRowInt64(t, res.Rows[0], CountField))
}

func TestStoreQueryEntityTypeDimensionWithoutColumnYieldsNil(t *testing.T) {
	store := NewStore(t.TempDir())
	_, _, err := store.IngestRow("entities", time.Unix(1704067200, 0).UTC(), map[string]any{
		"product_name": "Tea",
	})
	require.NoError(t, err)

	res, err := store.Query(t.Context(), Query{
		Start:       time.Unix(1704067200, 0).UTC(),
		End:         time.Unix(1704153600, 0).UTC(),
		Dataset:     "entities",
		Dimensions:  []string{"date", "entity_type"},
		Granularity: "day",
		Timezone:    "UTC",
		Metric:      Count(),
	})
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	assert.Equal(t, time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), requireRowTime(t, res.Rows[0], "date"))
	assert.True(t, res.Rows[0].Null("entity_type"))
}

func TestStoreQuerySupportsMultipleMetrics(t *testing.T) {
	store := NewStore(t.TempDir())
	mustIngest := func(ts int64, row map[string]any) {
		_, _, err := store.IngestRow("test_dataset", time.Unix(ts, 0).UTC(), row)
		require.NoError(t, err)
	}

	mustIngest(1704067200, map[string]any{"entity_type": "SalesTransaction", "product_name": "Tea", "value": int64(10), "customer": "A"})
	mustIngest(1704067300, map[string]any{"entity_type": "SalesTransaction", "product_name": "Tea", "value": int64(20), "customer": "A"})
	mustIngest(1704067400, map[string]any{"entity_type": "SalesTransaction", "product_name": "Tea", "value": int64(5), "customer": "B"})
	mustIngest(1704067500, map[string]any{"entity_type": "SalesTransaction", "product_name": "Coffee", "value": int64(7), "customer": "C"})

	res, err := store.Query(t.Context(), Query{
		Start:       time.Unix(1704067200, 0).UTC(),
		End:         time.Unix(1704153600, 0).UTC(),
		Dataset:     "test_dataset",
		Dimensions:  []string{"product_name"},
		Granularity: "day",
		Timezone:    "UTC",
		Metrics: []Metric{
			Count(),
			Sum("value"),
			Avg("value"),
			Min("value"),
			Max("value"),
			CountDistinct("customer"),
		},
		Filter: Eq("entity_type", "SalesTransaction"),
	})
	require.NoError(t, err)
	require.Len(t, res.Rows, 2)

	got := make(map[string]QueryResultRow, len(res.Rows))
	for _, row := range res.Rows {
		product := requireRowString(t, row, "product_name")
		got[product] = row
	}

	tea := got["Tea"]
	assert.Equal(t, int64(3), requireRowInt64(t, tea, CountField))
	assert.Equal(t, int64(3), requireRowInt64(t, tea, "count"))
	assert.Equal(t, int64(35), requireRowInt64(t, tea, "sum(value)"))
	assert.InDelta(t, float64(35)/3.0, requireRowFloat64(t, tea, "avg(value)"), 0.00001)
	assert.Equal(t, int64(5), requireRowInt64(t, tea, "min(value)"))
	assert.Equal(t, int64(20), requireRowInt64(t, tea, "max(value)"))
	assert.Equal(t, int64(2), requireRowInt64(t, tea, "count(customer)"))

	coffee := got["Coffee"]
	assert.Equal(t, int64(1), requireRowInt64(t, coffee, CountField))
	assert.Equal(t, int64(1), requireRowInt64(t, coffee, "count"))
	assert.Equal(t, int64(7), requireRowInt64(t, coffee, "sum(value)"))
	assert.Equal(t, 7.0, requireRowFloat64(t, coffee, "avg(value)"))
	assert.Equal(t, int64(7), requireRowInt64(t, coffee, "min(value)"))
	assert.Equal(t, int64(7), requireRowInt64(t, coffee, "max(value)"))
	assert.Equal(t, int64(1), requireRowInt64(t, coffee, "count(customer)"))
}

func TestStoreQueryCountDistinctStringsAcrossPartitionsApproximate(t *testing.T) {
	store := NewStore(t.TempDir())

	startJan := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	startFeb := time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC)
	expected := 5000
	rows := make([]RowInput, 0, expected*2)
	for i := range expected {
		ts := startJan.Add(time.Second * time.Duration(i%1000))
		if i%2 == 1 {
			ts = startFeb.Add(time.Duration(i%1000) * time.Second)
		}
		customer := fmt.Sprintf("customer-%d", i)
		rows = append(rows,
			RowInput{ts, map[string]any{
				"entity_type": "SalesTransaction",
				"group":       "A",
				"customer":    customer,
			}},
			RowInput{ts.Add(time.Second), map[string]any{
				"entity_type": "SalesTransaction",
				"group":       "A",
				"customer":    customer,
			}},
		)
	}

	_, err := store.IngestBatch("test_dataset", rows)
	require.NoError(t, err)

	res, err := store.Query(t.Context(), Query{
		Start:       startJan,
		End:         time.Date(2024, 3, 1, 0, 0, 0, 0, time.UTC),
		Dataset:     "test_dataset",
		Dimensions:  []string{"group"},
		Granularity: "day",
		Timezone:    "UTC",
		Metrics:     []Metric{CountDistinct("customer")},
		Filter:      Eq("entity_type", "SalesTransaction"),
	})
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	assert.InDelta(t, expected, requireRowInt64(t, res.Rows[0], "count(customer)"), float64(expected)*0.05)
}

func TestStoreQueryAvgAcrossPartitionsWeightedMerge(t *testing.T) {
	store := NewStore(t.TempDir())
	mustIngest := func(ts int64, value int64) {
		_, _, err := store.IngestRow("entities", time.Unix(ts, 0).UTC(), map[string]any{
			"entity_type": "SalesTransaction",
			"group":       "A",
			"value":       value,
		})
		require.NoError(t, err)
	}

	mustIngest(1704067200, 10) // 2024-01
	mustIngest(1704153600, 20) // 2024-01
	mustIngest(1706745600, 40) // 2024-02

	res, err := store.Query(t.Context(), Query{
		Start:       time.Unix(1704067200, 0).UTC(),
		End:         time.Unix(1709251200, 0).UTC(),
		Dataset:     "entities",
		Dimensions:  []string{"group"},
		Granularity: "day",
		Timezone:    "UTC",
		Metrics:     []Metric{Avg("value")},
		Filter:      Eq("entity_type", "SalesTransaction"),
	})
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	assert.InDelta(t, float64(70)/3.0, requireRowFloat64(t, res.Rows[0], "avg(value)"), 0.00001)
}

func TestStoreQueryEqFilterSurface(t *testing.T) {
	store := NewStore(t.TempDir())
	mustIngest := func(ts int64, row map[string]any) {
		_, _, err := store.IngestRow("entities", time.Unix(ts, 0).UTC(), row)
		require.NoError(t, err)
	}

	baseTS := int64(1704067200)
	mustIngest(baseTS, map[string]any{
		"entity_ref":                     "TestOrder/order-1",
		"aspects.presentation.label":     "Alice",
		"aspects.demography.family_size": int64(5),
	})
	mustIngest(baseTS+60, map[string]any{
		"entity_ref":                     "TestOrder/order-2",
		"aspects.presentation.label":     "Bob",
		"aspects.demography.family_size": int64(3),
	})

	assertCount := func(t *testing.T, filter Filter, expected int64) {
		t.Helper()
		res, err := store.Query(t.Context(), Query{
			Start:   time.Unix(baseTS, 0).UTC(),
			End:     time.Unix(baseTS+3600, 0).UTC(),
			Dataset: "entities",
			Metric:  Count(),
			Filter:  filter,
		})
		require.NoError(t, err)
		require.Len(t, res.Rows, 1)
		assert.Equal(t, expected, requireRowInt64(t, res.Rows[0], CountField))
	}

	t.Run("entity_ref", func(t *testing.T) {
		assertCount(t, Eq("entity_ref", "TestOrder/order-1"), 1)
	})

	t.Run("nested string field path", func(t *testing.T) {
		assertCount(t, Eq("aspects.presentation.label", "Alice"), 1)
	})

	t.Run("nested numeric field path", func(t *testing.T) {
		assertCount(t, Eq("aspects.demography.family_size", 5), 1)
	})
}

func TestStoreQuerySupportsAndFilter(t *testing.T) {
	store := NewStore(t.TempDir())
	mustIngest := func(ts int64, row map[string]any) {
		_, _, err := store.IngestRow("entities", time.Unix(ts, 0).UTC(), row)
		require.NoError(t, err)
	}

	baseTS := int64(1704067200)
	mustIngest(baseTS, map[string]any{"entity_type": "SalesTransaction", "site": "A"})
	mustIngest(baseTS+60, map[string]any{"entity_type": "SalesTransaction", "site": "B"})
	mustIngest(baseTS+120, map[string]any{"entity_type": "Invoice", "site": "A"})

	res, err := store.Query(t.Context(), Query{
		Start:   time.Unix(baseTS, 0).UTC(),
		End:     time.Unix(baseTS+3600, 0).UTC(),
		Dataset: "entities",
		Metric:  Count(),
		Filter: And(
			Eq("entity_type", "SalesTransaction"),
			Eq("site", "A"),
		),
	})
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	assert.Equal(t, int64(1), requireRowInt64(t, res.Rows[0], CountField))
}

func TestStoreQuerySupportsOrFilter(t *testing.T) {
	store := NewStore(t.TempDir())
	mustIngest := func(ts int64, row map[string]any) {
		_, _, err := store.IngestRow("entities", time.Unix(ts, 0).UTC(), row)
		require.NoError(t, err)
	}

	baseTS := int64(1704067200)
	mustIngest(baseTS, map[string]any{"entity_type": "SalesTransaction", "site": "A"})
	mustIngest(baseTS+60, map[string]any{"entity_type": "Invoice", "site": "B"})
	mustIngest(baseTS+120, map[string]any{"entity_type": "Resident", "site": "C"})

	res, err := store.Query(t.Context(), Query{
		Start:   time.Unix(baseTS, 0).UTC(),
		End:     time.Unix(baseTS+3600, 0).UTC(),
		Dataset: "entities",
		Metric:  Count(),
		Filter: Or(
			Eq("entity_type", "SalesTransaction"),
			Eq("site", "B"),
		),
	})
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	assert.Equal(t, int64(2), requireRowInt64(t, res.Rows[0], CountField))
}

func TestStoreQueryPercentilesAcrossThreePartitions(t *testing.T) {
	store := NewStore(t.TempDir())

	ingestSeries := func(base time.Time, startValue int64) {
		for i := range 100 {
			ts := base.Add(time.Duration(i) * time.Minute)
			_, _, err := store.IngestRow("entities", ts, map[string]any{
				"entity_type": "SalesTransaction",
				"value":       startValue + int64(i),
			})
			require.NoError(t, err)
		}
	}

	// 3 monthly partitions, 100 values each => 300 values total.
	ingestSeries(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), 1)
	ingestSeries(time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC), 101)
	ingestSeries(time.Date(2024, 3, 1, 0, 0, 0, 0, time.UTC), 201)

	res, err := store.Query(t.Context(), Query{
		Start:   time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		End:     time.Date(2024, 4, 1, 0, 0, 0, 0, time.UTC),
		Dataset: "entities",
		Metrics: []Metric{
			Count(),
			P50("value"),
			P75("value"),
			P90("value"),
			P95("value"),
			P99("value"),
			P999("value"),
		},
		Filter: Eq("entity_type", "SalesTransaction"),
	})
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)

	row := res.Rows[0]
	assert.Equal(t, int64(300), requireRowInt64(t, row, "count"))

	// Expected exact quantiles for values [1..300] using linear interpolation.
	// t-digest is approximate; assert with conservative tolerances.
	assert.InDelta(t, 150.5, requireRowFloat64(t, row, "p50(value)"), 2.0)
	assert.InDelta(t, 225.25, requireRowFloat64(t, row, "p75(value)"), 2.0)
	assert.InDelta(t, 270.1, requireRowFloat64(t, row, "p90(value)"), 3.0)
	assert.InDelta(t, 285.05, requireRowFloat64(t, row, "p95(value)"), 3.0)
	assert.InDelta(t, 297.01, requireRowFloat64(t, row, "p99(value)"), 4.0)
	assert.InDelta(t, 299.701, requireRowFloat64(t, row, "p999(value)"), 4.0)
}

func TestStoreQueryReturnsCanceledContextError(t *testing.T) {
	store := NewStore(t.TempDir())
	_, _, err := store.IngestRow("entities", time.Unix(1704067200, 0).UTC(), map[string]any{"entity_type": "SalesTransaction"})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	_, err = store.Query(ctx, Query{
		Start:   time.Unix(1704067200, 0).UTC(),
		End:     time.Unix(1704153600, 0).UTC(),
		Dataset: "entities",
		Metric:  Count(),
	})
	require.ErrorIs(t, err, context.Canceled)
}
