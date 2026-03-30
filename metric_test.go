package columnstore

import (
	"slices"
	"testing"

	"github.com/nordfjord/columnstore/internal/metricconv"
	"github.com/stretchr/testify/require"
)

func TestCountAccumulatorGlobal(t *testing.T) {
	a := newCountAccumulator()

	a.Add(nil)
	a.Add(nil)
	a.Add([]any{})

	require.Equal(t, int64(3), a.CountFor(nil))
	rows := a.Rows()
	require.Len(t, rows, 1)
	require.Equal(t, int64(3), requireRowInt64(t, rows[0], CountField))
}

func TestCountAccumulatorGrouped(t *testing.T) {
	a := newCountAccumulator()

	a.Add([]any{"Product A"})
	a.Add([]any{"Product A"})
	a.Add([]any{"Product B"})

	require.Equal(t, int64(2), a.CountFor([]any{"Product A"}))
	require.Equal(t, int64(1), a.CountFor([]any{"Product B"}))
	require.Equal(t, int64(0), a.CountFor([]any{"Product C"}))
}

func TestCountAccumulatorGroupedWithNil(t *testing.T) {
	a := newCountAccumulator()

	a.Add([]any{"Product A", nil})
	a.Add([]any{"Product A", nil})
	a.Add([]any{"Product A", "desc"})

	require.Equal(t, int64(2), a.CountFor([]any{"Product A", nil}))
	require.Equal(t, int64(1), a.CountFor([]any{"Product A", "desc"}))

	rows := a.Rows()
	require.Len(t, rows, 2)
}

func TestMetricAccumulatorRows(t *testing.T) {
	agg, err := NewMetricAccumulator([]Metric{
		Count(),
		Sum("value"),
		Avg("value"),
		Min("value"),
		Max("value"),
		CountDistinct("category"),
	})
	require.NoError(t, err)

	agg.Add([]any{"A"}, map[string]any{"value": int64(10), "category": "x"})
	agg.Add([]any{"A"}, map[string]any{"value": float64(20), "category": "x"})
	agg.Add([]any{"A"}, map[string]any{"value": int64(5), "category": "y"})
	agg.Add([]any{"B"}, map[string]any{"category": "z"})

	rows := agg.Rows()
	require.Len(t, rows, 2)

	require.Equal(t, int64(3), requireRowInt64(t, rows[0], "count"))
	require.Equal(t, int64(3), requireRowInt64(t, rows[0], CountField))
	require.Equal(t, 35.0, requireRowFloat64(t, rows[0], "sum(value)"))
	require.InDelta(t, 35.0/3.0, requireRowFloat64(t, rows[0], "avg(value)"), 0.00001)
	require.Equal(t, 5.0, requireRowFloat64(t, rows[0], "min(value)"))
	require.Equal(t, 20.0, requireRowFloat64(t, rows[0], "max(value)"))
	require.Equal(t, int64(2), requireRowInt64(t, rows[0], "count(category)"))

	require.Equal(t, int64(1), requireRowInt64(t, rows[1], "count"))
	require.Equal(t, int64(1), requireRowInt64(t, rows[1], CountField))
	require.Equal(t, 0.0, requireRowFloat64(t, rows[1], "sum(value)"))
	require.Equal(t, 0.0, requireRowFloat64(t, rows[1], "avg(value)"))
	require.Equal(t, 0.0, requireRowFloat64(t, rows[1], "min(value)"))
	require.Equal(t, 0.0, requireRowFloat64(t, rows[1], "max(value)"))
	require.Equal(t, int64(1), requireRowInt64(t, rows[1], "count(category)"))
}

func TestMetricAccumulatorMerge(t *testing.T) {
	left, err := NewMetricAccumulator([]Metric{Count(), Avg("value")})
	require.NoError(t, err)
	right, err := NewMetricAccumulator([]Metric{Count(), Avg("value")})
	require.NoError(t, err)

	left.Add([]any{"A"}, map[string]any{"value": int64(10)})
	left.Add([]any{"A"}, map[string]any{"value": int64(20)})
	right.Add([]any{"A"}, map[string]any{"value": int64(40)})

	require.NoError(t, left.Merge(right))

	rows := left.Rows()
	require.Len(t, rows, 1)
	require.Equal(t, int64(3), requireRowInt64(t, rows[0], CountField))
	require.InDelta(t, float64(70)/3.0, requireRowFloat64(t, rows[0], "avg(value)"), 0.00001)
}

func TestMetricAccumulatorCountDistinctStaysExactBelowThreshold(t *testing.T) {
	agg, err := NewMetricAccumulator([]Metric{CountDistinct("category")})
	require.NoError(t, err)

	for i := range distinctExactThreshold {
		value := int64(i)
		agg.Add([]any{"A"}, map[string]any{"category": value})
		agg.Add([]any{"A"}, map[string]any{"category": value})
	}

	rows := agg.Rows()
	require.Len(t, rows, 1)
	require.Equal(t, int64(distinctExactThreshold), requireRowInt64(t, rows[0], "count(category)"))
}

func TestMetricAccumulatorCountDistinctPromotesAboveThreshold(t *testing.T) {
	agg, err := NewMetricAccumulator([]Metric{CountDistinct("category")})
	require.NoError(t, err)

	expected := 5000
	for i := range expected {
		agg.Add([]any{"A"}, map[string]any{"category": int64(i)})
	}

	rows := agg.Rows()
	require.Len(t, rows, 1)
	require.InDelta(t, expected, requireRowInt64(t, rows[0], "count(category)"), float64(expected)*0.05)
}

func TestMetricAccumulatorCountDistinctMergeAboveThreshold(t *testing.T) {
	left, err := NewMetricAccumulator([]Metric{CountDistinct("category")})
	require.NoError(t, err)
	right, err := NewMetricAccumulator([]Metric{CountDistinct("category")})
	require.NoError(t, err)

	for i := range 3000 {
		left.Add([]any{"A"}, map[string]any{"category": int64(i)})
	}
	for i := 3000; i < 6000; i++ {
		right.Add([]any{"A"}, map[string]any{"category": int64(i)})
	}

	require.NoError(t, left.Merge(right))

	rows := left.Rows()
	require.Len(t, rows, 1)
	require.InDelta(t, 6000, requireRowInt64(t, rows[0], "count(category)"), 6000*0.05)
}

func TestToFloat64RejectsInvalidNumericString(t *testing.T) {
	_, ok := metricconv.CoerceToFloat64("not-a-number")
	require.False(t, ok)

	v, ok := metricconv.CoerceToFloat64("123.5")
	require.True(t, ok)
	require.Equal(t, 123.5, v)
}

func TestMetricAccumulatorPercentiles(t *testing.T) {
	agg, err := NewMetricAccumulator([]Metric{P50("value"), P95("value"), P999("value")})
	require.NoError(t, err)
	count := 1000

	values := make([]int64, 0, count*6)
	for range count {
		values = append(values, []int64{1, 2, 3, 4, 5, 100}...)
	}

	for _, v := range values {
		agg.Add([]any{"A"}, map[string]any{"value": v})
	}

	rows := agg.Rows()
	require.Len(t, rows, 1)

	slices.Sort(values)
	expectedP50 := float64(values[len(values)/2])
	expectedP95 := float64(values[len(values)*95/100])
	expectedP999 := float64(values[len(values)*999/1000])

	require.InDelta(t, expectedP50, requireRowFloat64(t, rows[0], "p50(value)"), 1)
	require.InDelta(t, expectedP95, requireRowFloat64(t, rows[0], "p95(value)"), 0.00001)
	require.InDelta(t, expectedP999, requireRowFloat64(t, rows[0], "p999(value)"), 0.00001)
}
