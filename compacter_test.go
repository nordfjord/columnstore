package columnstore

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCompactionKeepsLatestByKeySinglePartition(t *testing.T) {
	store := NewStore(t.TempDir())
	start := time.Unix(1704067200, 0).UTC()
	end := start.Add(time.Hour)

	results, err := store.IngestBatch("entities", []RowInput{
		{Timestamp: start, Values: map[string]any{"entity_ref": "resident/1", "value": int64(10)}},
		{Timestamp: start.Add(10 * time.Second), Values: map[string]any{"entity_ref": "resident/1", "value": int64(20)}},
	})
	require.NoError(t, err)
	require.Len(t, results, 2)

	before := queryCountAndSumForRef(t, store, start, end, "resident/1")
	assert.Equal(t, int64(2), before.count)
	assert.Equal(t, int64(30), before.sum)

	partition := results[0].Partition
	require.NoError(t, store.CompactPartition(t.Context(), "entities", partition, "entity_ref"))

	after := queryCountAndSumForRef(t, store, start, end, "resident/1")
	assert.Equal(t, int64(1), after.count)
	assert.Equal(t, int64(20), after.sum)
}

func TestCompactionKeepsDistinctKeys(t *testing.T) {
	store := NewStore(t.TempDir())
	start := time.Unix(1704067200, 0).UTC()
	end := start.Add(time.Hour)

	results, err := store.IngestBatch("entities", []RowInput{
		{Timestamp: start, Values: map[string]any{"entity_ref": "resident/1", "value": int64(10)}},
		{Timestamp: start.Add(10 * time.Second), Values: map[string]any{"entity_ref": "resident/2", "value": int64(100)}},
		{Timestamp: start.Add(20 * time.Second), Values: map[string]any{"entity_ref": "resident/1", "value": int64(11)}},
		{Timestamp: start.Add(30 * time.Second), Values: map[string]any{"entity_ref": "resident/2", "value": int64(101)}},
	})
	require.NoError(t, err)
	partition := results[0].Partition

	before := queryCountAndSumByRef(t, store, start, end)
	assert.Equal(t, map[string]countAndSum{
		"resident/1": {count: 2, sum: 21},
		"resident/2": {count: 2, sum: 201},
	}, before)

	require.NoError(t, store.CompactPartition(t.Context(), "entities", partition, "entity_ref"))

	after := queryCountAndSumByRef(t, store, start, end)
	assert.Equal(t, map[string]countAndSum{
		"resident/1": {count: 1, sum: 11},
		"resident/2": {count: 1, sum: 101},
	}, after)
}

func TestCompactionMissingKeyColumnFails(t *testing.T) {
	store := NewStore(t.TempDir())
	results, err := store.IngestBatch("entities", []RowInput{{
		Timestamp: time.Unix(1704067200, 0).UTC(),
		Values:    map[string]any{"entity_ref": "resident/1", "value": int64(10)},
	}})
	require.NoError(t, err)

	err = store.CompactPartition(t.Context(), "entities", results[0].Partition, "missing_key")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

func TestCompactionIdempotent(t *testing.T) {
	store := NewStore(t.TempDir())
	start := time.Unix(1704067200, 0).UTC()
	end := start.Add(time.Hour)

	results, err := store.IngestBatch("entities", []RowInput{
		{Timestamp: start, Values: map[string]any{"entity_ref": "resident/1", "value": int64(10)}},
		{Timestamp: start.Add(10 * time.Second), Values: map[string]any{"entity_ref": "resident/1", "value": int64(20)}},
	})
	require.NoError(t, err)
	partition := results[0].Partition

	require.NoError(t, store.CompactPartition(t.Context(), "entities", partition, "entity_ref"))
	first := queryCountAndSumForRef(t, store, start, end, "resident/1")

	require.NoError(t, store.CompactPartition(t.Context(), "entities", partition, "entity_ref"))
	second := queryCountAndSumForRef(t, store, start, end, "resident/1")

	assert.Equal(t, countAndSum{count: 1, sum: 20}, first)
	assert.Equal(t, first, second)
}

type countAndSum struct {
	count int64
	sum   int64
}

func queryCountAndSumForRef(t *testing.T, store *Store, start, end time.Time, ref string) countAndSum {
	t.Helper()

	res, err := store.Query(t.Context(), Query{
		Start:   start,
		End:     end,
		Dataset: "entities",
		Metrics: []Metric{Count(), Sum("value")},
		Filter:  Eq("entity_ref", ref),
	})
	require.NoError(t, err)
	if len(res.Rows) == 0 {
		return countAndSum{}
	}
	require.Len(t, res.Rows, 1)
	row := res.Rows[0]
	sum := requireRowInt64(t, row, "sum(value)")
	return countAndSum{count: requireRowInt64(t, row, CountField), sum: sum}
}

func queryCountAndSumByRef(t *testing.T, store *Store, start, end time.Time) map[string]countAndSum {
	t.Helper()

	res, err := store.Query(t.Context(), Query{
		Start:      start,
		End:        end,
		Dataset:    "entities",
		Metrics:    []Metric{Count(), Sum("value")},
		Dimensions: []string{"entity_ref"},
	})
	require.NoError(t, err)

	out := make(map[string]countAndSum, len(res.Rows))
	for _, row := range res.Rows {
		ref := requireRowString(t, row, "entity_ref")
		sum := requireRowInt64(t, row, "sum(value)")
		out[ref] = countAndSum{count: requireRowInt64(t, row, CountField), sum: sum}
	}
	return out
}
