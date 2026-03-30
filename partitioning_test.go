package columnstore

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPartitionerFromFormat_Monthly(t *testing.T) {
	p := PartitionerFromFormat("monthly")
	ts := time.Date(2024, 2, 10, 13, 0, 0, 0, time.UTC)
	assert.Equal(t, "2024-02", p.FromTimestamp(ts))
	assert.True(t, p.IsInBounds("2024-02", time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC), time.Date(2024, 3, 1, 0, 0, 0, 0, time.UTC)))
	assert.False(t, p.IsInBounds("2024-01", time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC), time.Date(2024, 3, 1, 0, 0, 0, 0, time.UTC)))
}

func TestPartitionerFromFormat_Yearly(t *testing.T) {
	p := PartitionerFromFormat("yearly")
	ts := time.Date(2024, 2, 10, 13, 0, 0, 0, time.UTC)
	assert.Equal(t, "2024", p.FromTimestamp(ts))
	assert.True(t, p.IsInBounds("2024", time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC), time.Date(2024, 3, 1, 0, 0, 0, 0, time.UTC)))
	assert.False(t, p.IsInBounds("2023", time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC), time.Date(2024, 3, 1, 0, 0, 0, 0, time.UTC)))
}

func TestStoreWithYearlyPartitioning(t *testing.T) {
	store := NewStore(t.TempDir(), WithPartitioning(PartitionerFromFormat("yearly")))

	_, partitionOne, err := store.IngestRow("entities", time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), map[string]any{"entity_type": "Test"})
	require.NoError(t, err)
	_, partitionTwo, err := store.IngestRow("entities", time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC), map[string]any{"entity_type": "Test"})
	require.NoError(t, err)

	assert.Equal(t, "2024", partitionOne)
	assert.Equal(t, "2024", partitionTwo)
}

func TestPruneByPartitionBounds_UsesPartitioner(t *testing.T) {
	partitions := []string{"2023", "2024", "2025"}
	start := time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2024, 7, 1, 0, 0, 0, 0, time.UTC)

	out := pruneByPartitionBounds(partitions, start, end, PartitionerFromFormat("yearly"))
	assert.Equal(t, []string{"2024"}, out)
}
