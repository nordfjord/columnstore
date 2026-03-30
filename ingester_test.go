package columnstore

import (
	"bytes"
	"encoding/binary"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/nordfjord/columnstore/internal/format"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var partitioner = defaultPartitioner()

func TestPartitionRowIDAllocatorConcurrent(t *testing.T) {
	alloc := NewPartitionRowIDAllocator()
	const n = 200

	ids := make(chan int32, n)
	var wg sync.WaitGroup
	for range n {
		wg.Go(func() {
			ids <- alloc.Next("entities|2026-02")
		})
	}
	wg.Wait()
	close(ids)

	seen := make(map[int32]bool, n)
	for id := range ids {
		seen[id] = true
	}
	require.Len(t, seen, n)
	for i := range n {
		assert.True(t, seen[int32(i)])
	}
}

func TestIngesterRoutesByMonthAndWritesTypedFiles(t *testing.T) {
	base := t.TempDir()
	alloc := NewPartitionRowIDAllocator()
	ing := NewIngester(base, alloc, partitioner)

	rowID, partition, err := ing.IngestRow("entities", time.Unix(1704067200, 0).UTC(), map[string]any{
		"entity_type": "SalesTransaction",
		"amount":      float64(10.5),
		"active":      true,
	})
	require.NoError(t, err)
	assert.Equal(t, int32(0), rowID)
	assert.Equal(t, "2024-01", partition)

	partitionPath := filepath.Join(base, "entities", "2024-01")
	require.FileExists(t, filepath.Join(partitionPath, "timestamp.int64"))
	require.FileExists(t, filepath.Join(partitionPath, "entity_type.str"))
	require.FileExists(t, filepath.Join(partitionPath, "amount.float64"))
	require.FileExists(t, filepath.Join(partitionPath, "active.bool"))

	rows, err := readAllInt64Tuples(filepath.Join(partitionPath, "timestamp.int64"))
	require.NoError(t, err)
	require.Len(t, rows, 1)
	assert.Equal(t, int32(0), rows[0].rowID)
	assert.Equal(t, int64(1704067200), rows[0].value)
}

func TestIngesterConcurrentSamePartitionMonotonicRowIDs(t *testing.T) {
	base := t.TempDir()
	alloc := NewPartitionRowIDAllocator()
	ing := NewIngester(base, alloc, partitioner)

	const n = 100
	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, _, err := ing.IngestRow("entities", time.Unix(1704067200, 0).UTC(), map[string]any{
				"entity_type": "SalesTransaction",
				"value":       int64(i),
			})
			require.NoError(t, err)
		}(i)
	}
	wg.Wait()

	path := filepath.Join(base, "entities", "2024-01", "timestamp.int64")
	rows, err := readAllInt64Tuples(path)
	require.NoError(t, err)
	require.Len(t, rows, n)

	for i, row := range rows {
		assert.Equal(t, int32(i), row.rowID)
	}
}

func TestIngesterRejectsInvalidInput(t *testing.T) {
	base := t.TempDir()
	alloc := NewPartitionRowIDAllocator()
	ing := NewIngester(base, alloc, partitioner)

	_, _, err := ing.IngestRow("entities", time.Now().UTC(), nil)
	require.Error(t, err)

	_, _, err = ing.IngestRow("bad/../dataset", time.Now().UTC(), map[string]any{"entity_type": "x"})
	require.Error(t, err)
}

func TestIngesterRejectsColumnTypeChangeAcrossRows(t *testing.T) {
	base := t.TempDir()
	alloc := NewPartitionRowIDAllocator()
	ing := NewIngester(base, alloc, partitioner)

	_, _, err := ing.IngestRow("entities", time.Unix(1704067200, 0).UTC(), map[string]any{
		"thing": int64(123),
	})
	require.NoError(t, err)

	_, _, err = ing.IngestRow("entities", time.Unix(1704067300, 0).UTC(), map[string]any{
		"thing": "123",
	})
	require.Error(t, err)

	partitionPath := filepath.Join(base, "entities", "2024-01")
	_, statErr := os.Stat(filepath.Join(partitionPath, "thing.int64"))
	require.NoError(t, statErr)
	_, statErr = os.Stat(filepath.Join(partitionPath, "thing.str"))
	assert.Error(t, statErr)
}

func TestIngesterInitializesNextRowIDFromTimestampFile(t *testing.T) {
	base := t.TempDir()
	alloc := NewPartitionRowIDAllocator()
	ing1 := NewIngester(base, alloc, partitioner)

	_, _, err := ing1.IngestRow("entities", time.Unix(1704067200, 0).UTC(), map[string]any{"entity_type": "A"})
	require.NoError(t, err)
	_, _, err = ing1.IngestRow("entities", time.Unix(1704067300, 0).UTC(), map[string]any{"entity_type": "A"})
	require.NoError(t, err)

	alloc2 := NewPartitionRowIDAllocator()
	ing2 := NewIngester(base, alloc2, partitioner)
	rowID, partition, err := ing2.IngestRow("entities", time.Unix(1704067400, 0).UTC(), map[string]any{"entity_type": "A"})
	require.NoError(t, err)
	assert.Equal(t, "2024-01", partition)
	assert.Equal(t, int32(2), rowID)
}

func TestLoadPartitionKindsRejectsInvalidInt64Payload(t *testing.T) {
	base := t.TempDir()
	partitionPath := filepath.Join(base, "entities", "2024-01")
	require.NoError(t, os.MkdirAll(partitionPath, 0755))

	filePath := filepath.Join(partitionPath, "entity_type.int64")
	require.NoError(t, os.WriteFile(filePath, []byte("not-an-int64-column"), 0644))

	alloc := NewPartitionRowIDAllocator()
	ing := NewIngester(base, alloc, partitioner)
	_, _, err := ing.IngestRow("entities", time.Unix(1704067200, 0).UTC(), map[string]any{"entity_type": "A"})
	require.Error(t, err)
}

func TestEnsurePartitionNextRowIDReadsLastTuple(t *testing.T) {
	base := t.TempDir()
	partitionPath := filepath.Join(base, "entities", "2024-01")
	require.NoError(t, os.MkdirAll(partitionPath, 0755))

	var payload [12]byte
	binary.LittleEndian.PutUint32(payload[0:4], uint32(41))
	binary.LittleEndian.PutUint64(payload[4:12], uint64(1704067200))

	path := filepath.Join(partitionPath, "timestamp.int64")
	require.NoError(t, os.WriteFile(path, payload[:], 0644))

	alloc := NewPartitionRowIDAllocator()
	ing := NewIngester(base, alloc, partitioner)
	rowID, _, err := ing.IngestRow("entities", time.Unix(1704067300, 0).UTC(), map[string]any{"entity_type": "A"})
	require.NoError(t, err)
	assert.Equal(t, int32(42), rowID)
}

func TestIngestBatchAssignsRowIDsPerPartition(t *testing.T) {
	base := t.TempDir()
	alloc := NewPartitionRowIDAllocator()
	ing := NewIngester(base, alloc, partitioner)

	results, err := ing.IngestBatch("entities", []RowInput{
		{Timestamp: time.Unix(1704067200, 0).UTC(), Values: map[string]any{"entity_type": "A"}},
		{Timestamp: time.Unix(1704067300, 0).UTC(), Values: map[string]any{"entity_type": "A"}},
		{Timestamp: time.Unix(1706745600, 0).UTC(), Values: map[string]any{"entity_type": "A"}},
	})
	require.NoError(t, err)
	require.Len(t, results, 3)
	assert.Equal(t, int32(0), results[0].RowID)
	assert.Equal(t, int32(1), results[1].RowID)
	assert.Equal(t, int32(0), results[2].RowID)
	assert.Equal(t, "2024-01", results[0].Partition)
	assert.Equal(t, "2024-01", results[1].Partition)
	assert.Equal(t, "2024-02", results[2].Partition)
}

func TestIngestBatchMarshalsUnsupportedValuesToJSONStrings(t *testing.T) {
	base := t.TempDir()
	alloc := NewPartitionRowIDAllocator()
	ing := NewIngester(base, alloc, partitioner)

	_, err := ing.IngestBatch("entities", []RowInput{
		{
			Timestamp: time.Unix(1704067200, 0).UTC(),
			Values: map[string]any{
				"entity_type": "SalesTransaction",
				"relations":   []string{"A/1", "B/2"},
			},
		},
	})
	require.NoError(t, err)

	partitionPath := filepath.Join(base, "entities", "2024-01")
	path := filepath.Join(partitionPath, "relations.str")
	require.FileExists(t, path)

	b, err := os.ReadFile(path)
	require.NoError(t, err)
	r := format.NewStringTupleReader(bytes.NewReader(b))
	_, got, err := r.Next()
	require.NoError(t, err)
	assert.Equal(t, `["A/1","B/2"]`, got)
}

type int64Tuple struct {
	rowID int32
	value int64
}

func readAllInt64Tuples(path string) ([]int64Tuple, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	r := format.NewInt64TupleReader(bytes.NewReader(b))
	out := make([]int64Tuple, 0)
	for {
		rowID, value, err := r.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		out = append(out, int64Tuple{rowID: rowID, value: value})
	}
	return out, nil
}
