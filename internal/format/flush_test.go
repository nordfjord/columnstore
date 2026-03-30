package format

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFlushPartitionWritesMetadataAtomically(t *testing.T) {
	base := t.TempDir()
	partitionDir := filepath.Join(base, "entities", "2024-01")
	require.NoError(t, os.MkdirAll(partitionDir, 0755))

	dataPath := filepath.Join(partitionDir, "timestamp.int64")
	data, err := os.OpenFile(dataPath, os.O_CREATE|os.O_RDWR, 0644)
	require.NoError(t, err)
	defer data.Close()
	_, err = data.Write([]byte("data"))
	require.NoError(t, err)

	err = FlushPartition(partitionDir, []*os.File{data}, "_stats.json", []byte(`{"row_count":1}`))
	require.NoError(t, err)

	b, err := os.ReadFile(filepath.Join(partitionDir, "_stats.json"))
	require.NoError(t, err)
	assert.Equal(t, `{"row_count":1}`, string(b))

	_, err = os.Stat(filepath.Join(partitionDir, "_stats.json.tmp"))
	assert.Error(t, err)
}

func TestFlushPartitionCreatesPartitionDir(t *testing.T) {
	base := t.TempDir()
	partitionDir := filepath.Join(base, "entities", "2024-02")

	err := FlushPartition(partitionDir, nil, "_stats.json", []byte("{}"))
	require.NoError(t, err)

	_, err = os.Stat(filepath.Join(partitionDir, "_stats.json"))
	require.NoError(t, err)
}
