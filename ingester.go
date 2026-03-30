package columnstore

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/nordfjord/columnstore/internal/colkind"
	"github.com/nordfjord/columnstore/internal/format"
)

const defaultDataset = "entities"

// PartitionRowIDAllocator provides monotonic row IDs per partition key.
type PartitionRowIDAllocator struct {
	mu   sync.Mutex
	next map[string]int32
}

func NewPartitionRowIDAllocator() *PartitionRowIDAllocator {
	return &PartitionRowIDAllocator{next: make(map[string]int32)}
}

// Next returns the next row ID for the given partition key.
func (a *PartitionRowIDAllocator) Next(partitionKey string) int32 {
	a.mu.Lock()
	defer a.mu.Unlock()

	n := a.next[partitionKey]
	a.next[partitionKey] = n + 1
	return n
}

// EnsureNextAtLeast bumps the next row ID floor for a partition.
func (a *PartitionRowIDAllocator) EnsureNextAtLeast(partitionKey string, next int32) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if current, ok := a.next[partitionKey]; !ok || next > current {
		a.next[partitionKey] = next
	}
}

// Ingester appends sparse row tuples to partition column files.
type Ingester struct {
	basePath    string
	allocator   *PartitionRowIDAllocator
	partitioner Partitioner

	locksMu        sync.Mutex
	partitionLocks map[string]*sync.Mutex
	partitionKinds map[string]map[string]colkind.ColumnKind
	partitionInit  map[string]bool
}

func NewIngester(basePath string, allocator *PartitionRowIDAllocator, partitioner Partitioner) *Ingester {
	if allocator == nil {
		panic("allocator is required")
	}
	if partitioner == nil {
		panic("partitioner is required")
	}
	return &Ingester{
		basePath:       basePath,
		allocator:      allocator,
		partitioner:    partitioner,
		partitionLocks: make(map[string]*sync.Mutex),
		partitionKinds: make(map[string]map[string]colkind.ColumnKind),
		partitionInit:  make(map[string]bool),
	}
}

// IngestRow routes the row to a monthly partition and appends sparse tuples.
// Returns assigned rowID and partition.
func (i *Ingester) IngestRow(dataset string, timestamp time.Time, row map[string]any) (int32, string, error) {
	results, err := i.IngestBatch(dataset, []RowInput{{Timestamp: timestamp, Values: row}})
	if err != nil {
		return 0, "", err
	}
	if len(results) != 1 {
		return 0, "", fmt.Errorf("unexpected ingest result count %d", len(results))
	}
	return results[0].RowID, results[0].Partition, nil
}

type batchItem struct {
	idx int
	row RowInput
}

// IngestBatch appends multiple rows and flushes once per touched partition.
func (i *Ingester) IngestBatch(dataset string, rows []RowInput) ([]IngestResult, error) {
	if len(rows) == 0 {
		return nil, nil
	}

	dataset, err := resolveDataset(dataset)
	if err != nil {
		return nil, err
	}

	byPartition := make(map[string][]batchItem)
	ordered := make([]string, 0)
	seen := make(map[string]bool)
	for idx, row := range rows {
		if row.Values == nil {
			return nil, fmt.Errorf("row is nil")
		}
		partition := i.partitioner.FromTimestamp(row.Timestamp)
		partitionKey := dataset + "|" + partition
		if !seen[partitionKey] {
			seen[partitionKey] = true
			ordered = append(ordered, partitionKey)
		}
		byPartition[partitionKey] = append(byPartition[partitionKey], batchItem{idx: idx, row: row})
	}
	slices.Sort(ordered)

	results := make([]IngestResult, len(rows))
	workerCount := min(runtime.NumCPU(), len(ordered))
	if workerCount <= 0 {
		workerCount = 1
	}
	sem := make(chan struct{}, workerCount)
	errCh := make(chan error, len(ordered))
	var wg sync.WaitGroup

	for _, partitionKey := range ordered {
		partition := partitionKey[len(dataset)+1:]

		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			if err := i.ingestPartitionBatch(dataset, partition, partitionKey, byPartition[partitionKey], results); err != nil {
				errCh <- err
			}
		}()
	}

	wg.Wait()
	close(errCh)
	if len(errCh) > 0 {
		return nil, <-errCh
	}

	return results, nil
}

func (i *Ingester) ingestPartitionBatch(dataset, partition, partitionKey string, items []batchItem, results []IngestResult) error {
	lock := i.partitionLock(partitionKey)
	lock.Lock()
	defer lock.Unlock()

	partitionPath := filepath.Join(i.basePath, dataset, partition)
	if err := os.MkdirAll(partitionPath, 0755); err != nil {
		return fmt.Errorf("creating partition path: %w", err)
	}

	kinds, err := i.loadPartitionKinds(partitionKey, partitionPath)
	if err != nil {
		return err
	}
	if !i.isPartitionInitialized(partitionKey) {
		if err := i.ensurePartitionNextRowID(partitionKey, partitionPath); err != nil {
			return err
		}
		i.markPartitionInitialized(partitionKey)
	}

	openFiles := make(map[string]*os.File)
	touched := make([]*os.File, 0)
	lastRowIDByColumn := make(map[string]int32)
	defer func() {
		for _, f := range openFiles {
			_ = f.Close()
		}
	}()

	for _, item := range items {
		rowID := i.allocator.Next(partitionKey)
		results[item.idx] = IngestResult{RowID: rowID, Partition: partition}

		tsUnix := item.row.Timestamp.Unix()
		if err := i.appendColumnValueOpenFile(partitionPath, kinds, openFiles, &touched, lastRowIDByColumn, "timestamp", rowID, tsUnix); err != nil {
			return fmt.Errorf("appending column timestamp: %w", err)
		}

		columns := make([]string, 0, len(item.row.Values))
		for col := range item.row.Values {
			columns = append(columns, col)
		}
		slices.Sort(columns)
		for _, col := range columns {
			if col == "timestamp" {
				continue
			}
			value := item.row.Values[col]
			if value == nil {
				continue
			}
			if err := i.appendColumnValueOpenFile(partitionPath, kinds, openFiles, &touched, lastRowIDByColumn, col, rowID, value); err != nil {
				return fmt.Errorf("appending column %s: %w", col, err)
			}
		}
	}

	if err := format.FlushPartition(partitionPath, touched, "", nil); err != nil {
		return fmt.Errorf("flush partition: %w", err)
	}

	return nil
}

func (i *Ingester) appendColumnValueOpenFile(
	partitionPath string,
	kinds map[string]colkind.ColumnKind,
	openFiles map[string]*os.File,
	touched *[]*os.File,
	lastRowIDByColumn map[string]int32,
	column string,
	rowID int32,
	value any,
) error {
	value, err := normalizeValueForStorage(value)
	if err != nil {
		return fmt.Errorf("normalize column %s value: %w", column, err)
	}

	kind, ext, err := colkind.InferColumnKind(value)
	if err != nil {
		return err
	}
	if existingKind, exists := kinds[column]; exists && existingKind != kind {
		return fmt.Errorf("column type mismatch for %s: existing kind=%d incoming kind=%d", column, existingKind, kind)
	}
	if last, ok := lastRowIDByColumn[column]; ok && rowID <= last {
		return fmt.Errorf("rowid must be strictly increasing for %s: last=%d next=%d", column, last, rowID)
	}

	path := filepath.Join(partitionPath, column+"."+ext)
	f := openFiles[path]
	if f == nil {
		f, err = os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
		if err != nil {
			return fmt.Errorf("opening column file: %w", err)
		}
		if _, err := f.Seek(0, 2); err != nil {
			_ = f.Close()
			return fmt.Errorf("seek end: %w", err)
		}
		openFiles[path] = f
		*touched = append(*touched, f)
	}

	switch v := value.(type) {
	case int64:
		if err := format.NewInt64TupleWriter(f).Write(rowID, v); err != nil {
			return err
		}
	case float64:
		if err := format.NewFloat64TupleWriter(f).Write(rowID, v); err != nil {
			return err
		}
	case bool:
		if err := format.NewBoolTupleWriter(f).Write(rowID, v); err != nil {
			return err
		}
	case string:
		if err := format.NewStringTupleWriter(f).Write(rowID, v); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unsupported column value type %T", value)
	}

	kinds[column] = kind
	lastRowIDByColumn[column] = rowID
	return nil
}

func normalizeValueForStorage(value any) (any, error) {
	switch value.(type) {
	case int64, float64, bool, string:
		return value, nil
	}

	b, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	return string(b), nil
}

func (i *Ingester) loadPartitionKinds(partitionKey, partitionPath string) (map[string]colkind.ColumnKind, error) {
	i.locksMu.Lock()
	if kinds, ok := i.partitionKinds[partitionKey]; ok {
		i.locksMu.Unlock()
		return kinds, nil
	}
	i.locksMu.Unlock()

	entries, err := os.ReadDir(partitionPath)
	if err != nil {
		return nil, fmt.Errorf("read partition dir: %w", err)
	}

	kinds := make(map[string]colkind.ColumnKind)
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		for kind, ext := range colkind.KindToExt {
			suffix := "." + ext
			if !strings.HasSuffix(name, suffix) {
				continue
			}
			col := name[:len(name)-len(suffix)]

			k, exists := kinds[col]
			if exists && k != kind {
				return nil, fmt.Errorf("multiple type files for column %s in %s", col, partitionPath)
			}
			kinds[col] = kind
		}
	}

	i.locksMu.Lock()
	i.partitionKinds[partitionKey] = kinds
	i.locksMu.Unlock()
	return kinds, nil
}

func (i *Ingester) isPartitionInitialized(partitionKey string) bool {
	i.locksMu.Lock()
	defer i.locksMu.Unlock()
	return i.partitionInit[partitionKey]
}

func (i *Ingester) markPartitionInitialized(partitionKey string) {
	i.locksMu.Lock()
	defer i.locksMu.Unlock()
	i.partitionInit[partitionKey] = true
}

func (i *Ingester) ensurePartitionNextRowID(partitionKey, partitionPath string) error {
	path := filepath.Join(partitionPath, "timestamp.int64")
	st, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("stat timestamp file: %w", err)
	}
	if st.Size() == 0 {
		return nil
	}

	if st.Size() < 12 {
		return fmt.Errorf("timestamp file too short for tuples")
	}
	lastOffset := st.Size() - 12
	var last [12]byte
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open timestamp file: %w", err)
	}
	defer f.Close()
	if _, err := f.ReadAt(last[:], lastOffset); err != nil {
		return fmt.Errorf("read last timestamp tuple: %w", err)
	}
	lastRowID := int32(binary.LittleEndian.Uint32(last[0:4]))
	next := lastRowID + 1
	if next < 0 {
		return fmt.Errorf("invalid next rowid computed")
	}
	i.allocator.EnsureNextAtLeast(partitionKey, next)
	return nil
}

func (i *Ingester) partitionLock(partitionKey string) *sync.Mutex {
	i.locksMu.Lock()
	defer i.locksMu.Unlock()

	if l, ok := i.partitionLocks[partitionKey]; ok {
		return l
	}
	l := &sync.Mutex{}
	i.partitionLocks[partitionKey] = l
	return l
}

func resolveDataset(dataset string) (string, error) {
	if dataset == "" {
		return defaultDataset, nil
	}

	for _, r := range dataset {
		if (r >= 'a' && r <= 'z') ||
			(r >= 'A' && r <= 'Z') ||
			(r >= '0' && r <= '9') ||
			r == '_' ||
			r == '-' {
			continue
		}
		return "", fmt.Errorf("invalid dataset name %q", dataset)
	}

	return dataset, nil
}
