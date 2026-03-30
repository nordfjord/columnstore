package columnstore

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/nordfjord/columnstore/internal/colkind"
	"github.com/nordfjord/columnstore/internal/format"
	"github.com/nordfjord/columnstore/internal/scan"
	"go.opentelemetry.io/otel/attribute"
)

type Compacter struct {
	ingester *Ingester
}

type compactionStats struct {
	BytesRead    int64
	BytesWritten int64
	Files        int
}

type fileCompactionStats struct {
	BytesRead    int64
	BytesWritten int64
}

func NewCompacter(ingester *Ingester) *Compacter {
	return &Compacter{ingester: ingester}
}

func (c *Compacter) CompactDataset(ctx context.Context, dataset, keyColumn string, includeActive bool) error {
	ctx, span := tracer.Start(ctx, "columnstore.dataset.compact")
	defer span.End()
	span.SetAttributes(
		attribute.String("columnstore.dataset", dataset),
		attribute.String("columnstore.key_column", keyColumn),
		attribute.Bool("columnstore.include_active", includeActive),
	)
	dataset, err := resolveDataset(dataset)
	if err != nil {
		return err
	}

	datasetPath := filepath.Join(c.ingester.basePath, dataset)
	entries, err := os.ReadDir(datasetPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("read dataset dir: %w", err)
	}

	partitions := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			partitions = append(partitions, entry.Name())
		}
	}
	if len(partitions) == 0 {
		return nil
	}
	slices.Sort(partitions)

	if !includeActive {
		partitions = partitions[:len(partitions)-1]
	}
	span.SetAttributes(attribute.Int("columnstore.dataset.partitions", len(partitions)))

	var totalRead int64
	var totalWritten int64
	filesCompacted := 0

	for _, partition := range partitions {
		stats, err := c.compactPartition(ctx, dataset, partition, keyColumn)
		if err != nil {
			return err
		}
		totalRead += stats.BytesRead
		totalWritten += stats.BytesWritten
		filesCompacted += stats.Files
	}

	span.SetAttributes(
		attribute.Int64("columnstore.compact.bytes_read", totalRead),
		attribute.Int64("columnstore.compact.bytes_written", totalWritten),
		attribute.Int("columnstore.compact.files", filesCompacted),
	)

	return nil
}

func (c *Compacter) CompactPartition(ctx context.Context, dataset, partition, keyColumn string) error {
	_, err := c.compactPartition(ctx, dataset, partition, keyColumn)
	return err
}

func (c *Compacter) compactPartition(ctx context.Context, dataset, partition, keyColumn string) (compactionStats, error) {
	ctx, span := tracer.Start(ctx, "columnstore.partition.compact")
	defer span.End()
	stats := compactionStats{}
	span.SetAttributes(
		attribute.String("columnstore.dataset", dataset),
		attribute.String("columnstore.partition", partition),
		attribute.String("columnstore.key_column", keyColumn),
	)
	if keyColumn == "" {
		err := fmt.Errorf("key column cannot be empty")
		span.RecordError(err)
		return stats, err
	}
	dataset, err := resolveDataset(dataset)
	if err != nil {
		span.RecordError(err)
		return stats, err
	}

	partitionKey := dataset + "|" + partition
	span.SetAttributes(attribute.String("columnstore.partition_key", partitionKey))
	start := time.Now()
	lock := c.ingester.partitionLock(partitionKey)
	lock.Lock()
	defer lock.Unlock()
	span.SetAttributes(attribute.Int64("columnstore.partition.lock_acquire_ms", time.Since(start).Milliseconds()))

	start = time.Now()
	partitionPath := filepath.Join(c.ingester.basePath, dataset, partition)
	stat, err := os.Stat(partitionPath)
	span.SetAttributes(attribute.Int64("columnstore.partition.stat_ms", time.Since(start).Milliseconds()))
	if err != nil {
		if os.IsNotExist(err) {
			return stats, nil
		}
		err := fmt.Errorf("stat partition path: %w", err)
		span.RecordError(err)
		return stats, err
	}
	if !stat.IsDir() {
		err := fmt.Errorf("partition path is not a directory: %s", partitionPath)
		span.RecordError(err)
		return stats, err
	}

	keyPath, keyKind, err := findColumnFile(partitionPath, keyColumn)
	if err != nil {
		span.RecordError(err)
		return stats, err
	}

	keepRowIDs, keyBytesRead, err := latestRowIDsByKey(ctx, keyPath, keyKind)
	if err != nil {
		span.RecordError(err)
		return stats, err
	}
	stats.BytesRead += keyBytesRead
	if len(keepRowIDs) == 0 {
		span.SetAttributes(
			attribute.Int64("columnstore.compact.bytes_read", stats.BytesRead),
			attribute.Int64("columnstore.compact.bytes_written", stats.BytesWritten),
			attribute.Int("columnstore.compact.files", stats.Files),
		)
		return stats, nil
	}

	entries, err := os.ReadDir(partitionPath)
	if err != nil {
		err := fmt.Errorf("read partition dir: %w", err)
		span.RecordError(err)
		return stats, err
	}

	span.SetAttributes(attribute.Int("columnstore.partition.compact.files", len(entries)))

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if !isColumnDataFile(name) {
			continue
		}
		path := filepath.Join(partitionPath, name)
		fileStats, err := compactColumnFile(ctx, path, keepRowIDs)
		if err != nil {
			span.RecordError(err)
			return stats, err
		}
		stats.BytesRead += fileStats.BytesRead
		stats.BytesWritten += fileStats.BytesWritten
		stats.Files++
	}

	_, syncSpan := tracer.Start(ctx, "columnstore.partition.compact.sync")
	defer syncSpan.End()

	dir, err := os.Open(partitionPath)
	if err != nil {
		err := fmt.Errorf("open partition dir: %w", err)
		syncSpan.RecordError(err)
		span.RecordError(err)
		return stats, err
	}
	defer dir.Close()
	if err := dir.Sync(); err != nil {
		err := fmt.Errorf("fsync partition dir: %w", err)
		syncSpan.RecordError(err)
		span.RecordError(err)
		return stats, err
	}

	span.SetAttributes(
		attribute.Int64("columnstore.compact.bytes_read", stats.BytesRead),
		attribute.Int64("columnstore.compact.bytes_written", stats.BytesWritten),
		attribute.Int("columnstore.compact.files", stats.Files),
	)

	return stats, nil
}

func isColumnDataFile(name string) bool {
	for _, ext := range colkind.KindToExt {
		if strings.HasSuffix(name, "."+ext) {
			return true
		}
	}
	return false
}

func findColumnFile(partitionPath, column string) (string, colkind.ColumnKind, error) {
	for kind, ext := range colkind.KindToExt {
		path := filepath.Join(partitionPath, column+"."+ext)
		if _, err := os.Stat(path); err == nil {
			return path, kind, nil
		}
	}
	return "", 0, fmt.Errorf("key column %q not found in partition %s", column, partitionPath)
}

func latestRowIDsByKey(ctx context.Context, path string, kind colkind.ColumnKind) (map[int32]struct{}, int64, error) {
	_, span := tracer.Start(ctx, "columnstore.latest_row_ids_by_key")
	defer span.End()
	span.SetAttributes(attribute.String("columnstore.key_column", path))

	start := time.Now()
	file, err := os.Open(path)
	span.SetAttributes(attribute.Int64("columnstore.key_column.open_ms", time.Since(start).Milliseconds()))
	if err != nil {
		err := fmt.Errorf("open key column: %w", err)
		span.RecordError(err)
		return nil, 0, err
	}
	defer file.Close()

	st, err := file.Stat()
	if err != nil {
		err := fmt.Errorf("stat key column: %w", err)
		span.RecordError(err)
		return nil, 0, err
	}
	bytesRead := st.Size()
	span.SetAttributes(attribute.Int64("columnstore.compact.bytes_read", bytesRead))

	// latestByKey := map[string]int32{}
	reader := bufio.NewReaderSize(file, scan.ScannerBufferSize)

	switch kind {
	case colkind.KindInt64:
		r := format.NewInt64TupleReader(reader)
		keep, err := collectLatestRowIDs(r.Next)
		if err != nil {
			err := fmt.Errorf("scan key column: %w", err)
			span.RecordError(err)
			return nil, 0, err
		}
		return keep, bytesRead, nil

	case colkind.KindFloat64:
		r := format.NewFloat64TupleReader(reader)
		keep, err := collectLatestRowIDs(func() (int32, uint64, error) {
			rowID, value, err := r.Next()
			if err != nil {
				return 0, 0, err
			}
			return rowID, math.Float64bits(value), nil
		})
		if err != nil {
			err := fmt.Errorf("scan key column: %w", err)
			span.RecordError(err)
			return nil, 0, err
		}
		return keep, bytesRead, nil

	case colkind.KindBool:
		r := format.NewBoolTupleReader(reader)
		keep, err := collectLatestRowIDs(r.Next)
		if err != nil {
			err := fmt.Errorf("scan key column: %w", err)
			span.RecordError(err)
			return nil, 0, err
		}
		return keep, bytesRead, nil

	case colkind.KindString:
		r := format.NewStringTupleReader(reader)
		keep, err := collectLatestRowIDs(r.Next)
		if err != nil {
			err := fmt.Errorf("scan key column: %w", err)
			span.RecordError(err)
			return nil, 0, err
		}
		return keep, bytesRead, nil

	default:
		err := fmt.Errorf("unsupported key column kind %d", kind)
		span.RecordError(err)
		return nil, 0, err
	}

}

func collectLatestRowIDs[K comparable](next func() (int32, K, error)) (map[int32]struct{}, error) {
	latestByKey := make(map[K]int32)
	for {
		rowID, key, err := next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, err
		}
		latestByKey[key] = rowID
	}

	keep := make(map[int32]struct{}, len(latestByKey))
	for _, rowID := range latestByKey {
		keep[rowID] = struct{}{}
	}
	return keep, nil
}
