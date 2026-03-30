package columnstore

import (
	"bufio"
	"context"
	"fmt"
	"os"

	"github.com/nordfjord/columnstore/internal/colkind"
	format "github.com/nordfjord/columnstore/internal/format"
	"github.com/nordfjord/columnstore/internal/scan"
	"go.opentelemetry.io/otel/attribute"
)

func compactColumnFile(ctx context.Context, path string, keepRowIDs map[int32]struct{}) (fileCompactionStats, error) {
	_, span := tracer.Start(ctx, "columnstore.compact_column_file")
	defer span.End()
	stats := fileCompactionStats{}
	span.SetAttributes(
		attribute.String("columnstore.key_column", path),
		attribute.Int("columnstore.key_column.rows", len(keepRowIDs)),
	)

	in, err := os.Open(path)
	if err != nil {
		err := fmt.Errorf("open column file %s: %w", path, err)
		span.RecordError(err)
		return stats, err
	}
	defer in.Close()
	inStat, err := in.Stat()
	if err != nil {
		err := fmt.Errorf("stat column file %s: %w", path, err)
		span.RecordError(err)
		return stats, err
	}
	stats.BytesRead = inStat.Size()

	kind, err := colkind.ResolveColumnKindFromPath(path)
	if err != nil {
		err := fmt.Errorf("resolve column kind %s: %w", path, err)
		span.RecordError(err)
		return stats, err
	}

	tmpPath := path + ".compact.tmp"
	out, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0644)
	if err != nil {
		err := fmt.Errorf("open compact temp file %s: %w", tmpPath, err)
		span.RecordError(err)
		return stats, err
	}
	cleanupTmp := true
	defer func() {
		if cleanupTmp {
			_ = os.Remove(tmpPath)
		}
	}()

	r := bufio.NewReaderSize(in, scan.ScannerBufferSize)
	if err := format.RewriteColumnTuples(out, r, kind, keepRowIDs); err != nil {
		_ = out.Close()
		err := fmt.Errorf("compact column file %s: %w", path, err)
		span.RecordError(err)
		return stats, err
	}

	if err := out.Sync(); err != nil {
		_ = out.Close()
		err := fmt.Errorf("fsync compact temp file %s: %w", tmpPath, err)
		span.RecordError(err)
		return stats, err
	}
	if err := out.Close(); err != nil {
		err := fmt.Errorf("close compact temp file %s: %w", tmpPath, err)
		span.RecordError(err)
		return stats, err
	}
	outStat, err := os.Stat(tmpPath)
	if err != nil {
		err := fmt.Errorf("stat compact temp file %s: %w", tmpPath, err)
		span.RecordError(err)
		return stats, err
	}
	stats.BytesWritten = outStat.Size()

	if err := os.Rename(tmpPath, path); err != nil {
		err := fmt.Errorf("rename compact temp file %s -> %s: %w", tmpPath, path, err)
		span.RecordError(err)
		return stats, err
	}
	cleanupTmp = false
	span.SetAttributes(
		attribute.Int64("columnstore.compact.bytes_read", stats.BytesRead),
		attribute.Int64("columnstore.compact.bytes_written", stats.BytesWritten),
	)

	return stats, nil
}
