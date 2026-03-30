package columnstore

import (
	"context"
	"fmt"
	"path/filepath"
	"runtime"
	"sync"
	"time"

	"github.com/nordfjord/columnstore/internal/filter"
	"github.com/nordfjord/columnstore/internal/timecalc"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

var tracer trace.Tracer

func init() {
	tracer = otel.Tracer("columnstore")
}

type queryPerfStats struct {
	prepMetricsNs     int64
	prepFilterNs      int64
	resolveDatasetNs  int64
	listPartitionsNs  int64
	prunePartitionsNs int64
	initGlobalAggNs   int64
	workerWallNs      int64
	mergeNs           int64
	rowsMaterializeNs int64
}

type partitionPerfStats struct {
	listColumnsNs   int64
	openTimestampNs int64
	openDimsNs      int64
	openMetricsNs   int64
	rowLoopNs       int64
	rowsTotal       int64
	rowsVisited     int64
	rowsAfterFilter int64
}

func (s *partitionPerfStats) add(other *partitionPerfStats) {
	if s == nil || other == nil {
		return
	}
	s.listColumnsNs += other.listColumnsNs
	s.openTimestampNs += other.openTimestampNs
	s.openDimsNs += other.openDimsNs
	s.openMetricsNs += other.openMetricsNs
	s.rowLoopNs += other.rowLoopNs
	s.rowsTotal += other.rowsTotal
	s.rowsVisited += other.rowsVisited
	s.rowsAfterFilter += other.rowsAfterFilter
}

func (s *Store) Query(ctx context.Context, q Query) (*QueryResult, error) {
	ctx, span := tracer.Start(ctx, "columnstore.query")
	defer span.End()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	queryStart := time.Now()
	collectPerfStats := span.IsRecording()
	perf := &queryPerfStats{}
	partitionsTotal := 0
	partitionsSkippedTime := 0
	partitionsScanned := 0
	totalRowsVisited := 0
	totalRowsMatched := 0
	resultRows := 0
	defer func() {
		if !span.IsRecording() {
			return
		}
		span.SetAttributes(
			attribute.Int("app.query.partitions_total", partitionsTotal),
			attribute.Int("app.query.partitions_skipped_time", partitionsSkippedTime),
			attribute.Int("app.query.partitions_scanned", partitionsScanned),
			attribute.Int("app.query.total_rows_visited", totalRowsVisited),
			attribute.Int("app.query.total_rows_matched", totalRowsMatched),
			attribute.Int("app.query.result_rows", resultRows),
		)
	}()

	span.SetAttributes(
		attribute.String("app.query.dataset", q.Dataset),
		attribute.StringSlice("app.query.dimensions", q.Dimensions),
		attribute.String("app.query.granularity", q.Granularity),
		attribute.String("app.query.timezone", q.Timezone),
		attribute.String("app.query.start", q.Start.UTC().Format(time.RFC3339)),
		attribute.String("app.query.end", q.End.UTC().Format(time.RFC3339)),
	)

	stageStart := time.Now()
	metrics, err := normalizeMetricsInput(q)
	if err != nil {
		return nil, err
	}
	metricNames := make([]string, 0, len(metrics))
	for _, metric := range metrics {
		if metric == nil {
			return nil, fmt.Errorf("metric is required")
		}
		if err := metric.Validate(); err != nil {
			return nil, err
		}
		metricNames = append(metricNames, metric.Name())
	}
	span.SetAttributes(attribute.StringSlice("app.query.metrics", metricNames))
	if collectPerfStats {
		perf.prepMetricsNs = time.Since(stageStart).Nanoseconds()
	}

	if q.Filter != nil {
		span.SetAttributes(attribute.String("app.query.filter", q.Filter.String()))
	}

	if q.End.Before(q.Start) || q.End.Equal(q.Start) {
		return nil, fmt.Errorf("invalid time range")
	}

	stageStart = time.Now()
	var preparedFilter filter.Filter
	if q.Filter != nil {
		if err := q.Filter.Validate(); err != nil {
			return nil, err
		}
		preparedFilter = q.Filter
	}
	if collectPerfStats {
		perf.prepFilterNs = time.Since(stageStart).Nanoseconds()
	}

	stageStart = time.Now()
	dataset, err := resolveDataset(q.Dataset)
	if err != nil {
		return nil, err
	}
	if collectPerfStats {
		perf.resolveDatasetNs = time.Since(stageStart).Nanoseconds()
	}

	stageStart = time.Now()
	partitions, err := listDatasetPartitions(filepath.Join(s.basePath, dataset))
	if err != nil {
		return nil, err
	}
	partitionsTotal = len(partitions)
	if len(partitions) == 0 {
		return &QueryResult{Rows: nil}, nil
	}
	if collectPerfStats {
		perf.listPartitionsNs = time.Since(stageStart).Nanoseconds()
	}

	// Cheap coarse pruning by partition name without touching _stats.json.
	stageStart = time.Now()
	toScan := pruneByPartitionBounds(partitions, q.Start, q.End, s.partitioner)
	partitionsSkippedTime = len(partitions) - len(toScan)
	partitionsScanned = len(toScan)
	if len(toScan) == 0 {
		return &QueryResult{Rows: nil}, nil
	}
	if collectPerfStats {
		perf.prunePartitionsNs = time.Since(stageStart).Nanoseconds()
	}

	stageStart = time.Now()
	global, err := NewMetricAccumulator(metrics)
	if err != nil {
		return nil, err
	}
	if collectPerfStats {
		perf.initGlobalAggNs = time.Since(stageStart).Nanoseconds()
	}

	maxWorkers := min(runtime.NumCPU(), len(toScan))
	sem := make(chan struct{}, maxWorkers)
	type partResult struct {
		agg  *MetricAccumulator
		perf *partitionPerfStats
		err  error
	}
	results := make([]partResult, len(toScan))

	parentCtx := ctx
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup
	for idx, partition := range toScan {
		wg.Add(1)
		sem <- struct{}{}
		go func(i int, p string) {
			defer wg.Done()
			defer func() { <-sem }()

			select {
			case <-ctx.Done():
				results[i].err = ctx.Err()
				return
			default:
			}

			agg, partPerf, err := scanPartition(
				ctx,
				p,
				filepath.Join(s.basePath, dataset, p),
				q,
				preparedFilter,
				metrics,
				collectPerfStats,
			)
			if err != nil {
				results[i].err = err
				cancel()
				return
			}
			results[i].agg = agg
			results[i].perf = partPerf
		}(idx, partition)
	}
	waitStart := time.Now()
	wg.Wait()
	if collectPerfStats {
		perf.workerWallNs = time.Since(waitStart).Nanoseconds()
	}
	if err := parentCtx.Err(); err != nil {
		return nil, err
	}

	for _, r := range results {
		if r.err != nil && r.err != context.Canceled {
			return nil, r.err
		}
	}

	mergeStart := time.Now()
	for _, r := range results {
		if r.agg == nil {
			continue
		}
		if err := global.Merge(r.agg); err != nil {
			return nil, err
		}
	}
	if collectPerfStats {
		perf.mergeNs = time.Since(mergeStart).Nanoseconds()
	}

	rowsStart := time.Now()
	rows := global.RowsNamedInLocation(q.Dimensions, timecalc.ResolveTimeZone(q.Timezone))
	if len(q.OrderBy) > 0 {
		if err := sortQueryRows(rows, q.Dimensions, metricNames, q.OrderBy); err != nil {
			return nil, err
		}
	}
	if collectPerfStats {
		perf.rowsMaterializeNs = time.Since(rowsStart).Nanoseconds()
	}
	resultRows = len(rows)
	for _, r := range results {
		if r.perf == nil {
			continue
		}
		totalRowsVisited += int(r.perf.rowsVisited)
		totalRowsMatched += int(r.perf.rowsAfterFilter)
	}
	queryNanos := time.Since(queryStart).Nanoseconds()
	if collectPerfStats {
		partPerfTotal := &partitionPerfStats{}
		for i, r := range results {
			partPerfTotal.add(r.perf)
			_ = i
		}

		preOpenNs := perf.prepMetricsNs + perf.prepFilterNs + perf.resolveDatasetNs +
			perf.listPartitionsNs + perf.prunePartitionsNs + perf.initGlobalAggNs
		span.SetAttributes(
			attribute.Float64("app.query.query_total_ms", float64(queryNanos)/1e6),
			attribute.Float64("app.query.pre_open_ms", float64(preOpenNs)/1e6),
			attribute.Float64("app.query.workers_wall_ms", float64(perf.workerWallNs)/1e6),
			attribute.Float64("app.query.merge_ms", float64(perf.mergeNs)/1e6),
			attribute.Float64("app.query.rows_materialize_ms", float64(perf.rowsMaterializeNs)/1e6),
			attribute.Float64("app.query.partitions_list_columns_ms", float64(partPerfTotal.listColumnsNs)/1e6),
			attribute.Float64("app.query.partitions_open_timestamp_ms", float64(partPerfTotal.openTimestampNs)/1e6),
			attribute.Float64("app.query.partitions_open_dims_ms", float64(partPerfTotal.openDimsNs)/1e6),
			attribute.Float64("app.query.partitions_open_metrics_ms", float64(partPerfTotal.openMetricsNs)/1e6),
			attribute.Float64("app.query.partitions_row_loop_ms", float64(partPerfTotal.rowLoopNs)/1e6),
		)
	}

	return &QueryResult{Rows: rows}, nil
}
