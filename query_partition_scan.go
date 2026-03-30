package columnstore

import (
	"context"
	"math"
	"os"
	"path/filepath"
	"time"

	"github.com/nordfjord/columnstore/internal/colkind"
	"github.com/nordfjord/columnstore/internal/filter"
	"github.com/nordfjord/columnstore/internal/scan"
	"github.com/nordfjord/columnstore/internal/timecalc"
	"go.opentelemetry.io/otel/attribute"
)

const partitionScanCancelCheckInterval = 1024

func scanPartition(
	ctx context.Context,
	partition string,
	partitionPath string,
	q Query,
	preparedFilter filter.Filter,
	metrics []Metric,
	collectPerfStats bool,
) (*MetricAccumulator, *partitionPerfStats, error) {
	_, span := tracer.Start(ctx, "analytics.query.partition.scan")
	defer span.End()
	span.SetAttributes(
		attribute.String("app.query.partition", partition),
		attribute.String("app.query.partition_path", partitionPath),
	)

	var perf *partitionPerfStats
	if collectPerfStats {
		perf = &partitionPerfStats{}
	}

	stageStart := time.Now()
	columns, err := listPartitionColumns(partitionPath)
	if perf != nil {
		perf.listColumnsNs = time.Since(stageStart).Nanoseconds()
	}
	span.SetAttributes(attribute.Int("app.query.columns_count", len(columns)))
	if err != nil {
		if os.IsNotExist(err) {
			span.SetAttributes(attribute.String("app.query.partition_outcome", "empty"))
			return nil, perf, nil
		}
		span.RecordError(err)
		return nil, perf, err
	}

	agg, err := NewMetricAccumulator(metrics)
	if err != nil {
		span.RecordError(err)
		return nil, perf, err
	}

	stageStart = time.Now()
	timestamp, closeTS, err := scan.OpenInt64Scanner(filepath.Join(partitionPath, "timestamp.int64"))
	if perf != nil {
		perf.openTimestampNs = time.Since(stageStart).Nanoseconds()
	}
	if err != nil {
		if os.IsNotExist(err) {
			span.SetAttributes(attribute.String("app.query.partition_outcome", "empty"))
			return nil, perf, nil
		}
		span.RecordError(err)
		return nil, perf, err
	}
	defer closeTS.Close()

	columnScanners := make(map[string]*openedColumnScanner)
	getColumnScanner := func(column string) (*openedColumnScanner, bool, error) {
		if column == "timestamp" {
			return &openedColumnScanner{scanner: timestamp, kind: colkind.KindInt64}, true, nil
		}
		if entry, exists := columnScanners[column]; exists {
			return entry, true, nil
		}
		scanner, kind, closer, ok, err := openMetricScannerIfExists(partitionPath, column, columns)
		if err != nil {
			return nil, false, err
		}
		if !ok {
			return nil, false, nil
		}
		entry := &openedColumnScanner{scanner: scanner, kind: kind, closer: closer}
		columnScanners[column] = entry
		return entry, true, nil
	}
	dynamicScanners := make(map[string]*openedColumnScanner)
	stageStart = time.Now()
	for _, dim := range q.Dimensions {
		switch dim {
		case "date", "day_of_week":
			continue
		}
		entry, exists, err := getColumnScanner(dim)
		if err != nil {
			return nil, perf, err
		}
		if exists {
			dynamicScanners[dim] = entry
		}
	}
	if perf != nil {
		perf.openDimsNs = time.Since(stageStart).Nanoseconds()
	}
	defer func() {
		for _, s := range columnScanners {
			if s.closer != nil {
				_ = s.closer.Close()
			}
		}
	}()

	type metricScannerWithCloser struct {
		index   int
		kind    colkind.ColumnKind
		scanner metricValueScanner
	}
	metricScanners := make([]metricScannerWithCloser, 0)
	metricColumns := metricSourceColumns(metrics)
	stageStart = time.Now()
	for idx, column := range metricColumns {
		entry, ok, err := getColumnScanner(column)
		if err != nil {
			return nil, perf, err
		}
		if !ok {
			continue
		}
		metricScanners = append(metricScanners, metricScannerWithCloser{index: idx, kind: entry.kind, scanner: entry.scanner})
	}
	if perf != nil {
		perf.openMetricsNs = time.Since(stageStart).Nanoseconds()
	}

	var filterEvaluator *partitionRowFilter
	if preparedFilter != nil {
		evaluator, ok, err := newPartitionRowFilter(columns, preparedFilter, getColumnScanner)
		if err != nil {
			return nil, perf, err
		}
		if !ok {
			return agg, perf, nil
		}
		filterEvaluator = evaluator
	}

	zone := timecalc.ResolveTimeZone(q.Timezone)
	countOnly := isCountOnlyMetric(metrics)
	dims := make([]dimAtom, len(q.Dimensions))
	var metricValueVec []any
	var metricIntValues []int64
	var metricFloatValues []float64
	var metricKinds []colkind.ColumnKind
	var metricPresent []bool
	var specValueIndex []int
	if !countOnly {
		metricValueVec = make([]any, len(metricColumns))
		metricIntValues = make([]int64, len(metricColumns))
		metricFloatValues = make([]float64, len(metricColumns))
		metricKinds = make([]colkind.ColumnKind, len(metricColumns))
		metricPresent = make([]bool, len(metricColumns))
		specValueIndex = make([]int, len(metrics))
		for i, metric := range metrics {
			column := metric.Field()
			if column == "" {
				specValueIndex[i] = -1
				continue
			}
			if idx := indexOfString(metricColumns, column); idx >= 0 {
				specValueIndex[i] = idx
			} else {
				specValueIndex[i] = -1
			}
		}
	}

	rowLoopStart := time.Now()
	rowsScanned := 0
	for {
		rowsScanned++
		if rowsScanned%partitionScanCancelCheckInterval == 0 {
			select {
			case <-ctx.Done():
				return nil, perf, ctx.Err()
			default:
			}
		}

		ok, err := timestamp.Next()
		if err != nil {
			return nil, perf, err
		}
		if !ok {
			break
		}
		if perf != nil {
			perf.rowsTotal++
		}

		ts := timestamp.Value()
		if ts < q.Start.Unix() || ts >= q.End.Unix() {
			continue
		}
		if perf != nil {
			perf.rowsVisited++
		}

		if filterEvaluator != nil {
			passes, err := filterEvaluator.Match(timestamp.RowID())
			if err != nil {
				return nil, perf, err
			}
			if !passes {
				continue
			}
		}
		if perf != nil {
			perf.rowsAfterFilter++
		}

		for i, dim := range q.Dimensions {
			switch dim {
			case "date":
				dims[i] = dimAtom{kind: dimKindTime, bits: uint64(timecalc.ComputeTimeBucketInLocation(ts, zone, q.Granularity))}
			case "day_of_week":
				day := timecalc.ComputeDayOfWeekInLocation(ts, zone)
				id, ok := agg.stringIDs[day]
				if !ok {
					id = agg.nextString
					agg.nextString++
					agg.stringIDs[day] = id
					agg.stringByID[id] = day
				}
				dims[i] = dimAtom{kind: dimKindString, bits: id}
			default:
				s, exists := dynamicScanners[dim]
				if !exists {
					dims[i] = dimAtom{kind: dimKindNil}
					continue
				}
				matched, err := s.scanner.AdvanceTo(timestamp.RowID())
				if err != nil {
					return nil, perf, err
				}
				if matched {
					switch s.kind {
					case colkind.KindString:
						dims[i] = dimAtom{kind: dimKindString, bits: s.scanner.(*scan.StringScanner).ValueID()}
					case colkind.KindInt64:
						dims[i] = dimAtom{kind: dimKindInt64, bits: uint64(s.scanner.(*scan.Int64Scanner).Value())}
					case colkind.KindFloat64:
						dims[i] = dimAtom{kind: dimKindFloat64, bits: math.Float64bits(s.scanner.(*scan.Float64Scanner).Value())}
					case colkind.KindBool:
						bits := uint64(0)
						if s.scanner.(*scan.BoolScanner).Value() {
							bits = 1
						}
						dims[i] = dimAtom{kind: dimKindBool, bits: bits}
					default:
						dims[i] = internOneDimension(agg, s.scanner.ValueAny())
					}
				} else {
					dims[i] = dimAtom{kind: dimKindNil}
				}
			}
		}

		if countOnly {
			group := agg.getOrCreateGroup(dims)
			if group != nil {
				for i := range agg.specs {
					if agg.specs[i].op == MetricOpCount {
						group.metrics[i].count++
					}
				}
			}
			continue
		}

		for i := range metricPresent {
			metricPresent[i] = false
		}
		for _, scanner := range metricScanners {
			matched, err := scanner.scanner.AdvanceTo(timestamp.RowID())
			if err != nil {
				return nil, perf, err
			}
			if !matched {
				continue
			}
			idx := scanner.index
			metricPresent[idx] = true
			metricKinds[idx] = scanner.kind
			switch scanner.kind {
			case colkind.KindInt64:
				metricIntValues[idx] = scanner.scanner.(*scan.Int64Scanner).Value()
			case colkind.KindFloat64:
				metricFloatValues[idx] = scanner.scanner.(*scan.Float64Scanner).Value()
			case colkind.KindString:
				metricValueVec[idx] = scanner.scanner.(*scan.StringScanner).Value()
			default:
				metricValueVec[idx] = scanner.scanner.ValueAny()
			}
		}
		agg.AddResolvedTyped(dims, metricKinds, metricIntValues, metricFloatValues, metricValueVec, metricPresent, specValueIndex)
	}
	for _, s := range dynamicScanners {
		if s.kind != colkind.KindString {
			continue
		}
		agg.RegisterInternedStrings(s.scanner.(*scan.StringScanner).InternedValues())
	}
	if perf != nil {
		perf.rowLoopNs = time.Since(rowLoopStart).Nanoseconds()
		span.SetAttributes(attribute.Int("app.query.partition_row_count", int(perf.rowsTotal)))
		span.SetAttributes(
			attribute.Int("app.query.rows_visited", int(perf.rowsVisited)),
			attribute.Int("app.query.rows_matched", int(perf.rowsAfterFilter)),
		)
		if perf.rowsTotal == 0 {
			span.SetAttributes(attribute.String("app.query.partition_outcome", "empty"))
		} else if perf.rowsVisited == 0 {
			span.SetAttributes(attribute.String("app.query.partition_outcome", "out_of_range"))
		} else {
			span.SetAttributes(attribute.String("app.query.partition_outcome", "scanned"))
		}
	}

	return agg, perf, nil
}

func listPartitionColumns(partitionPath string) (map[string]bool, error) {
	entries, err := os.ReadDir(partitionPath)
	if err != nil {
		return nil, err
	}
	columns := make(map[string]bool, len(entries))
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		columns[e.Name()] = true
	}
	return columns, nil
}
