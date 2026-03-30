# Columnstore

Columnstore is a small Go columnstore inspired by Honeycomb

It is not a general-purpose database or SQL engine. The package exposes a typed
API for:

- ingesting rows into dataset/partition directories
- querying grouped aggregates over a time range
- compacting a partition or dataset by keeping the latest row for a key

## Package layout

- `columnstore` contains the public API used by callers
- `columnstore/internal/...` contains storage, scan, filter, time, hashing,
  HLL, and t-digest helpers

Notable internal packages currently include:

- `internal/format` - tuple encoding, rewrite, durability flush helpers
- `internal/scan` - forward-only typed scanners
- `internal/filter` - typed filter AST and normalization
- `internal/timecalc` - time bucket and timezone helpers
- `internal/colkind` - column kind inference and resolution
- `internal/metricconv` - numeric coercion helpers
- `internal/hll` - approximate distinct counting once exact sets get large
- `internal/tdigest` - percentile aggregation support

## Public API

The main entry point is `Store`:

```go
store := columnstore.NewStore(basePath)

_, _, err := store.IngestRow("entities", ts, map[string]any{
    "entity_ref":  "resident/1",
    "entity_type": "Resident",
    "value":       int64(42),
})

res, err := store.Query(ctx, columnstore.Query{
    Start:      start,
    End:        end,
    Dataset:    "entities",
    Dimensions: []string{"date", "entity_type"},
    Granularity: "day",
    Timezone:    "UTC",
    Metrics: []columnstore.Metric{
        columnstore.Count(),
        columnstore.Sum("value"),
    },
    Filter: columnstore.And(
        columnstore.Exists("entity_type"),
        columnstore.In("entity_type", "Resident", "Prospect"),
    ),
})
```

`Store` also exposes:

- `IngestBatch`
- `CompactPartition`
- `CompactDataset`

## Storage model

### Datasets and partitions

- datasets live under `<base>/<dataset>/`
- default partitioning is monthly (`2006-01`)
- partitioning is configurable with `WithPartitioning(...)`
- built-in helpers support monthly and yearly partitioning via
  `PartitionerFromFormat(...)`
- Partitions are the only form of predicate pushdown implemented today

### Row identity

- each partition has monotonic `int32` row IDs
- row IDs are allocated independently per `dataset|partition`
- column files store sparse `(rowid, value)` tuples in append order
- this means each partition can only have a maximum of `2^31 - 1` rows

### Column types

Supported on-disk column types are:

- `<column>.int64`
- `<column>.float64`
- `<column>.bool`
- `<column>.str`

Every ingested row always writes `timestamp.int64` using Unix seconds.

Within a partition, a column must keep one consistent type. If a later write
would change the type for a column, ingestion fails.

### Tuple encoding

The on-disk format is simple append-only tuples.

- `int64` and `float64`: 4-byte rowid + 8-byte value
- `bool`: 4-byte rowid + 4-byte boolean payload
- `string`: 4-byte rowid + 4-byte length + bytes

String values are truncated to `MaxStringBytes` (currently 1 MiB) on write.

## Ingestion behavior

Ingestion is partition-parallel and single-writer per partition.

- `IngestBatch` groups rows by partition and processes touched partitions in
  parallel
- each partition mutation is guarded by a partition mutex
- touched files are fsynced and then the partition directory is fsynced
- existing partitions recover the next row ID by reading the tail of
  `timestamp.int64`

Accepted input value shapes:

- `int64`
- `float64`
- `bool`
- `string`

Any other non-nil value is JSON-marshaled and stored as a string.

## Query model

Queries are time-range scans over partitions plus grouped metric aggregation.

Execution flow is:

1. resolve dataset
2. list partition directories
3. prune partitions with the configured partitioner
4. scan partitions in parallel
5. drive the row loop from `timestamp.int64`
6. evaluate filters with forward-only scanners
7. materialize dimensions and aggregate metrics
8. merge partition-local accumulators

The engine does not maintain indexes, stats files, or a query cache.

### Dimensions

Dimensions may be regular stored columns or computed time dimensions.

Computed dimensions currently supported:

- `date`
- `day_of_week`

`date` respects `Granularity` and `Timezone`.

Supported granularities in time bucketing are:

- `minute`
- `hour`
- `day` (default)
- `week`
- `month`

Missing dimension values are returned as `null` in result rows.

### Filters

Supported filter builders are:

- `Eq(column, value)`
- `Exists(column)`
- `NotExists(column)`
- `In(column, values...)`
- `NotIn(column, values...)`
- `Lt/Lte/Gt/Gte` for `int64`, `float64`, and `string`
- `And(...)`
- `Or(...)`

Filters are normalized against the actual partition column types before scan.
If a filter cannot match a partition because the column is missing or the value
cannot be coerced to the stored type, that partition is skipped or the filter is
reduced accordingly.

### Metrics

Supported metrics are:

- `Count()`
- `CountDistinct(column)`
- `Sum(column)`
- `Avg(column)`
- `Min(column)`
- `Max(column)`
- `P50(column)`
- `P75(column)`
- `P90(column)`
- `P95(column)`
- `P99(column)`
- `P999(column)`

Metric result types:

- `count` and `count(field)` return `int64`
- `avg(...)` and percentiles return `float64`
- `sum/min/max` stay integer when the source data stays integer, and widen to
  float when needed

`CountDistinct` is exact for small cardinalities and switches to HyperLogLog
after an internal threshold.

Percentiles are estimated with an internal t-digest histogram.

### Result rows and ordering

Results are returned as `[]QueryResultRow`.

- dimensions are materialized under their requested names
- metric fields use canonical names like `count`, `sum(value)`, `p95(value)`
- rows are deterministic even without explicit ordering
- `OrderBy` is supported on dimension and metric fields

## Compaction

Compaction rewrites a partition so only the latest row for each key value
remains.

- `CompactPartition(ctx, dataset, partition, keyColumn)` compacts one partition
- `CompactDataset(ctx, dataset, keyColumn, includeActive)` compacts all dataset
  partitions, optionally skipping the latest partition

Compaction works by:

1. scanning the key column
2. remembering the latest row ID for each key
3. rewriting every column file, keeping only those row IDs

This is useful for append-only upsert-like ingestion patterns, but it is not an
in-place update system.

## Observability

The package emits OpenTelemetry spans for query and compaction work, including:

- `columnstore.query`
- `analytics.query.partition.scan`
- `columnstore.dataset.compact`
- `columnstore.partition.compact`

Recorded attributes include query shape, partition counts, row counts, and
stage timings when the span is recording.

## Current boundaries

This package currently does not provide:

- SQL parsing or a planner
- secondary indexes
- stats-based pruning
- a query result cache
- in-place updates
- built-in CLI tools in this module

If you need to understand behavior, the tests are the best up-to-date executable documentation.
