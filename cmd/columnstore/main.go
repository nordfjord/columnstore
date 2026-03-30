// columnstore is a CLI tool for the custom analytics column store.
//
// Commands:
//
//	ingest - Read entities from append-only log and ingest them into the store
//	query  - Execute analytics queries against the store
//
// Usage:
//
//	go run cmd/columnstore/main.go ingest -log=/data/log -store=/data/analytics
//	go run cmd/columnstore/main.go query -store=/data/analytics \
//	  -dimensions=date,entity_type -metrics=count -start=2025-01-01 -end=2025-12-31
//
// The tool provides both log ingestion and querying capabilities for the
// custom per-column store.
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/nordfjord/columnstore"
	"github.com/olekukonko/tablewriter"
)

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	command := os.Args[1]

	switch command {
	case "ingest":
		ingestCommand(os.Args[2:])
	case "query":
		queryCommand(os.Args[2:])
	case "help", "-h", "--help":
		printUsage()
	default:
		fmt.Fprintf(os.Stderr, "Unknown command: %s\n\n", command)
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Println(`Usage: columnstore <command> [options]

Commands:
  ingest    Read entities from log and ingest into store
  query     Execute analytics query against the store

Examples:
  columnstore query -store=.data/columnstore -dimensions=date,entity_type -metrics=count

For command-specific help:
  columnstore query -h`)
}

func defaultStoreDir() string {
	return ".data"
}

// =============================================================================
// QUERY COMMAND
// =============================================================================

type queryFlags struct {
	store           string
	partitionFormat string
	dataset         string
	dimensions      string
	metrics         string
	orderBy         string
	start           string
	end             string
	granularity     string
	timezone        string
	where           string
	output          string
	verbose         bool
}

type ingestFlags struct {
	store           string
	file            string
	partitionFormat string
	dataset         string
	batchSize       int
	verbose         bool
}

type metricBinding struct {
	output string
	name   string
	metric columnstore.Metric
}

func queryCommand(args []string) {
	fs := flag.NewFlagSet("query", flag.ExitOnError)

	var flags queryFlags
	fs.StringVar(&flags.store, "store", "", "Store directory (required)")
	fs.StringVar(&flags.partitionFormat, "partition-format", "monthly", "Partition format: monthly, yearly, or Go time format")
	fs.StringVar(&flags.dataset, "dataset", "entities", "Dataset name (default: entities)")
	fs.StringVar(&flags.dimensions, "dimensions", "", "Comma-separated dimensions (optional)")
	fs.StringVar(&flags.metrics, "metrics", "count", "Comma-separated metrics (default: count)")
	fs.StringVar(&flags.orderBy, "order-by", "", "Comma-separated order terms: <field>[:asc|desc]")
	fs.StringVar(&flags.start, "start", "", "Start time (RFC3339 or YYYY-MM-DD)")
	fs.StringVar(&flags.end, "end", "", "End time (RFC3339 or YYYY-MM-DD)")
	fs.StringVar(&flags.granularity, "granularity", "day", "Time granularity: minute, hour, day, week, month (default: day)")
	fs.StringVar(&flags.timezone, "timezone", "UTC", "Timezone (default: UTC)")
	fs.StringVar(&flags.where, "where", "", "Filter s-expression (optional)")
	fs.StringVar(&flags.output, "output", "table", "Output format: table, json, none")
	fs.BoolVar(&flags.verbose, "v", false, "Verbose logging")

	fs.Usage = func() {
		fmt.Println(`Usage: columnstore query [options]

Options:`)
		fs.PrintDefaults()
		fmt.Println(`
Examples:
  # Total count (no dimensions)
  columnstore query -store=.data/columnstore -metrics=count

  # Sum a metric field
  columnstore query -store=.data/columnstore -metrics='sum(random_value)'

  # Count entities by day
  columnstore query -store=.data/columnstore -dimensions=date -metrics=count

  # Daily breakdown for a filtered entity type
  columnstore query -store=.data/columnstore \
    -dimensions=date,entity_type -metrics=count \
    -where='(eq entity_type Order)' -granularity=day

  # Sort by descending count then ascending date
  columnstore query -store=.data/columnstore \
    -dimensions=date,entity_type -metrics=count \
    -order-by=count:desc,date:asc

Metric formats:
  count                                - Row count
  sum(field), avg(field), max(field), min(field)
  p50(field), p75(field), p90(field), p95(field), p99(field), p999(field)
  count(field) or count_distinct(field)`)
	}

	if err := fs.Parse(args); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	// Validate required flags
	if flags.store == "" {
		fmt.Fprintf(os.Stderr, "Error: -store flag is required\n\n")
		fs.Usage()
		os.Exit(1)
	}

	// Setup logging
	logLevel := slog.LevelWarn
	if flags.verbose {
		logLevel = slog.LevelDebug
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: logLevel}))
	slog.SetDefault(logger)

	// Parse dimensions (optional)
	dimensions := parseList(flags.dimensions)

	// Parse metrics
	metricInputs := parseList(flags.metrics)
	if len(metricInputs) == 0 {
		fmt.Fprintf(os.Stderr, "Error: at least one metric is required\n")
		os.Exit(1)
	}
	if flags.output != "table" && flags.output != "json" && flags.output != "none" {
		fmt.Fprintf(os.Stderr, "Error: -output must be one of: table, json, none\n")
		os.Exit(1)
	}

	dataset := strings.TrimSpace(flags.dataset)
	if dataset == "" {
		dataset = inferDataset(dimensions, metricInputs)
	}

	metricBindings, err := parseMetricsForDataset(metricInputs, dataset)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error parsing metrics: %v\n", err)
		os.Exit(1)
	}
	metrics := make([]columnstore.Metric, 0, len(metricBindings))
	for _, binding := range metricBindings {
		metrics = append(metrics, binding.metric)
	}

	// Parse time range
	start, end, err := parseTimeRange(flags.start, flags.end)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error parsing time range: %v\n", err)
		os.Exit(1)
	}

	normalizedDimensions := normalizeDimensionsForDataset(dimensions, dataset)
	orderBy, err := parseOrderByForDataset(flags.orderBy, dataset, metricBindings)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error parsing order-by: %v\n", err)
		os.Exit(1)
	}

	// Build query
	query := columnstore.Query{
		Start:       start,
		End:         end,
		Dimensions:  normalizedDimensions,
		Granularity: flags.granularity,
		Timezone:    flags.timezone,
		Dataset:     dataset,
		OrderBy:     orderBy,
		Metrics:     metrics,
	}

	if flags.where != "" {
		query.Filter, err = parseFilter(flags.where)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error parsing filter: %v\n", err)
			os.Exit(1)
		}
	}

	// Execute query
	startTime := time.Now()

	store := columnstore.NewStore(flags.store, columnstore.WithPartitioning(columnstore.PartitionerFromFormat(flags.partitionFormat)))
	result, err := store.Query(context.Background(), query)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error executing query: %v\n", err)
		os.Exit(1)
	}

	queryTime := time.Since(startTime)

	if flags.output == "none" {
		fmt.Fprintf(os.Stderr, "\nQuery Stats:\n")
		fmt.Fprintf(os.Stderr, "  Rows: %d\n", len(result.Rows))
		fmt.Fprintf(os.Stderr, "  Time: %d ms\n", queryTime.Milliseconds())
		return
	}

	data := make([]map[string]any, 0, len(result.Rows))
	for _, row := range result.Rows {
		obj := make(map[string]any, len(dimensions)+len(metricBindings))
		for _, dim := range dimensions {
			value, ok := row.Any(dim)
			if ok {
				obj[dim] = value
			}
		}
		for _, binding := range metricBindings {
			value, ok := row.Any(binding.name)
			if !ok {
				value = 0
			}
			obj[binding.output] = value
		}
		data = append(data, obj)
	}

	// Output results
	if len(data) == 0 {
		fmt.Println("No results found")
		fmt.Printf("\nQuery completed in %d ms\n", queryTime.Milliseconds())
		return
	}

	switch flags.output {
	case "table":
		// Render as ASCII table
		renderTable(data, dimensions, metricInputs)
	case "json":
		for _, row := range data {
			encoded, _ := json.Marshal(row)
			fmt.Println(string(encoded))
		}
	case "none":
		// Intentionally suppress row output and print stats only.
	}

	// Print stats
	fmt.Fprintf(os.Stderr, "\nQuery Stats:\n")
	fmt.Fprintf(os.Stderr, "  Rows: %d\n", len(data))
	fmt.Fprintf(os.Stderr, "  Time: %d ms\n", queryTime.Milliseconds())
}

func ingestCommand(args []string) {
	fs := flag.NewFlagSet("ingest", flag.ExitOnError)

	var flags ingestFlags
	fs.StringVar(&flags.store, "store", defaultStoreDir(), "Store directory (default: .data)")
	fs.StringVar(&flags.file, "file", "", "NDJSON input file (default: stdin)")
	fs.StringVar(&flags.partitionFormat, "partition-format", "monthly", "Partition format: monthly, yearly, or Go time format")
	fs.StringVar(&flags.dataset, "dataset", "entities", "Dataset name (default: entities)")
	fs.IntVar(&flags.batchSize, "batch-size", 1000, "Rows per ingest batch")
	fs.BoolVar(&flags.verbose, "v", false, "Verbose logging")

	fs.Usage = func() {
		fmt.Println(`Usage: columnstore ingest [options]

Options:`)
		fs.PrintDefaults()
		fmt.Println(`
Input format:
  Newline-delimited JSON objects. Each row must include a top-level timestamp field.
  Nested objects and arrays are flattened into dotted field paths before ingest.

Timestamp formats:
  RFC3339 string, YYYY-MM-DD string, or Unix seconds number

Examples:
  cat rows.ndjson | columnstore ingest
  columnstore ingest -store=.data/columnstore -file=rows.ndjson
  columnstore ingest -dataset=orders -batch-size=5000 < rows.ndjson`)
	}

	if err := fs.Parse(args); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
	if flags.batchSize <= 0 {
		fmt.Fprintf(os.Stderr, "Error: -batch-size must be greater than 0\n")
		os.Exit(1)
	}

	logLevel := slog.LevelWarn
	if flags.verbose {
		logLevel = slog.LevelDebug
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: logLevel}))
	slog.SetDefault(logger)

	input, err := openInput(flags.file, os.Stdin)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error opening input: %v\n", err)
		os.Exit(1)
	}
	defer input.Close()

	store := columnstore.NewStore(flags.store, columnstore.WithPartitioning(columnstore.PartitionerFromFormat(flags.partitionFormat)))
	stats, err := ingestNDJSON(input, store, flags.dataset, flags.batchSize)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error ingesting input: %v\n", err)
		os.Exit(1)
	}

	fmt.Fprintf(os.Stderr, "\nIngest Stats:\n")
	fmt.Fprintf(os.Stderr, "  Rows: %d\n", stats.Rows)
	fmt.Fprintf(os.Stderr, "  Batches: %d\n", stats.Batches)
	fmt.Fprintf(os.Stderr, "  Time: %d ms\n", stats.Duration.Milliseconds())
}

func normalizeDimensionsForDataset(dims []string, dataset string) []string {
	out := make([]string, len(dims))
	for i, d := range dims {
		out[i] = normalizeFieldForDataset(d, dataset)
	}
	return out
}

func normalizeFieldForDataset(field string, _ string) string { return field }

func inferDataset(_ []string, _ []string) string { return "entities" }

func parseMetricsForDataset(inputs []string, dataset string) ([]metricBinding, error) {
	bindings := make([]metricBinding, 0, len(inputs))
	for _, input := range inputs {
		metric, err := parseMetricForDataset(input, dataset)
		if err != nil {
			return nil, err
		}
		bindings = append(bindings, metricBinding{
			output: input,
			name:   metric.Name(),
			metric: metric,
		})
	}
	return bindings, nil
}

func parseMetricForDataset(input, dataset string) (columnstore.Metric, error) {
	metric := strings.TrimSpace(input)
	if metric == "" {
		return nil, fmt.Errorf("metric cannot be empty")
	}

	if metric == "count" || metric == "count()" {
		return columnstore.Count(), nil
	}

	open := strings.IndexByte(metric, '(')
	close := strings.LastIndexByte(metric, ')')
	if open > 0 && close == len(metric)-1 && open < close {
		op := strings.ToLower(strings.TrimSpace(metric[:open]))
		field := strings.TrimSpace(metric[open+1 : close])
		if op == "count" && field == "" {
			return columnstore.Count(), nil
		}
		if op != "count" && field == "" {
			return nil, fmt.Errorf("metric %q requires field", metric)
		}
		field = normalizeFieldForDataset(field, dataset)
		switch op {
		case "sum":
			return columnstore.Sum(field), nil
		case "avg":
			return columnstore.Avg(field), nil
		case "max":
			return columnstore.Max(field), nil
		case "min":
			return columnstore.Min(field), nil
		case "count_distinct", "distinct_count":
			return columnstore.CountDistinct(field), nil
		case "count":
			return columnstore.CountDistinct(field), nil
		case "p50":
			return columnstore.P50(field), nil
		case "p75":
			return columnstore.P75(field), nil
		case "p90":
			return columnstore.P90(field), nil
		case "p95":
			return columnstore.P95(field), nil
		case "p99":
			return columnstore.P99(field), nil
		case "p999":
			return columnstore.P999(field), nil
		default:
			return nil, fmt.Errorf("unsupported metric op %q in %q", op, metric)
		}
	}

	return nil, fmt.Errorf("unsupported metric format %q (use function style, e.g. sum(field), count(field), p95(field))", metric)
}

func parseOrderByForDataset(spec, dataset string, metrics []metricBinding) ([]columnstore.OrderBy, error) {
	terms := parseList(spec)
	if len(terms) == 0 {
		return nil, nil
	}

	aliasToMetricName := make(map[string]string, len(metrics))
	for _, metric := range metrics {
		aliasToMetricName[metric.output] = metric.name
	}

	out := make([]columnstore.OrderBy, 0, len(terms))
	for _, term := range terms {
		field, desc, err := parseOrderTerm(term)
		if err != nil {
			return nil, err
		}
		field = normalizeOrderFieldForDataset(field, dataset)
		if metricName, ok := aliasToMetricName[field]; ok {
			field = metricName
		}
		out = append(out, columnstore.OrderBy{Field: field, Desc: desc})
	}
	return out, nil
}

func parseOrderTerm(term string) (field string, desc bool, err error) {
	trimmed := strings.TrimSpace(term)
	if trimmed == "" {
		return "", false, fmt.Errorf("order term cannot be empty")
	}

	field = trimmed
	desc = false
	if idx := strings.LastIndex(trimmed, ":"); idx > 0 {
		suffix := strings.ToLower(strings.TrimSpace(trimmed[idx+1:]))
		switch suffix {
		case "asc":
			field = strings.TrimSpace(trimmed[:idx])
			desc = false
		case "desc":
			field = strings.TrimSpace(trimmed[:idx])
			desc = true
		}
	}

	if field == "" {
		return "", false, fmt.Errorf("order term %q has empty field", term)
	}
	return field, desc, nil
}

func parseFilter(spec string) (columnstore.Filter, error) {
	sexp, err := Parse(spec)
	if err != nil {
		return nil, err
	}
	return filterFromSexp(sexp)
}

type ingestStats struct {
	Rows     int
	Batches  int
	Duration time.Duration
}

func openInput(path string, stdin io.Reader) (io.ReadCloser, error) {
	trimmed := strings.TrimSpace(path)
	if trimmed == "" {
		return io.NopCloser(stdin), nil
	}
	return os.Open(trimmed)
}

func ingestNDJSON(r io.Reader, store *columnstore.Store, dataset string, batchSize int) (ingestStats, error) {
	if batchSize <= 0 {
		return ingestStats{}, fmt.Errorf("batch size must be greater than 0")
	}

	reader := bufio.NewReaderSize(r, 1024*1024)
	batch := make([]columnstore.RowInput, 0, batchSize)
	var stats ingestStats
	start := time.Now()

	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		if _, err := store.IngestBatch(dataset, batch); err != nil {
			return err
		}
		stats.Rows += len(batch)
		stats.Batches++
		batch = batch[:0]
		return nil
	}

	lineNo := 0
	for {
		line, err := reader.ReadBytes('\n')
		if err != nil && err != io.EOF {
			return ingestStats{}, err
		}

		trimmed := strings.TrimSpace(string(line))
		if trimmed != "" {
			lineNo++
			row, parseErr := parseNDJSONRow(trimmed)
			if parseErr != nil {
				return ingestStats{}, fmt.Errorf("line %d: %w", lineNo, parseErr)
			}
			batch = append(batch, row)
			if len(batch) >= batchSize {
				if flushErr := flush(); flushErr != nil {
					return ingestStats{}, flushErr
				}
			}
		}

		if err == io.EOF {
			break
		}
	}

	if err := flush(); err != nil {
		return ingestStats{}, err
	}
	stats.Duration = time.Since(start)
	return stats, nil
}

func parseNDJSONRow(line string) (columnstore.RowInput, error) {
	dec := json.NewDecoder(strings.NewReader(line))
	dec.UseNumber()

	var raw map[string]any
	if err := dec.Decode(&raw); err != nil {
		return columnstore.RowInput{}, err
	}
	if dec.More() {
		return columnstore.RowInput{}, fmt.Errorf("expected one JSON object per line")
	}

	timestampValue, ok := raw["timestamp"]
	if !ok {
		return columnstore.RowInput{}, fmt.Errorf("missing timestamp field")
	}
	timestamp, err := parseJSONTimestamp(timestampValue)
	if err != nil {
		return columnstore.RowInput{}, fmt.Errorf("parse timestamp: %w", err)
	}

	delete(raw, "timestamp")
	values := make(map[string]any, len(raw))
	flattenJSONValues("", raw, values)

	return columnstore.RowInput{Timestamp: timestamp, Values: values}, nil
}

func parseJSONTimestamp(value any) (time.Time, error) {
	switch v := value.(type) {
	case string:
		return parseTime(v)
	case json.Number:
		if i, err := v.Int64(); err == nil {
			return time.Unix(i, 0).UTC(), nil
		}
		f, err := v.Float64()
		if err != nil {
			return time.Time{}, err
		}
		return time.Unix(int64(f), 0).UTC(), nil
	case float64:
		return time.Unix(int64(v), 0).UTC(), nil
	case int64:
		return time.Unix(v, 0).UTC(), nil
	default:
		return time.Time{}, fmt.Errorf("unsupported timestamp type %T", value)
	}
}

func normalizeJSONValue(value any) any {
	switch v := value.(type) {
	case json.Number:
		if i, err := v.Int64(); err == nil {
			return i
		}
		if f, err := v.Float64(); err == nil {
			return f
		}
		return v.String()
	case map[string]any:
		out := make(map[string]any, len(v))
		for key, nested := range v {
			out[key] = normalizeJSONValue(nested)
		}
		return out
	case []any:
		out := make([]any, len(v))
		for i, nested := range v {
			out[i] = normalizeJSONValue(nested)
		}
		return out
	default:
		return value
	}
}

func flattenJSONValues(prefix string, value any, out map[string]any) {
	value = normalizeJSONValue(value)

	switch v := value.(type) {
	case map[string]any:
		for key, nested := range v {
			flattenJSONValues(joinFlattenedKey(prefix, key), nested, out)
		}
	case []any:
		for i, nested := range v {
			flattenJSONValues(joinFlattenedKey(prefix, fmt.Sprintf("%d", i)), nested, out)
		}
	default:
		if prefix == "" {
			return
		}
		out[prefix] = v
	}
}

func joinFlattenedKey(prefix, key string) string {
	if prefix == "" {
		return key
	}
	return prefix + "." + key
}

func normalizeOrderFieldForDataset(field, dataset string) string {
	if field == "count" {
		return field
	}
	if open := strings.IndexByte(field, '('); open > 0 {
		if close := strings.LastIndexByte(field, ')'); close == len(field)-1 && open < close {
			op := strings.ToLower(strings.TrimSpace(field[:open]))
			column := strings.TrimSpace(field[open+1 : close])
			if op == "count" && column == "" {
				return "count"
			}
			return op + ":" + normalizeFieldForDataset(column, dataset)
		}
	}
	return normalizeFieldForDataset(field, dataset)
}

func parseList(s string) []string {
	if s == "" {
		return []string{}
	}
	parts := strings.Split(s, ",")
	result := make([]string, len(parts))
	for i, p := range parts {
		result[i] = strings.TrimSpace(p)
	}
	return result
}

func parseTimeRange(startStr, endStr string) (time.Time, time.Time, error) {
	now := time.Now().UTC()

	// Parse start time
	var start time.Time
	var err error
	if startStr == "" {
		// Default to 30 days ago
		start = now.AddDate(0, 0, -30)
	} else {
		start, err = parseTime(startStr)
		if err != nil {
			return time.Time{}, time.Time{}, fmt.Errorf("start time: %w", err)
		}
	}

	// Parse end time
	var end time.Time
	if endStr == "" {
		// Default to now
		end = now
	} else {
		end, err = parseTime(endStr)
		if err != nil {
			return time.Time{}, time.Time{}, fmt.Errorf("end time: %w", err)
		}
	}

	return start, end, nil
}

func parseTime(s string) (time.Time, error) {
	// Try RFC3339 first
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return t, nil
	}

	// Try date-only format (YYYY-MM-DD)
	if t, err := time.Parse("2006-01-02", s); err == nil {
		return t, nil
	}

	return time.Time{}, fmt.Errorf("unable to parse time: %s (expected RFC3339 or YYYY-MM-DD)", s)
}

func renderTable(data []map[string]any, dimensions, metrics []string) {
	if len(data) == 0 {
		return
	}

	// Build headers: dimensions first, then metrics
	headers := make([]string, 0, len(dimensions)+len(metrics))
	headers = append(headers, dimensions...)
	headers = append(headers, metrics...)

	// Build rows
	rows := make([][]string, 0, len(data))
	for _, row := range data {
		rowData := make([]string, 0, len(headers))

		// Add dimension values
		for _, dim := range dimensions {
			val := ""
			if v, ok := row[dim]; ok {
				val = fmt.Sprintf("%v", v)
			}
			rowData = append(rowData, val)
		}

		// Add metric values
		for _, metric := range metrics {
			val := ""
			if v, ok := row[metric]; ok {
				switch n := v.(type) {
				case float64:
					// Format floats nicely
					if n == float64(int64(n)) {
						val = fmt.Sprintf("%d", int64(n))
					} else {
						val = fmt.Sprintf("%.2f", n)
					}
				default:
					val = fmt.Sprintf("%v", v)
				}
			}
			rowData = append(rowData, val)
		}

		rows = append(rows, rowData)
	}

	// Create table using v1.1.x API
	table := tablewriter.NewWriter(os.Stdout)
	table.Header(headers)
	if err := table.Bulk(rows); err != nil {
		slog.Error("Failed to add rows to table", "error", err)
		return
	}
	if err := table.Render(); err != nil {
		slog.Error("Failed to render table", "error", err)
		return
	}
}
