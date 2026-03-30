package columnstore

import (
	"fmt"
	"slices"
	"time"
)

const (
	MetricOpCount         = "count"
	MetricOpSum           = "sum"
	MetricOpAvg           = "avg"
	MetricOpMax           = "max"
	MetricOpMin           = "min"
	MetricOpCountDistinct = "count_distinct"
	MetricOpP50           = "p50"
	MetricOpP75           = "p75"
	MetricOpP90           = "p90"
	MetricOpP95           = "p95"
	MetricOpP99           = "p99"
	MetricOpP999          = "p999"
	initialGroupMapHint   = 128
)

// Metric is the typed metric AST used by columnstore.
type Metric interface {
	Validate() error
	Name() string
	Field() string
	appendSpecs([]metricSpec, *metricSpecBuilder) []metricSpec
}

type metricSpecBuilder struct {
	percentileSourceByColumn map[string]int
}

func newMetricSpecBuilder() *metricSpecBuilder {
	return &metricSpecBuilder{percentileSourceByColumn: make(map[string]int)}
}

func (b *metricSpecBuilder) percentileSource(column string, next int) int {
	if source, ok := b.percentileSourceByColumn[column]; ok {
		return source
	}
	b.percentileSourceByColumn[column] = next
	return next
}

// CountMetric represents Count.
type CountMetric struct{}

var _ Metric = (*CountMetric)(nil)

// Count constructs a Count metric.
func Count() CountMetric {
	return CountMetric{}
}

func (m CountMetric) Field() string {
	return ""
}

// Validate validates the Count metric.
func (m CountMetric) Validate() error {
	return nil
}

// Name returns the canonical metric name.
func (m CountMetric) Name() string {
	return MetricOpCount
}

func (m CountMetric) appendSpecs(specs []metricSpec, _ *metricSpecBuilder) []metricSpec {
	return append(specs, metricSpec{name: m.Name(), op: MetricOpCount})
}

// SumMetric represents Sum(column).
type SumMetric struct {
	Column string
}

var _ Metric = (*SumMetric)(nil)

// Sum constructs a Sum metric.
func Sum(column string) SumMetric {
	return SumMetric{Column: column}
}

func (m SumMetric) Validate() error {
	if m.Column == "" {
		return fmt.Errorf("sum metric requires column")
	}
	return nil
}

func (m SumMetric) Name() string {
	return fmt.Sprintf("%s(%s)", MetricOpSum, m.Column)
}

func (m SumMetric) Field() string {
	return m.Column
}

func (m SumMetric) appendSpecs(specs []metricSpec, _ *metricSpecBuilder) []metricSpec {
	return append(specs, metricSpec{name: m.Name(), op: MetricOpSum, column: m.Column})
}

// AvgMetric represents Avg(column).
type AvgMetric struct {
	Column string
}

var _ Metric = (*AvgMetric)(nil)

// Avg constructs an Avg metric.
func Avg(column string) AvgMetric {
	return AvgMetric{Column: column}
}

func (m AvgMetric) Validate() error {
	if m.Column == "" {
		return fmt.Errorf("avg metric requires column")
	}
	return nil
}

func (m AvgMetric) Name() string {
	return fmt.Sprintf("%s(%s)", MetricOpAvg, m.Column)
}

func (m AvgMetric) Field() string {
	return m.Column
}

func (m AvgMetric) appendSpecs(specs []metricSpec, _ *metricSpecBuilder) []metricSpec {
	return append(specs, metricSpec{name: m.Name(), op: MetricOpAvg, column: m.Column})
}

// MaxMetric represents Max(column).
type MaxMetric struct {
	Column string
}

var _ Metric = (*MaxMetric)(nil)

// Max constructs a Max metric.
func Max(column string) MaxMetric {
	return MaxMetric{Column: column}
}

func (m MaxMetric) Validate() error {
	if m.Column == "" {
		return fmt.Errorf("max metric requires column")
	}
	return nil
}

func (m MaxMetric) Name() string {
	return fmt.Sprintf("%s(%s)", MetricOpMax, m.Column)
}
func (m MaxMetric) Field() string {
	return m.Column
}

func (m MaxMetric) appendSpecs(specs []metricSpec, _ *metricSpecBuilder) []metricSpec {
	return append(specs, metricSpec{name: m.Name(), op: MetricOpMax, column: m.Column})
}

// MinMetric represents Min(column).
type MinMetric struct {
	Column string
}

var _ Metric = (*MinMetric)(nil)

// Min constructs a Min metric.
func Min(column string) MinMetric {
	return MinMetric{Column: column}
}

func (m MinMetric) Validate() error {
	if m.Column == "" {
		return fmt.Errorf("min metric requires column")
	}
	return nil
}

func (m MinMetric) Name() string {
	return fmt.Sprintf("%s(%s)", MetricOpMin, m.Column)
}

func (m MinMetric) Field() string {
	return m.Column
}

func (m MinMetric) appendSpecs(specs []metricSpec, _ *metricSpecBuilder) []metricSpec {
	return append(specs, metricSpec{name: m.Name(), op: MetricOpMin, column: m.Column})
}

// CountDistinctMetric represents CountDistinct(column).
type CountDistinctMetric struct {
	Column string
}

var _ Metric = (*CountDistinctMetric)(nil)

// CountDistinct constructs a CountDistinct metric.
func CountDistinct(column string) CountDistinctMetric {
	return CountDistinctMetric{Column: column}
}

func (m CountDistinctMetric) Validate() error {
	if m.Column == "" {
		return fmt.Errorf("count_distinct metric requires column")
	}
	return nil
}

func (m CountDistinctMetric) Name() string {
	return fmt.Sprintf("count(%s)", m.Column)
}
func (m CountDistinctMetric) Field() string {
	return m.Column
}

func (m CountDistinctMetric) appendSpecs(specs []metricSpec, _ *metricSpecBuilder) []metricSpec {
	return append(specs, metricSpec{name: m.Name(), op: MetricOpCountDistinct, column: m.Column})
}

// PercentileMetric represents percentile(column) operations.
type PercentileMetric struct {
	Column   string
	Op       string
	Quantile float64
}

var _ Metric = (*PercentileMetric)(nil)

func newPercentileMetric(op, column string, quantile float64) PercentileMetric {
	return PercentileMetric{Column: column, Op: op, Quantile: quantile}
}

func P50(column string) PercentileMetric {
	return newPercentileMetric(MetricOpP50, column, 0.50)
}

func P75(column string) PercentileMetric {
	return newPercentileMetric(MetricOpP75, column, 0.75)
}

func P90(column string) PercentileMetric {
	return newPercentileMetric(MetricOpP90, column, 0.90)
}

func P95(column string) PercentileMetric {
	return newPercentileMetric(MetricOpP95, column, 0.95)
}

func P99(column string) PercentileMetric {
	return newPercentileMetric(MetricOpP99, column, 0.99)
}

func P999(column string) PercentileMetric {
	return newPercentileMetric(MetricOpP999, column, 0.999)
}

func (m PercentileMetric) Validate() error {
	if m.Column == "" {
		return fmt.Errorf("%s metric requires column", m.Op)
	}
	if _, ok := percentileOpToQuantile[m.Op]; !ok {
		return fmt.Errorf("unsupported percentile metric op %q", m.Op)
	}
	if m.Quantile <= 0 || m.Quantile > 1 {
		return fmt.Errorf("invalid percentile quantile %v", m.Quantile)
	}
	return nil
}

func (m PercentileMetric) Name() string {
	return fmt.Sprintf("%s(%s)", m.Op, m.Column)
}
func (m PercentileMetric) Field() string {
	return m.Column
}

func (m PercentileMetric) appendSpecs(specs []metricSpec, builder *metricSpecBuilder) []metricSpec {
	source := builder.percentileSource(m.Column, len(specs))
	return append(specs, metricSpec{
		name:             m.Name(),
		op:               m.Op,
		column:           m.Column,
		percentile:       m.Quantile,
		percentileSource: source,
	})
}

// countAccumulator accumulates grouped counts.
type countAccumulator struct {
	groups map[string]*QueryResultRow
}

// newCountAccumulator creates an empty grouped count accumulator.
func newCountAccumulator() *countAccumulator {
	return &countAccumulator{groups: make(map[string]*QueryResultRow)}
}

// Add increments the count for the provided dimension tuple.
func (a *countAccumulator) Add(dimensions []any) {
	if a == nil {
		return
	}

	key := BuildGroupKey(dimensions)
	row, exists := a.groups[key]
	if !exists {
		row = &QueryResultRow{}
		for i := range dimensions {
			setCountAccumulatorDimension(row, i, dimensions[i])
		}
		a.groups[key] = row
	}
	count := row.Int64(CountField)
	row.SetInt64(CountField, count+1)
}

// AddN increments the count by n for the provided dimension tuple.
func (a *countAccumulator) AddN(dimensions []any, n int64) {
	if a == nil || n <= 0 {
		return
	}
	key := BuildGroupKey(dimensions)
	row, exists := a.groups[key]
	if !exists {
		row = &QueryResultRow{}
		for i := range dimensions {
			setCountAccumulatorDimension(row, i, dimensions[i])
		}
		a.groups[key] = row
	}
	count := row.Int64(CountField)
	row.SetInt64(CountField, count+n)
}

// CountFor returns the count for a specific dimension tuple.
func (a *countAccumulator) CountFor(dimensions []any) int64 {
	if a == nil {
		return 0
	}
	row, ok := a.groups[BuildGroupKey(dimensions)]
	if !ok {
		return 0
	}
	count := row.Int64(CountField)
	return count
}

// Rows returns grouped rows sorted by deterministic group key.
func (a *countAccumulator) Rows() []QueryResultRow {
	if a == nil || len(a.groups) == 0 {
		return nil
	}

	keys := make([]string, 0, len(a.groups))
	for k := range a.groups {
		keys = append(keys, k)
	}
	slices.Sort(keys)

	rows := make([]QueryResultRow, 0, len(keys))
	for _, k := range keys {
		row := a.groups[k]
		rows = append(rows, row.clone())
	}

	return rows
}

func setCountAccumulatorDimension(row *QueryResultRow, index int, value any) {
	name := dimensionFieldName(index)
	switch v := value.(type) {
	case nil:
		row.SetNull(name)
	case int64:
		row.SetInt64(name, v)
	case int:
		row.SetInt64(name, int64(v))
	case float64:
		row.SetFloat64(name, v)
	case float32:
		row.SetFloat64(name, float64(v))
	case string:
		row.SetString(name, v)
	case bool:
		row.SetBool(name, v)
	case time.Time:
		row.SetTime(name, v)
	default:
		panic("unsupported count accumulator dimension type")
	}
}
