package columnstore

import (
	"fmt"
	"os"
	"slices"
	"time"
)

func normalizeMetricsInput(q Query) ([]Metric, error) {
	if q.Metric != nil && len(q.Metrics) > 0 {
		return nil, fmt.Errorf("provide either metric or metrics, not both")
	}
	if len(q.Metrics) > 0 {
		return q.Metrics, nil
	}
	if q.Metric == nil {
		return nil, fmt.Errorf("metric is required")
	}
	return []Metric{q.Metric}, nil
}

func metricSourceColumns(metrics []Metric) []string {
	seen := make(map[string]struct{})
	for _, metric := range metrics {
		column := metric.Field()
		if column == "" {
			continue
		}
		seen[column] = struct{}{}
	}

	columns := make([]string, 0, len(seen))
	for column := range seen {
		columns = append(columns, column)
	}
	slices.Sort(columns)
	return columns
}

func listDatasetPartitions(datasetPath string) ([]string, error) {
	entries, err := os.ReadDir(datasetPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	partitions := make([]string, 0, len(entries))
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		partitions = append(partitions, e.Name())
	}
	slices.Sort(partitions)
	return partitions, nil
}

func pruneByPartitionBounds(partitions []string, start, end time.Time, partitioner Partitioner) []string {
	out := make([]string, 0, len(partitions))
	for _, p := range partitions {
		if !partitioner.IsInBounds(p, start, end) {
			continue
		}
		out = append(out, p)
	}
	return out
}

func isCountOnlyMetric(metrics []Metric) bool {
	if len(metrics) != 1 {
		return false
	}
	return metrics[0].Name() == MetricOpCount && metrics[0].Field() == ""
}

func indexOfString(values []string, target string) int {
	for i := range values {
		if values[i] == target {
			return i
		}
	}
	return -1
}
