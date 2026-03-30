package columnstore

import (
	"strings"
	"time"
)

const (
	PartitionFormatMonthly = "2006-01"
	PartitionFormatYearly  = "2006"
)

type Partitioner interface {
	FromTimestamp(ts time.Time) string
	IsInBounds(partition string, start, end time.Time) bool
}

type formatPartitioner struct {
	format string
	step   partitionStep
}

type partitionStep int

const (
	partitionStepDay partitionStep = iota
	partitionStepMonth
	partitionStepYear
)

func (p formatPartitioner) FromTimestamp(ts time.Time) string {
	return ts.UTC().Format(p.format)
}

func (p formatPartitioner) IsInBounds(partition string, start, end time.Time) bool {
	partitionStart, err := time.Parse(p.format, partition)
	if err != nil {
		return true
	}

	partitionStart = partitionStart.UTC()
	partitionEnd := addPartitionStep(partitionStart, p.step)

	if !partitionEnd.After(start.UTC()) {
		return false
	}
	if !partitionStart.Before(end.UTC()) {
		return false
	}
	return true
}

func addPartitionStep(ts time.Time, step partitionStep) time.Time {
	switch step {
	case partitionStepYear:
		return ts.AddDate(1, 0, 0)
	case partitionStepMonth:
		return ts.AddDate(0, 1, 0)
	default:
		return ts.AddDate(0, 0, 1)
	}
}

func stepFromFormat(format string) partitionStep {
	if strings.Contains(format, "02") {
		return partitionStepDay
	}
	if strings.Contains(format, "01") {
		return partitionStepMonth
	}
	return partitionStepYear
}

func defaultPartitioner() Partitioner {
	return formatPartitioner{format: PartitionFormatMonthly, step: partitionStepMonth}
}

func PartitionerFromFormat(format string) Partitioner {
	normalized := strings.TrimSpace(strings.ToLower(format))
	switch normalized {
	case "", "month", "monthly":
		return formatPartitioner{format: PartitionFormatMonthly, step: partitionStepMonth}
	case "year", "yearly", "annual":
		return formatPartitioner{format: PartitionFormatYearly, step: partitionStepYear}
	default:
		return formatPartitioner{format: format, step: stepFromFormat(format)}
	}
}
