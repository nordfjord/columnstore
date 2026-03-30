package columnstore

import (
	"context"
	"time"

	"github.com/nordfjord/columnstore/internal/filter"
)

type Store struct {
	basePath    string
	ingester    *Ingester
	compacter   *Compacter
	partitioner Partitioner
}

type StoreOption func(*Store)

func WithPartitioning(partitioner Partitioner) StoreOption {
	if partitioner == nil {
		panic("partitioner is required")
	}
	return func(s *Store) {
		s.partitioner = partitioner
	}
}

type RowInput struct {
	Timestamp time.Time
	Values    map[string]any
}

type IngestResult struct {
	RowID     int32
	Partition string
}

func NewStore(basePath string, opts ...StoreOption) *Store {
	store := &Store{
		basePath:    basePath,
		partitioner: defaultPartitioner(),
	}
	for _, opt := range opts {
		if opt != nil {
			opt(store)
		}
	}

	allocator := NewPartitionRowIDAllocator()
	ingester := NewIngester(basePath, allocator, store.partitioner)
	store.ingester = ingester
	store.compacter = NewCompacter(ingester)
	return store
}

type Query struct {
	Start       time.Time
	End         time.Time
	Dataset     string
	Dimensions  []string
	Granularity string
	Timezone    string
	OrderBy     []OrderBy
	Metrics     []Metric
	Metric      Metric
	Filter      filter.Filter
}

type OrderBy struct {
	Field string
	Desc  bool
}

type QueryResult struct {
	Rows []QueryResultRow
}

func (s *Store) IngestRow(dataset string, timestamp time.Time, row map[string]any) (int32, string, error) {
	return s.ingester.IngestRow(dataset, timestamp, row)
}

func (s *Store) IngestBatch(dataset string, rows []RowInput) ([]IngestResult, error) {
	return s.ingester.IngestBatch(dataset, rows)
}

func (s *Store) CompactPartition(ctx context.Context, dataset, partition, keyColumn string) error {
	return s.compacter.CompactPartition(ctx, dataset, partition, keyColumn)
}

func (s *Store) CompactDataset(ctx context.Context, dataset, keyColumn string, includeActive bool) error {
	return s.compacter.CompactDataset(ctx, dataset, keyColumn, includeActive)
}
