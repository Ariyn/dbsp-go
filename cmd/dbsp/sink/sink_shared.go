package sink

import (
	"github.com/ariyn/dbsp/cmd/dbsp/provider"
	"github.com/ariyn/dbsp/internal/dbsp/types"
)

// NoopCloseSink delegates writes while preventing shared sinks from being closed multiple times.
type NoopCloseSink struct {
	inner provider.Sink
}

func NewNoopCloseSink(inner provider.Sink) *NoopCloseSink {
	return &NoopCloseSink{inner: inner}
}

func (s *NoopCloseSink) WriteBatch(batch types.Batch) error {
	return s.inner.WriteBatch(batch)
}

func (s *NoopCloseSink) WriteBatchWithPartition(batch types.Batch, values map[string]string) error {
	if ps, ok := s.inner.(PartitionedSink); ok {
		return ps.WriteBatchWithPartition(batch, values)
	}
	return s.inner.WriteBatch(batch)
}

func (s *NoopCloseSink) Close() error {
	return nil
}
