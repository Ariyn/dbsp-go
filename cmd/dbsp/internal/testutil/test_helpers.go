package testutil

import (
"sync"

"github.com/ariyn/dbsp/internal/dbsp/types"
)

type RecordingSink struct {
	mu        sync.Mutex
	Batches   []types.Batch
	WriteCh   chan struct{}
	CloseCall int
}

func NewRecordingSink() *RecordingSink {
	return &RecordingSink{WriteCh: make(chan struct{}, 100)}
}

func (r *RecordingSink) WriteBatch(b types.Batch) error {
	r.mu.Lock()
	copied := append(types.Batch(nil), b...)
	r.Batches = append(r.Batches, copied)
	r.mu.Unlock()

	r.WriteCh <- struct{}{}
	return nil
}

func (r *RecordingSink) Close() error {
	r.mu.Lock()
	r.CloseCall++
	r.mu.Unlock()
	return nil
}

func (r *RecordingSink) BatchLens() []int {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]int, 0, len(r.Batches))
	for _, b := range r.Batches {
		out = append(out, len(b))
	}
	return out
}

type SliceSource struct {
	Batches []types.Batch
	Current int
}

func NewSliceSource(batches []types.Batch) *SliceSource {
	return &SliceSource{Batches: batches}
}

func (s *SliceSource) NextBatch() (types.Batch, error) {
	if s.Current >= len(s.Batches) {
		return nil, nil
	}
	b := s.Batches[s.Current]
	s.Current++
	return b, nil
}

func (s *SliceSource) Close() error {
	return nil
}
