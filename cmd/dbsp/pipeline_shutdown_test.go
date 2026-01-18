package main

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/ariyn/dbsp/internal/dbsp/types"
)

type blockingSource struct {
	closed     chan struct{}
	closeOnce  sync.Once
	closeCount int
	mu         sync.Mutex
}

func newBlockingSource() *blockingSource {
	return &blockingSource{closed: make(chan struct{})}
}

func (s *blockingSource) NextBatch() (types.Batch, error) {
	<-s.closed
	return nil, nil
}

func (s *blockingSource) Close() error {
	s.closeOnce.Do(func() {
		s.mu.Lock()
		s.closeCount++
		s.mu.Unlock()
		close(s.closed)
	})
	return nil
}

func (s *blockingSource) CloseCalled() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closeCount
}

type noopSink struct{}

func (n *noopSink) WriteBatch(types.Batch) error { return nil }
func (n *noopSink) Close() error                 { return nil }

func TestRunPipeline_CancelUnblocksSource(t *testing.T) {
	source := newBlockingSource()
	sink := &noopSink{}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := runPipeline(ctx, source, sink, func(b types.Batch) (types.Batch, error) { return b, nil }, nil, nil, 0)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	if source.CloseCalled() != 1 {
		t.Fatalf("expected source.Close to be called once, got %d", source.CloseCalled())
	}
}
