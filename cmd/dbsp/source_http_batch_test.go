package main

import (
	"testing"
	"time"

	"github.com/ariyn/dbsp/internal/dbsp/types"
)

func TestHTTPSourceBatch_MaxBatchSize(t *testing.T) {
	s := &HTTPSource{
		buffer:        make(chan types.TupleDelta, 100),
		done:          make(chan struct{}),
		maxBatchSize:  3,
		maxBatchDelay: 500 * time.Millisecond,
	}

	// Pre-fill more than maxBatchSize.
	for i := 0; i < 5; i++ {
		s.buffer <- types.TupleDelta{Tuple: types.Tuple{"id": i}, Count: 1}
	}

	batch, err := s.NextBatch()
	if err != nil {
		t.Fatalf("NextBatch error: %v", err)
	}
	if len(batch) != 3 {
		t.Fatalf("expected 3 items, got %d", len(batch))
	}

	batch2, err := s.NextBatch()
	if err != nil {
		t.Fatalf("NextBatch (2) error: %v", err)
	}
	if len(batch2) != 2 {
		t.Fatalf("expected 2 items, got %d", len(batch2))
	}
}

func TestHTTPSourceBatch_MaxBatchDelay(t *testing.T) {
	s := &HTTPSource{
		buffer:        make(chan types.TupleDelta, 100),
		done:          make(chan struct{}),
		maxBatchSize:  100,
		maxBatchDelay: 50 * time.Millisecond,
	}

	// First item is available immediately.
	s.buffer <- types.TupleDelta{Tuple: types.Tuple{"id": 1}, Count: 1}

	// Second item arrives before the deadline; third arrives after.
	go func() {
		time.Sleep(10 * time.Millisecond)
		s.buffer <- types.TupleDelta{Tuple: types.Tuple{"id": 2}, Count: 1}
		time.Sleep(70 * time.Millisecond)
		s.buffer <- types.TupleDelta{Tuple: types.Tuple{"id": 3}, Count: 1}
	}()

	start := time.Now()
	batch, err := s.NextBatch()
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("NextBatch error: %v", err)
	}
	if len(batch) != 2 {
		t.Fatalf("expected 2 items before delay, got %d", len(batch))
	}
	if elapsed < 40*time.Millisecond || elapsed > 150*time.Millisecond {
		t.Fatalf("expected batch to return around delay, elapsed=%v", elapsed)
	}

	batch2, err := s.NextBatch()
	if err != nil {
		t.Fatalf("NextBatch (2) error: %v", err)
	}
	if len(batch2) != 1 {
		t.Fatalf("expected remaining 1 item, got %d", len(batch2))
	}
}
