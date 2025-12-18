package main

import (
	"sync"
	"testing"
	"time"

	"github.com/ariyn/dbsp/internal/dbsp/types"
)

type recordingSink struct {
	mu        sync.Mutex
	batches   []types.Batch
	writeCh   chan struct{}
	closeCall int
}

func newRecordingSink() *recordingSink {
	return &recordingSink{writeCh: make(chan struct{}, 100)}
}

func (r *recordingSink) WriteBatch(b types.Batch) error {
	r.mu.Lock()
	copied := append(types.Batch(nil), b...)
	r.batches = append(r.batches, copied)
	r.mu.Unlock()

	r.writeCh <- struct{}{}
	return nil
}

func (r *recordingSink) Close() error {
	r.mu.Lock()
	r.closeCall++
	r.mu.Unlock()
	return nil
}

func (r *recordingSink) batchLens() []int {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]int, 0, len(r.batches))
	for _, b := range r.batches {
		out = append(out, len(b))
	}
	return out
}

func TestBatchSink_FlushOnMaxSize(t *testing.T) {
	inner := newRecordingSink()
	sink := NewBatchSink(inner, 3, 0)

	for i := 0; i < 5; i++ {
		if err := sink.WriteBatch(types.Batch{{Tuple: types.Tuple{"id": i}, Count: 1}}); err != nil {
			t.Fatalf("WriteBatch error: %v", err)
		}
	}
	if err := sink.Close(); err != nil {
		t.Fatalf("Close error: %v", err)
	}

	lens := inner.batchLens()
	if len(lens) != 2 {
		t.Fatalf("expected 2 flushes, got %d (%v)", len(lens), lens)
	}
	if lens[0] != 3 || lens[1] != 2 {
		t.Fatalf("expected [3 2], got %v", lens)
	}
}

func TestBatchSink_FlushOnDelay(t *testing.T) {
	inner := newRecordingSink()
	sink := NewBatchSink(inner, 100, 40*time.Millisecond)

	if err := sink.WriteBatch(types.Batch{{Tuple: types.Tuple{"id": 1}, Count: 1}}); err != nil {
		t.Fatalf("WriteBatch error: %v", err)
	}

	select {
	case <-inner.writeCh:
		// ok
	case <-time.After(200 * time.Millisecond):
		t.Fatalf("expected delayed flush")
	}

	lens := inner.batchLens()
	if len(lens) != 1 || lens[0] != 1 {
		t.Fatalf("expected one flush of 1, got %v", lens)
	}

	if err := sink.Close(); err != nil {
		t.Fatalf("Close error: %v", err)
	}
}

func TestBatchSink_CloseFlushesRemaining(t *testing.T) {
	inner := newRecordingSink()
	sink := NewBatchSink(inner, 10, time.Second)

	if err := sink.WriteBatch(types.Batch{{Tuple: types.Tuple{"id": 1}, Count: 1}}); err != nil {
		t.Fatalf("WriteBatch error: %v", err)
	}
	if err := sink.WriteBatch(types.Batch{{Tuple: types.Tuple{"id": 2}, Count: 1}}); err != nil {
		t.Fatalf("WriteBatch error: %v", err)
	}

	if err := sink.Close(); err != nil {
		t.Fatalf("Close error: %v", err)
	}

	lens := inner.batchLens()
	if len(lens) != 1 || lens[0] != 2 {
		t.Fatalf("expected one flush of 2 on Close, got %v", lens)
	}
}
