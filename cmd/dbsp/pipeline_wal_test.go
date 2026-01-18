package main

import (
	"context"
	"path/filepath"
	"sync"
	"testing"

	"github.com/ariyn/dbsp/internal/dbsp/types"
	"github.com/ariyn/dbsp/internal/dbsp/wal"
)

type sliceSource struct {
	mu      sync.Mutex
	batches []types.Batch
	closed  bool
}

func newSliceSource(batches []types.Batch) *sliceSource {
	return &sliceSource{batches: batches}
}

func (s *sliceSource) NextBatch() (types.Batch, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, nil
	}
	if len(s.batches) == 0 {
		return nil, nil
	}
	b := s.batches[0]
	s.batches = s.batches[1:]
	return b, nil
}

func (s *sliceSource) Close() error {
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()
	return nil
}

type countingSink struct {
	mu         sync.Mutex
	writeCalls int
}

func (s *countingSink) WriteBatch(types.Batch) error {
	s.mu.Lock()
	s.writeCalls++
	s.mu.Unlock()
	return nil
}

func (s *countingSink) Close() error { return nil }

func (s *countingSink) Calls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.writeCalls
}

func TestRunPipeline_WAL_AppendsBatches(t *testing.T) {
	tmp := t.TempDir()
	dbPath := filepath.Join(tmp, "wal.db")

	w, err := wal.NewSQLiteWAL(dbPath)
	if err != nil {
		t.Fatalf("NewSQLiteWAL: %v", err)
	}
	defer w.Close()

	source := newSliceSource([]types.Batch{
		{{Tuple: types.Tuple{"id": int64(1)}, Count: 1}},
		{{Tuple: types.Tuple{"id": int64(2)}, Count: 1}},
	})
	sink := &countingSink{}

	execute := func(b types.Batch) (types.Batch, error) { return b, nil }
	if err := runPipeline(context.Background(), source, sink, execute, w, nil, 0); err != nil {
		t.Fatalf("runPipeline: %v", err)
	}

	// Reopen and replay to confirm 2 appends were persisted.
	w2, err := wal.NewSQLiteWAL(dbPath)
	if err != nil {
		t.Fatalf("reopen WAL: %v", err)
	}
	defer w2.Close()

	count := 0
	if err := w2.Replay(context.Background(), func(types.Batch) error {
		count++
		return nil
	}); err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if count != 2 {
		t.Fatalf("expected 2 replayed batches, got %d", count)
	}
}

func TestRunPipeline_WAL_ReplayDoesNotWriteSink(t *testing.T) {
	tmp := t.TempDir()
	dbPath := filepath.Join(tmp, "wal.db")

	w, err := wal.NewSQLiteWAL(dbPath)
	if err != nil {
		t.Fatalf("NewSQLiteWAL: %v", err)
	}
	defer w.Close()

	if err := w.Append(context.Background(), types.Batch{{Tuple: types.Tuple{"id": int64(1)}, Count: 1}}); err != nil {
		t.Fatalf("Append: %v", err)
	}

	source := newSliceSource(nil) // no live batches
	sink := &countingSink{}

	executeCalls := 0
	execute := func(b types.Batch) (types.Batch, error) {
		executeCalls++
		return b, nil
	}

	if err := runPipeline(context.Background(), source, sink, execute, w, nil, 0); err != nil {
		t.Fatalf("runPipeline: %v", err)
	}

	if executeCalls != 1 {
		t.Fatalf("expected execute to be called once via replay, got %d", executeCalls)
	}
	if sink.Calls() != 0 {
		t.Fatalf("expected sink to not be written during replay, got %d", sink.Calls())
	}
}

type testSnapshotter struct {
	mu           sync.Mutex
	snapshot     []byte
	restoreCalls int
}

func (t *testSnapshotter) Snapshot() ([]byte, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]byte(nil), t.snapshot...), nil
}

func (t *testSnapshotter) Restore(b []byte) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.restoreCalls++
	t.snapshot = append([]byte(nil), b...)
	return nil
}

func (t *testSnapshotter) RestoreCalls() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.restoreCalls
}

func TestRunPipeline_WAL_Checkpoint_RestoreAndSuffixReplay(t *testing.T) {
	tmp := t.TempDir()
	dbPath := filepath.Join(tmp, "wal.db")

	w, err := wal.NewSQLiteWAL(dbPath)
	if err != nil {
		t.Fatalf("NewSQLiteWAL: %v", err)
	}
	defer w.Close()

	// Persist 3 batches.
	ctx := context.Background()
	if err := w.Append(ctx, types.Batch{{Tuple: types.Tuple{"id": int64(1)}, Count: 1}}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := w.Append(ctx, types.Batch{{Tuple: types.Tuple{"id": int64(2)}, Count: 1}}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := w.Append(ctx, types.Batch{{Tuple: types.Tuple{"id": int64(3)}, Count: 1}}); err != nil {
		t.Fatalf("Append: %v", err)
	}

	maxSeq, err := w.MaxSeq(ctx)
	if err != nil {
		t.Fatalf("MaxSeq: %v", err)
	}
	if maxSeq != 3 {
		t.Fatalf("expected maxSeq=3, got %d", maxSeq)
	}
	if err := w.SaveCheckpoint(ctx, wal.Checkpoint{LastSeq: 1, Snapshot: []byte("snap")}); err != nil {
		t.Fatalf("SaveCheckpoint: %v", err)
	}

	// No live batches; pipeline should restore snapshot and replay batches with seq>1 (2 batches).
	source := newSliceSource(nil)
	sink := &countingSink{}
	snap := &testSnapshotter{}

	executeCalls := 0
	execute := func(b types.Batch) (types.Batch, error) {
		executeCalls++
		return b, nil
	}

	if err := runPipeline(ctx, source, sink, execute, w, snap, 0); err != nil {
		t.Fatalf("runPipeline: %v", err)
	}
	if snap.RestoreCalls() != 1 {
		t.Fatalf("expected snapshot restore once, got %d", snap.RestoreCalls())
	}
	if executeCalls != 2 {
		t.Fatalf("expected suffix replay executeCalls=2, got %d", executeCalls)
	}
	if sink.Calls() != 0 {
		t.Fatalf("expected sink to not be written during replay, got %d", sink.Calls())
	}
}
