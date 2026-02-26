package pipeline

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"testing"

	"github.com/ariyn/dbsp/internal/dbsp/testutil"
	"github.com/ariyn/dbsp/internal/dbsp/types"
	"github.com/ariyn/dbsp/internal/dbsp/wal"
)

type countingSink struct {
	mu         sync.Mutex
	writeCalls int
}

type failOnceSink struct {
	mu         sync.Mutex
	failed     bool
	writeCalls int
}

func (s *failOnceSink) WriteBatch(types.Batch) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.writeCalls++
	if !s.failed {
		s.failed = true
		return fmt.Errorf("injected sink failure")
	}
	return nil
}

func (s *failOnceSink) Close() error { return nil }

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

	src := testutil.NewSliceSource([]types.Batch{
		{{Tuple: types.Tuple{"id": int64(1)}, Count: 1}},
		{{Tuple: types.Tuple{"id": int64(2)}, Count: 1}},
	})
	snk := &countingSink{}

	exe := func(b types.Batch) (types.Batch, error) { return b, nil }
	if err := RunPipeline(context.Background(), src, snk, exe, w, nil, 0); err != nil {
		t.Fatalf("RunPipeline: %v", err)
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

	source := testutil.NewSliceSource(nil) // no live batches
	sink := &countingSink{}

	executeCalls := 0
	execute := func(b types.Batch) (types.Batch, error) {
		executeCalls++
		return b, nil
	}

	if err := RunPipeline(context.Background(), source, sink, execute, w, nil, 0); err != nil {
		t.Fatalf("RunPipeline: %v", err)
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

type policySnapshotter struct {
	*testSnapshotter
	mode             string
	fullEvery        int
	maxMutationBytes int
}

func (p *policySnapshotter) CheckpointMode() string           { return p.mode }
func (p *policySnapshotter) FullSnapshotEvery() int           { return p.fullEvery }
func (p *policySnapshotter) MaxIncrementalMutationBytes() int { return p.maxMutationBytes }

type mutationPolicySnapshotter struct {
	*policySnapshotter
	mu              sync.Mutex
	mutations       []wal.CheckpointMutation
	rolledBack      []wal.CheckpointMutation
	rollbackInvoked int
}

func (m *mutationPolicySnapshotter) DrainCheckpointMutations() []wal.CheckpointMutation {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := append([]wal.CheckpointMutation(nil), m.mutations...)
	m.mutations = nil
	return out
}

func (m *mutationPolicySnapshotter) RollbackCheckpointMutations(in []wal.CheckpointMutation) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.rollbackInvoked++
	m.rolledBack = append(m.rolledBack, in...)
	m.mutations = append(in, m.mutations...)
}

func (m *mutationPolicySnapshotter) RollbackCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.rollbackInvoked
}

func (m *mutationPolicySnapshotter) RolledBackMutations() []wal.CheckpointMutation {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]wal.CheckpointMutation(nil), m.rolledBack...)
}

type failingCheckpointWAL struct {
	inner wal.SQLiteWAL
	fail  bool
}

func (f *failingCheckpointWAL) Append(ctx context.Context, batch types.Batch) error {
	return f.inner.Append(ctx, batch)
}

func (f *failingCheckpointWAL) Replay(ctx context.Context, apply func(types.Batch) error) error {
	return f.inner.Replay(ctx, apply)
}

func (f *failingCheckpointWAL) ReplayFrom(ctx context.Context, afterSeq int64, apply func(types.Batch) error) error {
	return f.inner.ReplayFrom(ctx, afterSeq, apply)
}

func (f *failingCheckpointWAL) MaxSeq(ctx context.Context) (int64, error) {
	return f.inner.MaxSeq(ctx)
}

func (f *failingCheckpointWAL) LoadLatestCheckpoint(ctx context.Context) (*wal.Checkpoint, error) {
	return f.inner.LoadLatestCheckpoint(ctx)
}

func (f *failingCheckpointWAL) SaveCheckpoint(ctx context.Context, cp wal.Checkpoint) error {
	if f.fail {
		return fmt.Errorf("injected checkpoint save failure")
	}
	return f.inner.SaveCheckpoint(ctx, cp)
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
	source := testutil.NewSliceSource(nil)
	sink := &countingSink{}
	snap := &testSnapshotter{}

	executeCalls := 0
	execute := func(b types.Batch) (types.Batch, error) {
		executeCalls++
		return b, nil
	}

	if err := RunPipeline(ctx, source, sink, execute, w, snap, 0); err != nil {
		t.Fatalf("RunPipeline: %v", err)
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

func TestRunPipeline_WAL_Checkpoint_IncrementalMode_RestoresFromLatestFull(t *testing.T) {
	tmp := t.TempDir()
	dbPath := filepath.Join(tmp, "wal.db")

	w, err := wal.NewSQLiteWAL(dbPath)
	if err != nil {
		t.Fatalf("NewSQLiteWAL: %v", err)
	}
	defer w.Close()

	ctx := context.Background()
	batches := []types.Batch{
		{{Tuple: types.Tuple{"id": int64(1)}, Count: 1}},
		{{Tuple: types.Tuple{"id": int64(2)}, Count: 1}},
		{{Tuple: types.Tuple{"id": int64(3)}, Count: 1}},
	}

	sink := &countingSink{}
	baseSnap := &testSnapshotter{snapshot: []byte("snap")}
	policySnap := &policySnapshotter{testSnapshotter: baseSnap, mode: "incremental", fullEvery: 100}

	execute := func(b types.Batch) (types.Batch, error) { return b, nil }
	if err := RunPipeline(ctx, testutil.NewSliceSource(batches), sink, execute, w, policySnap, 1); err != nil {
		t.Fatalf("RunPipeline(incremental mode): %v", err)
	}

	latest, err := w.LoadLatestCheckpoint(ctx)
	if err != nil {
		t.Fatalf("LoadLatestCheckpoint: %v", err)
	}
	if latest == nil {
		t.Fatalf("expected latest checkpoint")
	}
	if latest.Mode != "incremental" {
		t.Fatalf("expected latest checkpoint mode incremental, got %q", latest.Mode)
	}
	if latest.BaseSeq != 2 {
		t.Fatalf("expected chained incremental base seq=2, got %d", latest.BaseSeq)
	}

	recoverySnap := &policySnapshotter{testSnapshotter: &testSnapshotter{}, mode: "incremental", fullEvery: 2}
	replayExecCalls := 0
	if err := RunPipeline(ctx, testutil.NewSliceSource(nil), &countingSink{}, func(b types.Batch) (types.Batch, error) {
		replayExecCalls++
		return b, nil
	}, w, recoverySnap, 0); err != nil {
		t.Fatalf("RunPipeline(recovery): %v", err)
	}

	if recoverySnap.RestoreCalls() != 1 {
		t.Fatalf("expected one snapshot restore from latest full checkpoint, got %d", recoverySnap.RestoreCalls())
	}
	if replayExecCalls != 0 {
		t.Fatalf("expected no suffix replay call after delta restore, got %d", replayExecCalls)
	}
}

func TestRunPipeline_WAL_Checkpoint_IncrementalMode_AutoCompactsLongChainToFull(t *testing.T) {
	tmp := t.TempDir()
	dbPath := filepath.Join(tmp, "wal.db")

	w, err := wal.NewSQLiteWAL(dbPath)
	if err != nil {
		t.Fatalf("NewSQLiteWAL: %v", err)
	}
	defer w.Close()

	ctx := context.Background()
	batches := make([]types.Batch, 12)
	for i := range batches {
		batches[i] = types.Batch{{Tuple: types.Tuple{"id": int64(i + 1)}, Count: 1}}
	}

	policySnap := &policySnapshotter{
		testSnapshotter: &testSnapshotter{snapshot: []byte("snap")},
		mode:            "incremental",
		fullEvery:       1000,
	}

	if err := RunPipeline(ctx, testutil.NewSliceSource(batches), &countingSink{}, func(b types.Batch) (types.Batch, error) {
		return b, nil
	}, w, policySnap, 1); err != nil {
		t.Fatalf("RunPipeline: %v", err)
	}

	latest, err := w.LoadLatestCheckpoint(ctx)
	if err != nil {
		t.Fatalf("LoadLatestCheckpoint: %v", err)
	}
	if latest == nil {
		t.Fatalf("expected latest checkpoint")
	}
	full, err := w.LoadLatestFullCheckpointBefore(ctx, latest.LastSeq)
	if err != nil {
		t.Fatalf("LoadLatestFullCheckpointBefore: %v", err)
	}
	if full == nil {
		t.Fatalf("expected at least one full checkpoint after auto compaction")
	}
	if full.LastSeq < 10 {
		t.Fatalf("expected auto-generated full checkpoint near chain threshold, got seq=%d", full.LastSeq)
	}
}

func TestRunPipeline_WAL_CheckpointSaveFailure_RollsBackDrainedMutations(t *testing.T) {
	tmp := t.TempDir()
	dbPath := filepath.Join(tmp, "wal.db")

	innerWAL, err := wal.NewSQLiteWAL(dbPath)
	if err != nil {
		t.Fatalf("NewSQLiteWAL: %v", err)
	}
	defer innerWAL.Close()

	w := &failingCheckpointWAL{inner: *innerWAL, fail: true}

	snapshotter := &mutationPolicySnapshotter{
		policySnapshotter: &policySnapshotter{
			testSnapshotter: &testSnapshotter{snapshot: []byte("snap")},
			mode:            "incremental",
			fullEvery:       100,
		},
		mutations: []wal.CheckpointMutation{{Type: "put", Key: []byte("k1"), Value: []byte("v1")}},
	}

	err = RunPipeline(
		context.Background(),
		testutil.NewSliceSource([]types.Batch{{{Tuple: types.Tuple{"id": int64(1)}, Count: 1}}}),
		&countingSink{},
		func(b types.Batch) (types.Batch, error) { return b, nil },
		w,
		snapshotter,
		1,
	)
	if err == nil {
		t.Fatalf("expected checkpoint save failure")
	}

	if snapshotter.RollbackCount() != 1 {
		t.Fatalf("expected rollback callback once, got %d", snapshotter.RollbackCount())
	}
	rolledBack := snapshotter.RolledBackMutations()
	if len(rolledBack) != 1 {
		t.Fatalf("expected 1 rolled back mutation, got %d", len(rolledBack))
	}
	if rolledBack[0].Type != "put" || string(rolledBack[0].Key) != "k1" {
		t.Fatalf("unexpected rolled back mutation: %+v", rolledBack[0])
	}
}

func TestRunPipeline_WAL_CheckpointBoundary_ExecuteFailure_DoesNotAdvanceCheckpoint(t *testing.T) {
	tmp := t.TempDir()
	dbPath := filepath.Join(tmp, "wal.db")

	w, err := wal.NewSQLiteWAL(dbPath)
	if err != nil {
		t.Fatalf("NewSQLiteWAL: %v", err)
	}
	defer w.Close()

	snap := &testSnapshotter{snapshot: []byte("snap")}
	err = RunPipeline(
		context.Background(),
		testutil.NewSliceSource([]types.Batch{{{Tuple: types.Tuple{"id": int64(1)}, Count: 1}}}),
		&countingSink{},
		func(types.Batch) (types.Batch, error) { return nil, fmt.Errorf("injected execute failure") },
		w,
		snap,
		1,
	)
	if err == nil {
		t.Fatalf("expected execute failure")
	}

	cp, err := w.LoadLatestCheckpoint(context.Background())
	if err != nil {
		t.Fatalf("LoadLatestCheckpoint: %v", err)
	}
	if cp != nil {
		t.Fatalf("expected no checkpoint persisted on execute failure, got %+v", cp)
	}

	replayExecCalls := 0
	if err := RunPipeline(context.Background(), testutil.NewSliceSource(nil), &countingSink{}, func(b types.Batch) (types.Batch, error) {
		replayExecCalls++
		return b, nil
	}, w, nil, 0); err != nil {
		t.Fatalf("RunPipeline(replay): %v", err)
	}
	if replayExecCalls != 1 {
		t.Fatalf("expected one replayed batch after execute failure, got %d", replayExecCalls)
	}
}

func TestRunPipeline_WAL_CheckpointBoundary_SinkFailure_DoesNotAdvanceCheckpoint(t *testing.T) {
	tmp := t.TempDir()
	dbPath := filepath.Join(tmp, "wal.db")

	w, err := wal.NewSQLiteWAL(dbPath)
	if err != nil {
		t.Fatalf("NewSQLiteWAL: %v", err)
	}
	defer w.Close()

	sink := &failOnceSink{}
	err = RunPipeline(
		context.Background(),
		testutil.NewSliceSource([]types.Batch{{{Tuple: types.Tuple{"id": int64(1)}, Count: 1}}}),
		sink,
		func(b types.Batch) (types.Batch, error) { return b, nil },
		w,
		&testSnapshotter{snapshot: []byte("snap")},
		1,
	)
	if err == nil {
		t.Fatalf("expected sink failure")
	}

	cp, err := w.LoadLatestCheckpoint(context.Background())
	if err != nil {
		t.Fatalf("LoadLatestCheckpoint: %v", err)
	}
	if cp != nil {
		t.Fatalf("expected no checkpoint persisted on sink failure, got %+v", cp)
	}

	replayExecCalls := 0
	if err := RunPipeline(context.Background(), testutil.NewSliceSource(nil), &countingSink{}, func(b types.Batch) (types.Batch, error) {
		replayExecCalls++
		return b, nil
	}, w, nil, 0); err != nil {
		t.Fatalf("RunPipeline(replay): %v", err)
	}
	if replayExecCalls != 1 {
		t.Fatalf("expected one replayed batch after sink failure, got %d", replayExecCalls)
	}
}

func TestRunPipeline_WAL_Checkpoint_IncrementalMode_ForceFullOnLargeMutationPayload(t *testing.T) {
	tmp := t.TempDir()
	dbPath := filepath.Join(tmp, "wal.db")

	w, err := wal.NewSQLiteWAL(dbPath)
	if err != nil {
		t.Fatalf("NewSQLiteWAL: %v", err)
	}
	defer w.Close()

	largeValue := make([]byte, 128)
	for index := range largeValue {
		largeValue[index] = 'x'
	}

	snapshotter := &mutationPolicySnapshotter{
		policySnapshotter: &policySnapshotter{
			testSnapshotter:  &testSnapshotter{snapshot: []byte("snap")},
			mode:             "incremental",
			fullEvery:        100,
			maxMutationBytes: 64,
		},
		mutations: []wal.CheckpointMutation{{Type: "put", Key: []byte("k1"), Value: largeValue}},
	}

	err = RunPipeline(
		context.Background(),
		testutil.NewSliceSource([]types.Batch{{{Tuple: types.Tuple{"id": int64(1)}, Count: 1}}}),
		&countingSink{},
		func(b types.Batch) (types.Batch, error) { return b, nil },
		w,
		snapshotter,
		1,
	)
	if err != nil {
		t.Fatalf("RunPipeline: %v", err)
	}

	latest, err := w.LoadLatestCheckpoint(context.Background())
	if err != nil {
		t.Fatalf("LoadLatestCheckpoint: %v", err)
	}
	if latest == nil {
		t.Fatalf("expected latest checkpoint")
	}
	if latest.Mode != "full" {
		t.Fatalf("expected forced full checkpoint, got mode=%q", latest.Mode)
	}
}
