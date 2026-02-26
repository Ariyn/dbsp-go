package main

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/ariyn/dbsp/cmd/dbsp/config"
	"github.com/ariyn/dbsp/cmd/dbsp/pipeline"
	"github.com/ariyn/dbsp/internal/dbsp/op"
	"github.com/ariyn/dbsp/internal/dbsp/types"
	"github.com/ariyn/dbsp/internal/dbsp/wal"
)

type drainingMutationSnapshotter struct {
	pipeline.PipelineSnapshotterFunc
	mutations []wal.CheckpointMutation
}

func (s *drainingMutationSnapshotter) DrainCheckpointMutations() []wal.CheckpointMutation {
	out := append([]wal.CheckpointMutation(nil), s.mutations...)
	s.mutations = nil
	return out
}

type passthroughOp struct{}

func (passthroughOp) Apply(batch types.Batch) (types.Batch, error) { return batch, nil }

type failingOp struct{}

func (failingOp) Apply(types.Batch) (types.Batch, error) { return nil, fmt.Errorf("injected execute failure") }

type failOncePartitionSink struct {
	failed bool
}

func (s *failOncePartitionSink) WriteBatch(types.Batch) error {
	if !s.failed {
		s.failed = true
		return fmt.Errorf("injected sink failure")
	}
	return nil
}

func (s *failOncePartitionSink) Close() error { return nil }

func basePartitionConfig() *config.PipelineConfig {
	cfg := &config.PipelineConfig{}
	cfg.Pipeline.WAL.Enabled = true
	cfg.Pipeline.WAL.CheckpointEveryBatches = 1
	cfg.Pipeline.State.CheckpointMode = "incremental"
	cfg.Pipeline.State.CheckpointEveryBatches = 10
	return cfg
}

func newPartitionRuntimeForTest(t *testing.T, root *op.Node) (*partitionRuntime, *wal.SQLiteWAL) {
	t.Helper()
	w, err := wal.NewSQLiteWAL(filepath.Join(t.TempDir(), "wal.db"))
	if err != nil {
		t.Fatalf("NewSQLiteWAL: %v", err)
	}
	rt := &partitionRuntime{
		values:   map[string]string{"region": "ap"},
		rootNode: root,
		sink:     &closeCountingSink{},
		wal:      w,
		snapshotter: pipeline.PipelineSnapshotterFunc{
			SnapFunc:    func() ([]byte, error) { return []byte("snap"), nil },
			RestoreFunc: func([]byte) error { return nil },
			Mode:        "incremental",
		},
	}
	return rt, w
}

func countReplayRows(t *testing.T, w *wal.SQLiteWAL) int {
	t.Helper()
	count := 0
	if err := w.Replay(context.Background(), func(types.Batch) error {
		count++
		return nil
	}); err != nil {
		t.Fatalf("Replay: %v", err)
	}
	return count
}

func TestRunPartitionBatch_ExecuteFailure_DoesNotCreateCheckpoint(t *testing.T) {
	cfg := basePartitionConfig()
	rt, w := newPartitionRuntimeForTest(t, &op.Node{Op: failingOp{}})
	defer w.Close()

	err := runPartitionBatch(context.Background(), cfg, rt, types.Batch{{Tuple: types.Tuple{"id": int64(1)}, Count: 1}})
	if err == nil {
		t.Fatalf("expected execute failure")
	}

	cp, err := w.LoadLatestCheckpoint(context.Background())
	if err != nil {
		t.Fatalf("LoadLatestCheckpoint: %v", err)
	}
	if cp != nil {
		t.Fatalf("expected no checkpoint after execute failure, got %+v", cp)
	}

	if got := countReplayRows(t, w); got != 1 {
		t.Fatalf("expected 1 WAL row after execute failure, got %d", got)
	}
}

func TestRunPartitionBatch_SinkFailure_DoesNotCreateCheckpoint(t *testing.T) {
	cfg := basePartitionConfig()
	rt, w := newPartitionRuntimeForTest(t, &op.Node{Op: passthroughOp{}})
	defer w.Close()
	rt.sink = &failOncePartitionSink{}

	err := runPartitionBatch(context.Background(), cfg, rt, types.Batch{{Tuple: types.Tuple{"id": int64(1)}, Count: 1}})
	if err == nil {
		t.Fatalf("expected sink failure")
	}

	cp, err := w.LoadLatestCheckpoint(context.Background())
	if err != nil {
		t.Fatalf("LoadLatestCheckpoint: %v", err)
	}
	if cp != nil {
		t.Fatalf("expected no checkpoint after sink failure, got %+v", cp)
	}

	if got := countReplayRows(t, w); got != 1 {
		t.Fatalf("expected 1 WAL row after sink failure, got %d", got)
	}
}

func TestRunPartitionBatch_IncrementalMode_ForceFullOnLargeMutationPayload(t *testing.T) {
	cfg := basePartitionConfig()
	cfg.Pipeline.State.MaxIncrementalMutationBytes = 64
	rt, w := newPartitionRuntimeForTest(t, &op.Node{Op: passthroughOp{}})
	defer w.Close()

	largeValue := make([]byte, 128)
	for index := range largeValue {
		largeValue[index] = 'm'
	}

	rt.snapshotter = &drainingMutationSnapshotter{
		PipelineSnapshotterFunc: pipeline.PipelineSnapshotterFunc{
			SnapFunc:    func() ([]byte, error) { return []byte("snap"), nil },
			RestoreFunc: func([]byte) error { return nil },
			Mode:        "incremental",
		},
		mutations: []wal.CheckpointMutation{{Type: "put", Key: []byte("k1"), Value: largeValue}},
	}

	if err := runPartitionBatch(context.Background(), cfg, rt, types.Batch{{Tuple: types.Tuple{"id": int64(1)}, Count: 1}}); err != nil {
		t.Fatalf("runPartitionBatch: %v", err)
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
