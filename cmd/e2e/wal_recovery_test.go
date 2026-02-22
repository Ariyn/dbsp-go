package e2e

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/ariyn/dbsp/internal/dbsp/testutil"
	"github.com/ariyn/dbsp/cmd/dbsp/pipeline"
	"github.com/ariyn/dbsp/internal/dbsp/op"
	sqlconv "github.com/ariyn/dbsp/internal/dbsp/sql"
	"github.com/ariyn/dbsp/internal/dbsp/types"
	"github.com/ariyn/dbsp/internal/dbsp/wal"
)

type aggState struct {
	sum   float64
	count int64
}

func accumulateDeltas(batches []types.Batch) map[string]aggState {
	state := make(map[string]aggState)
	for _, batch := range batches {
		for _, td := range batch {
			k, _ := td.Tuple["k"].(string)
			if k == "" {
				continue
			}
			s := state[k]
			agd, _ := td.Tuple["agg_delta"].(float64)
			cd, _ := td.Tuple["count_delta"].(int64)

			m := td.Count
			if m == 0 {
				m = 1
			}
			s.sum += agd * float64(m)
			s.count += cd * m
			state[k] = s
		}
	}
	return state
}

func assertStateEqual(t *testing.T, got, want map[string]aggState) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("state key count mismatch: got=%d want=%d", len(got), len(want))
	}
	for k, wv := range want {
		gv, ok := got[k]
		if !ok {
			t.Fatalf("missing key %q in got state", k)
		}
		if gv.sum != wv.sum || gv.count != wv.count {
			t.Fatalf("state mismatch for key %q: got(sum=%v,count=%v) want(sum=%v,count=%v)", k, gv.sum, gv.count, wv.sum, wv.count)
		}
	}
}

func TestWALRecovery(t *testing.T) {
	// Scenario:
	// 1. Process 3 batches with WAL and checkpointing.
	// 2. Shut down.
	// 3. Restart with another 3 batches.
	// 4. Verify the final result matches processing all 6 batches at once.

	query := "SELECT k, SUM(v), COUNT(id) FROM t GROUP BY k"
	batches := []types.Batch{
		{{Tuple: types.Tuple{"k": "A", "v": 10.0, "id": 1}, Count: 1}},
		{{Tuple: types.Tuple{"k": "A", "v": 20.0, "id": 2}, Count: 1}},
		{{Tuple: types.Tuple{"k": "B", "v": 5.0, "id": 3}, Count: 1}},
		{{Tuple: types.Tuple{"k": "A", "v": 20.0, "id": 2}, Count: -1}},
		{{Tuple: types.Tuple{"k": "B", "v": 7.0, "id": 4}, Count: 1}},
		{{Tuple: types.Tuple{"k": "A", "v": 10.0, "id": 1}, Count: -1}},
	}

	// 1. Baseline
	rootBase, _ := sqlconv.ParseQueryToIncrementalDBSP(query)
	baseSink := testutil.NewRecordingSink()
	pipeline.RunPipeline(context.Background(), testutil.NewSliceSource(batches), baseSink, func(b types.Batch) (types.Batch, error) {
		return op.Execute(rootBase, b)
	}, nil, nil, 0)
	baselineState := accumulateDeltas(baseSink.Batches)

	// 2. Run 1 (with checkpoint at 3)
	tmp := t.TempDir()
	dbPath := filepath.Join(tmp, "e2e_wal.db")
	w1, _ := wal.NewSQLiteWAL(dbPath)
	root1, _ := sqlconv.ParseQueryToIncrementalDBSP(query)
	sink1 := testutil.NewRecordingSink()
	
	pipeline.RunPipeline(context.Background(), testutil.NewSliceSource(batches[:4]), sink1, func(b types.Batch) (types.Batch, error) {
		return op.Execute(root1, b)
	}, w1, pipeline.PipelineSnapshotterFunc{
		SnapFunc:    func() ([]byte, error) { return op.SnapshotGraph(root1) },
		RestoreFunc: func(b []byte) error { return op.RestoreGraph(root1, b) },
	}, 3)
	w1.Close()

	// 3. Run 2 (Restart and process rest)
	w2, _ := wal.NewSQLiteWAL(dbPath)
	defer w2.Close()
	root2, _ := sqlconv.ParseQueryToIncrementalDBSP(query)
	sink2 := testutil.NewRecordingSink()

	pipeline.RunPipeline(context.Background(), testutil.NewSliceSource(batches[4:]), sink2, func(b types.Batch) (types.Batch, error) {
		return op.Execute(root2, b)
	}, w2, pipeline.PipelineSnapshotterFunc{
		SnapFunc:    func() ([]byte, error) { return op.SnapshotGraph(root2) },
		RestoreFunc: func(b []byte) error { return op.RestoreGraph(root2, b) },
	}, 3)

	// 4. Verification
	finalBatches := append(sink1.Batches, sink2.Batches...)
	finalState := accumulateDeltas(finalBatches)
	assertStateEqual(t, finalState, baselineState)
}
