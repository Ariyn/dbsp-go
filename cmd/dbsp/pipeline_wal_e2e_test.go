package main

import (
	"context"
	"path/filepath"
	"sort"
	"testing"

	"github.com/ariyn/dbsp/internal/dbsp/op"
	sqlconv "github.com/ariyn/dbsp/internal/dbsp/sql"
	"github.com/ariyn/dbsp/internal/dbsp/types"
	"github.com/ariyn/dbsp/internal/dbsp/wal"
)

type aggSnapshot struct {
	sum   float64
	count int64
}

func applyAggDeltas(out types.Batch, snap map[string]aggSnapshot) {
	if out == nil {
		return
	}
	for _, td := range out {
		k, _ := td.Tuple["k"].(string)
		if k == "" {
			// Ignore unexpected rows
			continue
		}
		prev := snap[k]

		agd, _ := td.Tuple["agg_delta"].(float64)
		cd, _ := td.Tuple["count_delta"].(int64)

		m := td.Count
		if m == 0 {
			m = 1
		}
		prev.sum += agd * float64(m)
		prev.count += cd * m
		snap[k] = prev
	}
}

func snapshotFromSink(s *recordingSink) map[string]aggSnapshot {
	out := make(map[string]aggSnapshot)
	for _, b := range s.batches {
		applyAggDeltas(b, out)
	}
	return out
}

func assertAggSnapshotsEqual(t *testing.T, got, want map[string]aggSnapshot) {
	t.Helper()

	keys := make([]string, 0, len(want))
	for k := range want {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// Ensure got doesn't have unexpected extra keys.
	if len(got) != len(want) {
		t.Fatalf("snapshot key count mismatch: got=%d want=%d (gotKeys=%v wantKeys=%v)", len(got), len(want), mapKeys(got), mapKeys(want))
	}

	for _, k := range keys {
		gv, ok := got[k]
		if !ok {
			t.Fatalf("missing key %q in got snapshot (gotKeys=%v)", k, mapKeys(got))
		}
		wv := want[k]
		if gv.sum != wv.sum || gv.count != wv.count {
			t.Fatalf("snapshot mismatch for key %q: got(sum=%v,count=%v) want(sum=%v,count=%v)", k, gv.sum, gv.count, wv.sum, wv.count)
		}
	}
}

func mapKeys(m map[string]aggSnapshot) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func TestE2E_WALCheckpointRestore_FinalSnapshotMatchesBaseline_GroupAgg(t *testing.T) {
	// Goal: WAL + checkpoint/restore + suffix replay가 있어도, "최종 스냅샷(델타 누적)"은 baseline과 동일해야 한다.
	// Note: runPipeline은 replay 출력이 sink로 가지 않도록 설계되어 있으므로,
	//  - 1차 실행의 live 출력 + 2차 실행의 live 출력만 누적하여 최종 스냅샷을 복원한다.
	//  - replay(복구) 과정에서 sink에 중복 출력이 발생하지 않는 것도 함께 검증한다.

	query := "SELECT k, SUM(v), COUNT(id) FROM t GROUP BY k"

	batches := []types.Batch{
		{{Tuple: types.Tuple{"k": "A", "v": 10.0, "id": int64(1)}, Count: 1}},
		{{Tuple: types.Tuple{"k": "A", "v": 20.0, "id": int64(2)}, Count: 1}},
		{{Tuple: types.Tuple{"k": "B", "v": 5.0, "id": int64(3)}, Count: 1}},
		{{Tuple: types.Tuple{"k": "A", "v": 20.0, "id": int64(2)}, Count: -1}},
		{{Tuple: types.Tuple{"k": "B", "v": 7.0, "id": int64(4)}, Count: 1}},
		{{Tuple: types.Tuple{"k": "A", "v": 10.0, "id": int64(1)}, Count: -1}},
	}

	// 1) Baseline: WAL 없이 전체 배치 처리
	rootBase, err := sqlconv.ParseQueryToIncrementalDBSP(query)
	if err != nil {
		t.Fatalf("ParseQueryToIncrementalDBSP(baseline): %v", err)
	}
	baseSink := newRecordingSink()
	if err := runPipeline(context.Background(), newSliceSource(batches), baseSink, func(b types.Batch) (types.Batch, error) {
		return op.Execute(rootBase, b)
	}, nil, nil, 0); err != nil {
		t.Fatalf("runPipeline(baseline): %v", err)
	}
	baselineSnap := snapshotFromSink(baseSink)

	// 2) Recovery run: 1차 실행에서 일부 배치 처리 + checkpoint 저장
	tmp := t.TempDir()
	dbPath := filepath.Join(tmp, "wal.db")

	w1, err := wal.NewSQLiteWAL(dbPath)
	if err != nil {
		t.Fatalf("NewSQLiteWAL(run1): %v", err)
	}
	// Explicitly close before reopening on run2.

	root1, err := sqlconv.ParseQueryToIncrementalDBSP(query)
	if err != nil {
		t.Fatalf("ParseQueryToIncrementalDBSP(run1): %v", err)
	}
	firstSink := newRecordingSink()

	checkpointEvery := 3
	if err := runPipeline(context.Background(), newSliceSource(batches[:4]), firstSink, func(b types.Batch) (types.Batch, error) {
		return op.Execute(root1, b)
	}, w1,
		pipelineSnapshotterFunc{
			snap:    func() ([]byte, error) { return op.SnapshotGraph(root1) },
			restore: func(b []byte) error { return op.RestoreGraph(root1, b) },
		},
		checkpointEvery,
	); err != nil {
		t.Fatalf("runPipeline(run1): %v", err)
	}
	if err := w1.Close(); err != nil {
		t.Fatalf("w1.Close: %v", err)
	}

	// 3) 2차 실행: 새 그래프 인스턴스에서 checkpoint restore + suffix replay 후 남은 live 배치 처리
	w2, err := wal.NewSQLiteWAL(dbPath)
	if err != nil {
		t.Fatalf("NewSQLiteWAL(run2): %v", err)
	}
	defer w2.Close()

	root2, err := sqlconv.ParseQueryToIncrementalDBSP(query)
	if err != nil {
		t.Fatalf("ParseQueryToIncrementalDBSP(run2): %v", err)
	}
	secondSink := newRecordingSink()

	if err := runPipeline(context.Background(), newSliceSource(batches[4:]), secondSink, func(b types.Batch) (types.Batch, error) {
		return op.Execute(root2, b)
	}, w2,
		pipelineSnapshotterFunc{
			snap:    func() ([]byte, error) { return op.SnapshotGraph(root2) },
			restore: func(b []byte) error { return op.RestoreGraph(root2, b) },
		},
		checkpointEvery,
	); err != nil {
		t.Fatalf("runPipeline(run2): %v", err)
	}

	// replay 중 sink에 쓰지 않으므로, 2차 sink는 live 배치 수만큼만 기록되어야 한다.
	if got, want := len(secondSink.batches), len(batches[4:]); got != want {
		t.Fatalf("unexpected sink writes in run2: got=%d want=%d (replay should not write)", got, want)
	}

	// 4) 최종 스냅샷 비교: (1차 live 출력 + 2차 live 출력) 누적 == baseline 누적
	recoverySnap := snapshotFromSink(firstSink)
	applyAggDeltasFromSink(secondSink, recoverySnap)

	assertAggSnapshotsEqual(t, recoverySnap, baselineSnap)
}

func applyAggDeltasFromSink(s *recordingSink, snap map[string]aggSnapshot) {
	for _, b := range s.batches {
		applyAggDeltas(b, snap)
	}
}
