package op

import (
	"fmt"
	"testing"
	"time"

	"github.com/ariyn/dbsp/internal/dbsp/types"
)

func TestOrderedWindowOpEmitsBasicLag(t *testing.T) {
	op := NewOrderedWindowOp(func(t types.Tuple) any { return t["id"] }, "ts", "v", 1, "v_last")

	out, err := op.Apply(types.Batch{
		{Tuple: types.Tuple{"id": "a", "ts": int64(1), "v": 10.0}, Count: 1},
		{Tuple: types.Tuple{"id": "a", "ts": int64(2), "v": 20.0}, Count: 1},
		{Tuple: types.Tuple{"id": "a", "ts": int64(3), "v": 30.0}, Count: 1},
	})
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if len(out) != 3 {
		t.Fatalf("expected 3 output rows, got %d", len(out))
	}

	seen := make(map[int64]any)
	for _, td := range out {
		ts, ok := td.Tuple["ts"].(int64)
		if !ok {
			t.Fatalf("expected ts int64, got %T", td.Tuple["ts"])
		}
		seen[ts] = td.Tuple["v_last"]
	}

	if v, ok := seen[int64(1)]; !ok || v != nil {
		t.Fatalf("expected ts=1 lag nil, got %v", v)
	}
	if v, ok := seen[int64(2)]; !ok || types.ToFloat64(v) != 10.0 {
		t.Fatalf("expected ts=2 lag 10.0, got %v", v)
	}
	if v, ok := seen[int64(3)]; !ok || types.ToFloat64(v) != 20.0 {
		t.Fatalf("expected ts=3 lag 20.0, got %v", v)
	}
}

func TestOrderedWindowOpOutOfOrderInsertRecomputesAffectedRows(t *testing.T) {
	op := NewOrderedWindowOp(func(t types.Tuple) any { return t["id"] }, "ts", "v", 1, "v_last")
	if _, err := op.Apply(types.Batch{
		{Tuple: types.Tuple{"id": "a", "ts": int64(1), "v": 10.0}, Count: 1},
		{Tuple: types.Tuple{"id": "a", "ts": int64(3), "v": 30.0}, Count: 1},
	}); err != nil {
		t.Fatalf("apply initial: %v", err)
	}

	out, err := op.Apply(types.Batch{{Tuple: types.Tuple{"id": "a", "ts": int64(2), "v": 20.0}, Count: 1}})
	if err != nil {
		t.Fatalf("apply insert: %v", err)
	}
	if len(out) != 3 {
		t.Fatalf("expected 3 output deltas, got %d (%v)", len(out), out)
	}

	type observed struct {
		count int64
		lag   any
	}
	seen := map[int64][]observed{}
	for _, td := range out {
		ts, ok := td.Tuple["ts"].(int64)
		if !ok {
			t.Fatalf("expected ts int64, got %T", td.Tuple["ts"])
		}
		seen[ts] = append(seen[ts], observed{count: td.Count, lag: td.Tuple["v_last"]})
	}

	if got := seen[int64(2)]; len(got) != 1 || got[0].count != 1 || types.ToFloat64(got[0].lag) != 10.0 {
		t.Fatalf("expected ts=2 +1 lag 10.0, got %v", got)
	}

	var removedOld, addedNew bool
	for _, item := range seen[int64(3)] {
		switch {
		case item.count == -1 && types.ToFloat64(item.lag) == 10.0:
			removedOld = true
		case item.count == 1 && types.ToFloat64(item.lag) == 20.0:
			addedNew = true
		}
	}
	if !removedOld || !addedNew {
		t.Fatalf("expected ts=3 to emit -1 lag 10.0 and +1 lag 20.0, got %v", seen[int64(3)])
	}
}

func TestOrderedWindowOpDeleteRetractsRemovedRow(t *testing.T) {
	op := NewOrderedWindowOp(func(t types.Tuple) any { return t["id"] }, "ts", "v", 1, "v_last")
	if _, err := op.Apply(types.Batch{
		{Tuple: types.Tuple{"id": "a", "ts": int64(1), "v": 10.0}, Count: 1},
		{Tuple: types.Tuple{"id": "a", "ts": int64(2), "v": 20.0}, Count: 1},
	}); err != nil {
		t.Fatalf("apply initial: %v", err)
	}

	out, err := op.Apply(types.Batch{{Tuple: types.Tuple{"id": "a", "ts": int64(2), "v": 20.0}, Count: -1}})
	if err != nil {
		t.Fatalf("apply delete: %v", err)
	}
	if len(out) != 1 {
		t.Fatalf("expected 1 retraction, got %d (%v)", len(out), out)
	}
	if out[0].Count != -1 || out[0].Tuple["ts"].(int64) != 2 || types.ToFloat64(out[0].Tuple["v_last"]) != 10.0 {
		t.Fatalf("unexpected delete output: %v", out)
	}
}

func TestOrderedWindowOpDeleteNonExistentRowFails(t *testing.T) {
	op := NewOrderedWindowOp(func(t types.Tuple) any { return t["id"] }, "ts", "v", 1, "v_last")
	_, err := op.Apply(types.Batch{{Tuple: types.Tuple{"id": "a", "ts": int64(2), "v": 20.0}, Count: -1}})
	if err == nil {
		t.Fatal("expected deleting a non-existent row to fail")
	}
}

func TestOrderedWindowOpOffsetTwoRecomputesTwoSuccessors(t *testing.T) {
	op := NewOrderedWindowOp(func(t types.Tuple) any { return t["id"] }, "ts", "v", 2, "v_last")
	if _, err := op.Apply(types.Batch{
		{Tuple: types.Tuple{"id": "a", "ts": int64(10), "v": 10.0}, Count: 1},
		{Tuple: types.Tuple{"id": "a", "ts": int64(30), "v": 30.0}, Count: 1},
		{Tuple: types.Tuple{"id": "a", "ts": int64(40), "v": 40.0}, Count: 1},
	}); err != nil {
		t.Fatalf("apply initial: %v", err)
	}

	out, err := op.Apply(types.Batch{{Tuple: types.Tuple{"id": "a", "ts": int64(20), "v": 20.0}, Count: 1}})
	if err != nil {
		t.Fatalf("apply insert: %v", err)
	}

	changes := map[int64][]types.TupleDelta{}
	for _, td := range out {
		changes[td.Tuple["ts"].(int64)] = append(changes[td.Tuple["ts"].(int64)], td)
	}
	if len(changes[20]) != 1 || changes[20][0].Tuple["v_last"] != nil {
		t.Fatalf("expected new row ts=20 with nil lag, got %v", changes[20])
	}
	if len(changes[30]) != 2 {
		t.Fatalf("expected ts=30 replacement deltas, got %v", changes[30])
	}
	if len(changes[40]) != 2 {
		t.Fatalf("expected ts=40 replacement deltas, got %v", changes[40])
	}
}

func TestOrderedWindowOpOffsetTwoInsertKeepsPriorContextOutsideBand(t *testing.T) {
	op := NewOrderedWindowOp(func(t types.Tuple) any { return t["id"] }, "ts", "v", 2, "v_last")
	if _, err := op.Apply(types.Batch{
		{Tuple: types.Tuple{"id": "a", "ts": int64(10), "v": 10.0}, Count: 1},
		{Tuple: types.Tuple{"id": "a", "ts": int64(20), "v": 20.0}, Count: 1},
		{Tuple: types.Tuple{"id": "a", "ts": int64(40), "v": 40.0}, Count: 1},
		{Tuple: types.Tuple{"id": "a", "ts": int64(50), "v": 50.0}, Count: 1},
	}); err != nil {
		t.Fatalf("apply initial: %v", err)
	}

	out, err := op.Apply(types.Batch{{Tuple: types.Tuple{"id": "a", "ts": int64(30), "v": 30.0}, Count: 1}})
	if err != nil {
		t.Fatalf("apply insert: %v", err)
	}
	if len(out) != 5 {
		t.Fatalf("expected 5 output deltas, got %d (%v)", len(out), out)
	}

	changes := map[int64][]types.TupleDelta{}
	for _, td := range out {
		changes[td.Tuple["ts"].(int64)] = append(changes[td.Tuple["ts"].(int64)], td)
	}

	if len(changes[30]) != 1 || types.ToFloat64(changes[30][0].Tuple["v_last"]) != 10.0 {
		t.Fatalf("expected ts=30 to emit +1 lag 10.0, got %v", changes[30])
	}

	var ts40Removed, ts40Added bool
	for _, td := range changes[40] {
		switch {
		case td.Count == -1 && types.ToFloat64(td.Tuple["v_last"]) == 10.0:
			ts40Removed = true
		case td.Count == 1 && types.ToFloat64(td.Tuple["v_last"]) == 20.0:
			ts40Added = true
		}
	}
	if !ts40Removed || !ts40Added {
		t.Fatalf("expected ts=40 to switch lag 10.0 -> 20.0, got %v", changes[40])
	}

	var ts50Removed, ts50Added bool
	for _, td := range changes[50] {
		switch {
		case td.Count == -1 && types.ToFloat64(td.Tuple["v_last"]) == 20.0:
			ts50Removed = true
		case td.Count == 1 && types.ToFloat64(td.Tuple["v_last"]) == 30.0:
			ts50Added = true
		}
	}
	if !ts50Removed || !ts50Added {
		t.Fatalf("expected ts=50 to switch lag 20.0 -> 30.0, got %v", changes[50])
	}
}

func TestOrderedWindowOpAppendInsertEmitsOnlyNewRow(t *testing.T) {
	op := NewOrderedWindowOp(func(t types.Tuple) any { return t["id"] }, "ts", "v", 2, "v_last")
	if _, err := op.Apply(types.Batch{
		{Tuple: types.Tuple{"id": "a", "ts": int64(10), "v": 10.0}, Count: 1},
		{Tuple: types.Tuple{"id": "a", "ts": int64(20), "v": 20.0}, Count: 1},
		{Tuple: types.Tuple{"id": "a", "ts": int64(30), "v": 30.0}, Count: 1},
	}); err != nil {
		t.Fatalf("apply initial: %v", err)
	}

	out, err := op.Apply(types.Batch{{Tuple: types.Tuple{"id": "a", "ts": int64(40), "v": 40.0}, Count: 1}})
	if err != nil {
		t.Fatalf("apply append insert: %v", err)
	}
	if len(out) != 1 {
		t.Fatalf("expected only 1 output delta for append insert, got %d (%v)", len(out), out)
	}
	if out[0].Count != 1 || out[0].Tuple["ts"].(int64) != 40 || types.ToFloat64(out[0].Tuple["v_last"]) != 20.0 {
		t.Fatalf("unexpected append output: %v", out[0])
	}
}

func TestOrderedWindowOpTiedOrderIsDeterministic(t *testing.T) {
	makeOp := func() *OrderedWindowOp {
		return NewOrderedWindowOp(func(t types.Tuple) any { return t["id"] }, "ts", "v", 1, "v_last")
	}

	run := func(rows types.Batch) map[string]types.Tuple {
		op := makeOp()
		out, err := op.Apply(rows)
		if err != nil {
			t.Fatalf("apply: %v", err)
		}
		result := map[string]types.Tuple{}
		for _, td := range out {
			result[td.Tuple["name"].(string)] = td.Tuple
		}
		return result
	}

	rowsA := types.Batch{
		{Tuple: types.Tuple{"id": "a", "name": "left", "ts": int64(1), "v": 10.0}, Count: 1},
		{Tuple: types.Tuple{"id": "a", "name": "right", "ts": int64(1), "v": 20.0}, Count: 1},
	}
	rowsB := types.Batch{
		{Tuple: types.Tuple{"id": "a", "name": "right", "ts": int64(1), "v": 20.0}, Count: 1},
		{Tuple: types.Tuple{"id": "a", "name": "left", "ts": int64(1), "v": 10.0}, Count: 1},
	}

	gotA := run(rowsA)
	gotB := run(rowsB)
	if !types.TuplesEqual(gotA["left"], gotB["left"]) || !types.TuplesEqual(gotA["right"], gotB["right"]) {
		t.Fatalf("expected deterministic outputs for tied order rows, got A=%v B=%v", gotA, gotB)
	}
}

func TestOrderedWindowOpDuplicateRowsHaveStableLagChain(t *testing.T) {
	op := NewOrderedWindowOp(func(t types.Tuple) any { return t["id"] }, "ts", "v", 1, "v_last")
	out, err := op.Apply(types.Batch{
		{Tuple: types.Tuple{"id": "a", "name": "first", "ts": int64(1), "v": 10.0}, Count: 1},
		{Tuple: types.Tuple{"id": "a", "name": "dup", "ts": int64(1), "v": 10.0}, Count: 1},
		{Tuple: types.Tuple{"id": "a", "name": "third", "ts": int64(2), "v": 30.0}, Count: 1},
	})
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	seen := map[string]any{}
	for _, td := range out {
		if td.Count < 0 {
			continue
		}
		seen[td.Tuple["name"].(string)] = td.Tuple["v_last"]
	}
	if types.ToFloat64(seen["first"]) != 10.0 {
		t.Fatalf("expected first row to be deterministically ordered after dup, got %v", seen["first"])
	}
	if seen["dup"] != nil {
		t.Fatalf("expected dup row to become deterministic first peer with nil lag, got %v", seen["dup"])
	}
	if types.ToFloat64(seen["third"]) != 10.0 {
		t.Fatalf("expected successor lag 10.0, got %v", seen["third"])
	}
	// Delete one duplicate and ensure the successor chain is recomputed.
	out, err = op.Apply(types.Batch{{Tuple: types.Tuple{"id": "a", "name": "first", "ts": int64(1), "v": 10.0}, Count: -1}})
	if err != nil {
		t.Fatalf("delete duplicate: %v", err)
	}
	if len(out) == 0 {
		t.Fatal("expected duplicate delete to emit recomputation deltas")
	}
	final := materializeOrderedSnapshot(out, "name")
	if row, ok := final["third"]; ok && types.ToFloat64(row["v_last"]) != 10.0 {
		t.Fatalf("expected third row lag to remain 10.0 after deleting one duplicate, got %v", row)
	}
}

func TestOrderedWindowOpStateBackendReload(t *testing.T) {
	backend := NewMemoryStateBackend()
	op1 := NewOrderedWindowOp(func(t types.Tuple) any { return t["id"] }, "ts", "v", 1, "v_last")
	op1.SetStateBackend(backend, "orderedwindow/test")

	if _, err := op1.Apply(types.Batch{
		{Tuple: types.Tuple{"id": "a", "ts": int64(1), "v": 10.0}, Count: 1},
		{Tuple: types.Tuple{"id": "a", "ts": int64(3), "v": 30.0}, Count: 1},
	}); err != nil {
		t.Fatalf("apply initial: %v", err)
	}

	op2 := NewOrderedWindowOp(func(t types.Tuple) any { return t["id"] }, "ts", "v", 1, "v_last")
	op2.SetStateBackend(backend, "orderedwindow/test")
	out, err := op2.Apply(types.Batch{{Tuple: types.Tuple{"id": "a", "ts": int64(2), "v": 20.0}, Count: 1}})
	if err != nil {
		t.Fatalf("apply reload: %v", err)
	}
	if len(out) != 3 {
		t.Fatalf("expected 3 output deltas after reload, got %d (%v)", len(out), out)
	}
}

func TestOrderedWindowOpSnapshotRestoreGraph(t *testing.T) {
	root := &Node{Op: NewOrderedWindowOp(func(t types.Tuple) any { return t["id"] }, "ts", "v", 1, "v_last")}
	initial := types.Batch{
		{Tuple: types.Tuple{"id": "a", "ts": int64(1), "v": 10.0}, Count: 1},
		{Tuple: types.Tuple{"id": "a", "ts": int64(3), "v": 30.0}, Count: 1},
	}
	if _, err := Execute(root, initial); err != nil {
		t.Fatalf("execute initial: %v", err)
	}

	snap, err := SnapshotGraph(root)
	if err != nil {
		t.Fatalf("snapshot graph: %v", err)
	}

	restored := &Node{Op: NewOrderedWindowOp(func(t types.Tuple) any { return t["id"] }, "ts", "v", 1, "v_last")}
	if err := RestoreGraph(restored, snap); err != nil {
		t.Fatalf("restore graph: %v", err)
	}

	out, err := Execute(restored, types.Batch{{Tuple: types.Tuple{"id": "a", "ts": int64(2), "v": 20.0}, Count: 1}})
	if err != nil {
		t.Fatalf("execute restored: %v", err)
	}
	if len(out) != 3 {
		t.Fatalf("expected 3 output deltas after restore, got %d (%v)", len(out), out)
	}

	snapshot := materializeOrderedSnapshot(out, "ts")
	row, ok := snapshot["3"]
	if !ok || types.ToFloat64(row["v_last"]) != 20.0 {
		t.Fatalf("expected restored ts=3 row with v_last=20.0, got %v", snapshot)
	}
}

func TestOrderedWindowOpStateTTL(t *testing.T) {
	op := NewOrderedWindowOp(func(t types.Tuple) any { return t["id"] }, "ts", "v", 1, "v_last")
	op.SetStateTTL(2 * time.Millisecond)
	if _, err := op.Apply(types.Batch{{Tuple: types.Tuple{"id": "a", "ts": int64(1), "v": 10.0}, Count: 1}}); err != nil {
		t.Fatalf("apply: %v", err)
	}
	if got := len(op.Partitions); got != 1 {
		t.Fatalf("expected 1 partition, got %d", got)
	}
	time.Sleep(5 * time.Millisecond)
	if _, err := op.Apply(nil); err != nil {
		t.Fatalf("apply for eviction: %v", err)
	}
	if got := len(op.Partitions); got != 0 {
		t.Fatalf("expected partitions to be evicted, got %d", got)
	}
}

func TestOrderedWindowOpPrunesExpiredRowsInHotPartition(t *testing.T) {
	op := NewOrderedWindowOp(func(t types.Tuple) any { return t["id"] }, "ts", "v", 1, "v_last")
	op.SetStateTTL(2 * time.Millisecond)
	if _, err := op.Apply(types.Batch{{Tuple: types.Tuple{"id": "a", "ts": int64(1), "v": 10.0}, Count: 1}}); err != nil {
		t.Fatalf("apply first: %v", err)
	}
	time.Sleep(5 * time.Millisecond)
	out, err := op.Apply(types.Batch{{Tuple: types.Tuple{"id": "a", "ts": int64(2), "v": 20.0}, Count: 1}})
	if err != nil {
		t.Fatalf("apply second: %v", err)
	}
	if len(out) != 1 {
		t.Fatalf("expected 1 output row, got %d (%v)", len(out), out)
	}
	if got := out[0].Tuple["v_last"]; got != nil {
		t.Fatalf("expected expired previous row to be pruned, got v_last=%v", got)
	}
	partition := op.Partitions["a"]
	if partition == nil || len(partition.Rows) != 1 {
		t.Fatalf("expected only latest row to remain after pruning, got %+v", partition)
	}
}

func materializeOrderedSnapshot(batch types.Batch, keyCol string) map[string]types.Tuple {
	snapshot := make(map[string]types.Tuple)
	for _, td := range batch {
		key := ""
		if td.Tuple != nil {
			key = orderedTupleKeyForCols(td.Tuple, keyCol)
		}
		if key == "" {
			continue
		}
		if td.Count < 0 {
			delete(snapshot, key)
			continue
		}
		snapshot[key] = types.CloneTuple(td.Tuple)
	}
	return snapshot
}

func orderedTupleKeyForCols(tuple types.Tuple, cols ...string) string {
	if tuple == nil || len(cols) == 0 {
		return ""
	}
	key := ""
	for idx, col := range cols {
		if idx > 0 {
			key += "|"
		}
		key += toString(tuple[col])
	}
	return key
}

func toString(v any) string {
	if v == nil {
		return ""
	}
	return fmt.Sprintf("%v", v)
}
