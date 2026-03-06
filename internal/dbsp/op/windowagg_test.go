package op

import (
	"testing"

	"github.com/ariyn/dbsp/internal/dbsp/types"
)

func TestWindowAggFrameValueModeReplacesRow(t *testing.T) {
	agg := &SumAgg{ColName: "v"}
	w := NewWindowAggOp(
		WindowSpecLite{},
		func(t types.Tuple) any { return t["id"] },
		[]string{"id"},
		func() any { return float64(0) },
		agg,
	)
	w.OrderByCol = "ts"
	w.FrameSpec = &FrameSpecLite{Type: "ROWS", StartType: "UNBOUNDED PRECEDING", EndType: "CURRENT ROW"}
	w.KeepInput = true
	w.EmitValue = true

	batch1 := types.Batch{{Tuple: types.Tuple{"id": "a", "ts": int64(1000), "v": 1.0}, Count: 1}}
	out, err := w.Apply(batch1)
	if err != nil {
		t.Fatalf("apply batch1: %v", err)
	}
	if len(out) != 1 {
		t.Fatalf("expected 1 output row, got %d", len(out))
	}
	if out[0].Count != 1 {
		t.Fatalf("expected count 1, got %d", out[0].Count)
	}
	if got := types.ToFloat64(out[0].Tuple["agg_result"]); got != 1.0 {
		t.Fatalf("expected agg_result 1.0, got %v", out[0].Tuple["agg_result"])
	}

	batch2 := types.Batch{
		{Tuple: types.Tuple{"id": "a", "ts": int64(1000), "v": 1.0}, Count: -1},
		{Tuple: types.Tuple{"id": "a", "ts": int64(1000), "v": 2.0}, Count: 1},
	}
	out, err = w.Apply(batch2)
	if err != nil {
		t.Fatalf("apply batch2: %v", err)
	}
	if len(out) != 2 {
		t.Fatalf("expected 2 output rows, got %d", len(out))
	}

	var minusOK, plusOK bool
	var minusVal, plusVal float64
	for _, td := range out {
		if td.Count == -1 {
			minusOK = true
			minusVal = types.ToFloat64(td.Tuple["agg_result"])
			if td.Tuple["id"] != "a" || td.Tuple["ts"] != int64(1000) {
				t.Fatalf("unexpected retraction key: %v", td.Tuple)
			}
		}
		if td.Count == 1 {
			plusOK = true
			plusVal = types.ToFloat64(td.Tuple["agg_result"])
			if td.Tuple["id"] != "a" || td.Tuple["ts"] != int64(1000) {
				t.Fatalf("unexpected insertion key: %v", td.Tuple)
			}
		}
	}

	if !minusOK || !plusOK {
		t.Fatalf("expected both retraction and insertion, got %+v", out)
	}
	if minusVal != 1.0 || plusVal != 2.0 {
		t.Fatalf("expected agg_result retraction 1.0 and insertion 2.0, got -%v +%v", minusVal, plusVal)
	}
}

func TestWindowAggCumulativeFrameAppendUsesPrefixSemantics(t *testing.T) {
	agg := &SumAgg{ColName: "v"}
	w := NewWindowAggOp(
		WindowSpecLite{},
		func(t types.Tuple) any { return t["id"] },
		[]string{"id"},
		func() any { return float64(0) },
		agg,
	)
	w.OrderByCol = "ts"
	w.FrameSpec = &FrameSpecLite{Type: "ROWS", StartType: "UNBOUNDED PRECEDING", EndType: "CURRENT ROW"}
	w.KeepInput = true
	w.EmitValue = true

	out, err := w.Apply(types.Batch{
		{Tuple: types.Tuple{"id": "a", "ts": int64(1), "v": 1.0}, Count: 1},
		{Tuple: types.Tuple{"id": "a", "ts": int64(2), "v": 2.0}, Count: 1},
		{Tuple: types.Tuple{"id": "a", "ts": int64(3), "v": 3.0}, Count: 1},
	})
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if len(out) != 3 {
		t.Fatalf("expected 3 output rows, got %d", len(out))
	}

	seen := map[int64]float64{}
	for _, td := range out {
		seen[td.Tuple["ts"].(int64)] = types.ToFloat64(td.Tuple["agg_result"])
	}
	if seen[1] != 1.0 || seen[2] != 3.0 || seen[3] != 6.0 {
		t.Fatalf("unexpected cumulative outputs: %v", seen)
	}
}

func TestWindowAggCumulativeFrameKeepsPackedRows(t *testing.T) {
	agg := &SumAgg{ColName: "v"}
	w := NewWindowAggOp(
		WindowSpecLite{},
		func(t types.Tuple) any { return t["id"] },
		[]string{"id"},
		func() any { return float64(0) },
		agg,
	)
	w.OrderByCol = "ts"
	w.FrameSpec = &FrameSpecLite{Type: "ROWS", StartType: "UNBOUNDED PRECEDING", EndType: "CURRENT ROW"}
	w.KeepInput = true
	w.EmitValue = true

	schema := types.NewPackedSchema([]string{"id", "ts", "v"})
	out, err := w.Apply(types.Batch{
		{Packed: types.NewPackedTupleWithPresence(schema, []any{"a", int64(1), 1.0}, []bool{true, true, true}), Count: 1},
		{Packed: types.NewPackedTupleWithPresence(schema, []any{"a", int64(2), 2.0}, []bool{true, true, true}), Count: 1},
	})
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if len(out) != 2 {
		t.Fatalf("expected 2 output rows, got %d", len(out))
	}
	for idx, td := range out {
		if td.Packed == nil {
			t.Fatalf("expected packed output at %d, got %+v", idx, td)
		}
	}
	first := out[0].Packed.Materialize()
	second := out[1].Packed.Materialize()
	if got := types.ToFloat64(first["agg_result"]); got != 1.0 {
		t.Fatalf("expected first cumulative 1.0, got %v", first)
	}
	if got := types.ToFloat64(second["agg_result"]); got != 3.0 {
		t.Fatalf("expected second cumulative 3.0, got %v", second)
	}
}

func TestWindowAggCumulativeFrameOutOfOrderInsertReplacesSuffix(t *testing.T) {
	agg := &SumAgg{ColName: "v"}
	w := NewWindowAggOp(
		WindowSpecLite{},
		func(t types.Tuple) any { return t["id"] },
		[]string{"id"},
		func() any { return float64(0) },
		agg,
	)
	w.OrderByCol = "ts"
	w.FrameSpec = &FrameSpecLite{Type: "ROWS", StartType: "UNBOUNDED PRECEDING", EndType: "CURRENT ROW"}
	w.KeepInput = true
	w.EmitValue = true

	if _, err := w.Apply(types.Batch{
		{Tuple: types.Tuple{"id": "a", "ts": int64(1), "v": 1.0}, Count: 1},
		{Tuple: types.Tuple{"id": "a", "ts": int64(3), "v": 3.0}, Count: 1},
	}); err != nil {
		t.Fatalf("apply initial: %v", err)
	}

	out, err := w.Apply(types.Batch{{Tuple: types.Tuple{"id": "a", "ts": int64(2), "v": 2.0}, Count: 1}})
	if err != nil {
		t.Fatalf("apply insert: %v", err)
	}

	changes := map[int64][]types.TupleDelta{}
	for _, td := range out {
		changes[td.Tuple["ts"].(int64)] = append(changes[td.Tuple["ts"].(int64)], td)
	}
	if len(changes[2]) != 1 || types.ToFloat64(changes[2][0].Tuple["agg_result"]) != 3.0 {
		t.Fatalf("expected ts=2 cumulative sum 3.0, got %v", changes[2])
	}

	var removedOld, addedNew bool
	for _, td := range changes[3] {
		switch {
		case td.Count == -1 && types.ToFloat64(td.Tuple["agg_result"]) == 4.0:
			removedOld = true
		case td.Count == 1 && types.ToFloat64(td.Tuple["agg_result"]) == 6.0:
			addedNew = true
		}
	}
	if !removedOld || !addedNew {
		t.Fatalf("expected ts=3 replacement 4.0 -> 6.0, got %v", changes[3])
	}
}

func TestWindowAggCumulativeFrameAppendOnlyEmitsOnlyNewRow(t *testing.T) {
	agg := &SumAgg{ColName: "v"}
	w := NewWindowAggOp(
		WindowSpecLite{},
		func(t types.Tuple) any { return t["id"] },
		[]string{"id"},
		func() any { return float64(0) },
		agg,
	)
	w.OrderByCol = "ts"
	w.FrameSpec = &FrameSpecLite{Type: "ROWS", StartType: "UNBOUNDED PRECEDING", EndType: "CURRENT ROW"}
	w.KeepInput = true
	w.EmitValue = true

	if _, err := w.Apply(types.Batch{
		{Tuple: types.Tuple{"id": "a", "ts": int64(1), "v": 1.0}, Count: 1},
		{Tuple: types.Tuple{"id": "a", "ts": int64(2), "v": 2.0}, Count: 1},
	}); err != nil {
		t.Fatalf("apply initial: %v", err)
	}

	out, err := w.Apply(types.Batch{{Tuple: types.Tuple{"id": "a", "ts": int64(3), "v": 3.0}, Count: 1}})
	if err != nil {
		t.Fatalf("apply append: %v", err)
	}
	if len(out) != 1 {
		t.Fatalf("expected 1 output row, got %d (%v)", len(out), out)
	}
	if out[0].Count != 1 || out[0].Tuple["ts"] != int64(3) || types.ToFloat64(out[0].Tuple["agg_result"]) != 6.0 {
		t.Fatalf("unexpected append output: %v", out)
	}
}

func TestWindowAggCumulativeFrameLatestRowReplacementTouchesTailOnly(t *testing.T) {
	agg := &SumAgg{ColName: "v"}
	w := NewWindowAggOp(
		WindowSpecLite{},
		func(t types.Tuple) any { return t["id"] },
		[]string{"id"},
		func() any { return float64(0) },
		agg,
	)
	w.OrderByCol = "ts"
	w.FrameSpec = &FrameSpecLite{Type: "ROWS", StartType: "UNBOUNDED PRECEDING", EndType: "CURRENT ROW"}
	w.KeepInput = true
	w.EmitValue = true

	if _, err := w.Apply(types.Batch{
		{Tuple: types.Tuple{"id": "a", "ts": int64(1), "v": 1.0}, Count: 1},
		{Tuple: types.Tuple{"id": "a", "ts": int64(2), "v": 2.0}, Count: 1},
	}); err != nil {
		t.Fatalf("apply initial: %v", err)
	}

	out, err := w.Apply(types.Batch{
		{Tuple: types.Tuple{"id": "a", "ts": int64(2), "v": 2.0}, Count: -1},
		{Tuple: types.Tuple{"id": "a", "ts": int64(2), "v": 4.0}, Count: 1},
	})
	if err != nil {
		t.Fatalf("apply replace: %v", err)
	}
	if len(out) != 2 {
		t.Fatalf("expected 2 output rows, got %d (%v)", len(out), out)
	}

	var removedOld, addedNew bool
	for _, td := range out {
		if td.Tuple["ts"] != int64(2) {
			t.Fatalf("expected only tail row ts=2 to change, got %v", out)
		}
		switch {
		case td.Count == -1 && types.ToFloat64(td.Tuple["agg_result"]) == 3.0:
			removedOld = true
		case td.Count == 1 && types.ToFloat64(td.Tuple["agg_result"]) == 5.0:
			addedNew = true
		}
	}
	if !removedOld || !addedNew {
		t.Fatalf("expected tail replacement 3.0 -> 5.0, got %v", out)
	}
}

func TestWindowAggCumulativeFrameAppendAfterFallbackUsesUpdatedTailState(t *testing.T) {
	agg := &SumAgg{ColName: "v"}
	w := NewWindowAggOp(
		WindowSpecLite{},
		func(t types.Tuple) any { return t["id"] },
		[]string{"id"},
		func() any { return float64(0) },
		agg,
	)
	w.OrderByCol = "ts"
	w.FrameSpec = &FrameSpecLite{Type: "ROWS", StartType: "UNBOUNDED PRECEDING", EndType: "CURRENT ROW"}
	w.KeepInput = true
	w.EmitValue = true

	if _, err := w.Apply(types.Batch{
		{Tuple: types.Tuple{"id": "a", "ts": int64(1), "v": 1.0}, Count: 1},
		{Tuple: types.Tuple{"id": "a", "ts": int64(3), "v": 3.0}, Count: 1},
	}); err != nil {
		t.Fatalf("apply initial: %v", err)
	}

	if _, err := w.Apply(types.Batch{{Tuple: types.Tuple{"id": "a", "ts": int64(2), "v": 2.0}, Count: 1}}); err != nil {
		t.Fatalf("apply out-of-order insert: %v", err)
	}

	out, err := w.Apply(types.Batch{{Tuple: types.Tuple{"id": "a", "ts": int64(4), "v": 4.0}, Count: 1}})
	if err != nil {
		t.Fatalf("apply tail append: %v", err)
	}
	if len(out) != 1 {
		t.Fatalf("expected only appended row output, got %d (%v)", len(out), out)
	}
	if out[0].Tuple["ts"] != int64(4) {
		t.Fatalf("expected appended ts=4 row, got %v", out[0].Tuple)
	}
	if got := types.ToFloat64(out[0].Tuple["agg_result"]); got != 10.0 {
		t.Fatalf("expected cumulative sum 10.0 after fallback+append, got %v", out[0].Tuple)
	}
	cache := w.getOrCreateCumulativeFrameCache("a")
	if cache.rowCount != 4 {
		t.Fatalf("expected cache rowCount=4, got %d", cache.rowCount)
	}
}

func TestWindowAggTumblingCompactsBatchDeltasPerWindowKey(t *testing.T) {
	w := NewWindowAggOp(
		WindowSpecLite{TimeCol: "ts", SizeMillis: 1000, WindowType: WindowTypeTumbling},
		func(t types.Tuple) any { return t["id"] },
		[]string{"id"},
		func() any { return float64(0) },
		&SumAgg{ColName: "v"},
	)

	out, err := w.Apply(types.Batch{
		{Tuple: types.Tuple{"id": "a", "ts": int64(100), "v": 1.0}, Count: 1},
		{Tuple: types.Tuple{"id": "a", "ts": int64(500), "v": 2.0}, Count: 1},
	})
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if len(out) != 1 {
		t.Fatalf("expected 1 compacted output row, got %d (%v)", len(out), out)
	}
	if out[0].Count != 1 {
		t.Fatalf("expected count 1, got %d", out[0].Count)
	}
	if out[0].Tuple["id"] != "a" {
		t.Fatalf("expected id=a, got %v", out[0].Tuple)
	}
	if got := types.ToFloat64(out[0].Tuple["agg_delta"]); got != 3.0 {
		t.Fatalf("expected agg_delta 3.0, got %v", out[0].Tuple)
	}
	if out[0].Tuple["__window_start"] != int64(0) || out[0].Tuple["__window_end"] != int64(1000) {
		t.Fatalf("unexpected window bounds: %v", out[0].Tuple)
	}
}

func TestWindowAggStateEntryCountExcludesDerivedCaches(t *testing.T) {
	w := NewWindowAggOp(
		WindowSpecLite{},
		func(t types.Tuple) any { return t["id"] },
		[]string{"id"},
		func() any { return float64(0) },
		&SumAgg{ColName: "v"},
	)
	w.State.Data[WindowID{Start: 0, End: 10}] = map[any]any{"a": 1.0, "b": 2.0}
	w.PartitionBuffers["frame"] = &PartitionBuffer{Rows: []RowWithOrder{{RowHash: 1}, {RowHash: 2}}}
	w.SessionBuffers["session"] = &PartitionBuffer{Rows: []RowWithOrder{{RowHash: 3}}}
	w.sessionOut["session"] = map[string]types.Tuple{"out": {"id": "a"}}
	w.frameOut = map[any]map[string]types.TupleDelta{"frame": {"out": {Tuple: types.Tuple{"id": "a"}, Count: 1}}}
	w.cumulativeFrameCache["frame"] = &cumulativeFramePartitionCache{rowCount: 2}

	if got := w.stateEntryCount(); got != 5 {
		t.Fatalf("expected state entries to exclude derived caches, got %d", got)
	}
}

func TestPartitionBufferRowIndexSurvivesInsertAndDelete(t *testing.T) {
	pb := &PartitionBuffer{}
	rows := []types.TupleDelta{
		{Tuple: types.Tuple{"ts": int64(1), "id": "a", "v": 1.0}, Count: 1},
		{Tuple: types.Tuple{"ts": int64(3), "id": "a", "v": 3.0}, Count: 1},
	}
	for _, td := range rows {
		pb.addRow(td, "ts")
	}

	if idx, _, _ := pb.findRow(int64(3), types.Tuple{"ts": int64(3), "id": "a", "v": 3.0}, nil); idx != 1 {
		t.Fatalf("expected ts=3 row at idx 1 before insert, got %d", idx)
	}

	pb.addRow(types.TupleDelta{Tuple: types.Tuple{"ts": int64(2), "id": "a", "v": 2.0}, Count: 1}, "ts")
	if idx, _, _ := pb.findRow(int64(2), types.Tuple{"ts": int64(2), "id": "a", "v": 2.0}, nil); idx != 1 {
		t.Fatalf("expected ts=2 row at idx 1 after insert, got %d", idx)
	}
	if idx, _, _ := pb.findRow(int64(3), types.Tuple{"ts": int64(3), "id": "a", "v": 3.0}, nil); idx != 2 {
		t.Fatalf("expected ts=3 row at idx 2 after insert, got %d", idx)
	}

	pb.addRow(types.TupleDelta{Tuple: types.Tuple{"ts": int64(2), "id": "a", "v": 2.0}, Count: -1}, "ts")
	if idx, _, _ := pb.findRow(int64(3), types.Tuple{"ts": int64(3), "id": "a", "v": 3.0}, nil); idx != 1 {
		t.Fatalf("expected ts=3 row at idx 1 after delete, got %d", idx)
	}
	if idx, _, _ := pb.findRow(int64(2), types.Tuple{"ts": int64(2), "id": "a", "v": 2.0}, nil); idx != -1 {
		t.Fatalf("expected ts=2 row to be absent after delete, got idx %d", idx)
	}
}
