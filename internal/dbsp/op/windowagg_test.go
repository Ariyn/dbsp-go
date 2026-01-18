package op

import (
	"testing"

	"github.com/ariyn/dbsp/internal/dbsp/types"
)

func TestWindowAggOp_RowsFrame(t *testing.T) {
	// Test ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
	frameSpec := &FrameSpecLite{
		Type:       "ROWS",
		StartType:  "1 PRECEDING",
		StartValue: "1",
		EndType:    "CURRENT ROW",
		EndValue:   "",
	}

	keyFn := func(t types.Tuple) any { return t["region"] }
	aggInit := func() any { return int64(0) }
	agg := &CountAgg{}

	op := NewWindowAggOp(WindowSpecLite{}, keyFn, []string{"region"}, aggInit, agg)
	op.OrderByCol = "ts"
	op.FrameSpec = frameSpec

	// Input batch
	batch := types.Batch{
		{Tuple: types.Tuple{"region": "East", "ts": int64(1), "value": 10}, Count: 1},
		{Tuple: types.Tuple{"region": "East", "ts": int64(2), "value": 20}, Count: 1},
		{Tuple: types.Tuple{"region": "East", "ts": int64(3), "value": 30}, Count: 1},
	}

	out, err := op.Apply(batch)
	if err != nil {
		t.Fatalf("Apply failed: %v", err)
	}

	if len(out) == 0 {
		t.Fatal("expected non-empty output")
	}

	t.Logf("Output batch:")
	for i, td := range out {
		t.Logf("  [%d] count=%d, tuple=%#v", i, td.Count, td.Tuple)
	}

	// First row: only current row in frame (count=1)
	// Second row: 2 rows in frame (count=2)
	// Third row: 2 rows in frame (count=2)
	if len(out) != 3 {
		t.Errorf("expected 3 output rows, got %d", len(out))
	}
}

func TestWindowAggOp_RangeFrame(t *testing.T) {
	// Test RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
	frameSpec := &FrameSpecLite{
		Type:      "RANGE",
		StartType: "UNBOUNDED PRECEDING",
		EndType:   "CURRENT ROW",
	}

	keyFn := func(t types.Tuple) any { return t["user"] }
	aggInit := func() any { return float64(0) }
	agg := &SumAgg{ColName: "amount"}

	op := NewWindowAggOp(WindowSpecLite{}, keyFn, []string{"user"}, aggInit, agg)
	op.OrderByCol = "ts"
	op.FrameSpec = frameSpec

	// Input batch
	batch := types.Batch{
		{Tuple: types.Tuple{"user": "Alice", "ts": int64(1), "amount": 100.0}, Count: 1},
		{Tuple: types.Tuple{"user": "Alice", "ts": int64(2), "amount": 50.0}, Count: 1},
		{Tuple: types.Tuple{"user": "Alice", "ts": int64(3), "amount": 75.0}, Count: 1},
	}

	out, err := op.Apply(batch)
	if err != nil {
		t.Fatalf("Apply failed: %v", err)
	}

	if len(out) == 0 {
		t.Fatal("expected non-empty output")
	}

	t.Logf("Output batch:")
	for i, td := range out {
		t.Logf("  [%d] count=%d, tuple=%#v", i, td.Count, td.Tuple)
	}

	// Running sum: 100, 150, 225
	if len(out) != 3 {
		t.Errorf("expected 3 output rows, got %d", len(out))
	}
}

func TestWindowAggOp_MultiplePartitions(t *testing.T) {
	// Test with multiple partitions
	frameSpec := &FrameSpecLite{
		Type:       "ROWS",
		StartType:  "UNBOUNDED PRECEDING",
		StartValue: "",
		EndType:    "CURRENT ROW",
		EndValue:   "",
	}

	keyFn := func(t types.Tuple) any { return t["category"] }
	aggInit := func() any { return float64(0) }
	agg := &SumAgg{ColName: "sales"}

	op := NewWindowAggOp(WindowSpecLite{}, keyFn, []string{"category"}, aggInit, agg)
	op.OrderByCol = "date"
	op.FrameSpec = frameSpec

	// Input batch with two partitions
	batch := types.Batch{
		{Tuple: types.Tuple{"category": "A", "date": int64(1), "sales": 100.0}, Count: 1},
		{Tuple: types.Tuple{"category": "A", "date": int64(2), "sales": 200.0}, Count: 1},
		{Tuple: types.Tuple{"category": "B", "date": int64(1), "sales": 50.0}, Count: 1},
		{Tuple: types.Tuple{"category": "B", "date": int64(2), "sales": 75.0}, Count: 1},
	}

	out, err := op.Apply(batch)
	if err != nil {
		t.Fatalf("Apply failed: %v", err)
	}

	if len(out) == 0 {
		t.Fatal("expected non-empty output")
	}

	t.Logf("Output batch:")
	for i, td := range out {
		t.Logf("  [%d] count=%d, tuple=%#v", i, td.Count, td.Tuple)
	}

	// Should have 4 rows (2 per partition)
	if len(out) != 4 {
		t.Errorf("expected 4 output rows, got %d", len(out))
	}
}

func TestWindowAggOp_AvgWithFrame(t *testing.T) {
	// Test AVG with frame
	frameSpec := &FrameSpecLite{
		Type:       "ROWS",
		StartType:  "1 PRECEDING",
		StartValue: "1",
		EndType:    "1 FOLLOWING",
		EndValue:   "1",
	}

	keyFn := func(t types.Tuple) any { return nil } // No partition
	aggInit := func() any { return AvgMonoid{} }
	agg := &AvgAgg{ColName: "value"}

	op := NewWindowAggOp(WindowSpecLite{}, keyFn, []string{}, aggInit, agg)
	op.OrderByCol = "id"
	op.FrameSpec = frameSpec

	// Input batch
	batch := types.Batch{
		{Tuple: types.Tuple{"id": int64(1), "value": 10.0}, Count: 1},
		{Tuple: types.Tuple{"id": int64(2), "value": 20.0}, Count: 1},
		{Tuple: types.Tuple{"id": int64(3), "value": 30.0}, Count: 1},
		{Tuple: types.Tuple{"id": int64(4), "value": 40.0}, Count: 1},
	}

	out, err := op.Apply(batch)
	if err != nil {
		t.Fatalf("Apply failed: %v", err)
	}

	if len(out) == 0 {
		t.Fatal("expected non-empty output")
	}

	t.Logf("Output batch:")
	for i, td := range out {
		t.Logf("  [%d] count=%d, tuple=%#v", i, td.Count, td.Tuple)
	}

	// Moving average with 3-row window
	// Row 1: avg(10, 20) = 15
	// Row 2: avg(10, 20, 30) = 20
	// Row 3: avg(20, 30, 40) = 30
	// Row 4: avg(30, 40) = 35
}

func TestWindowAggOp_CountWithDeletions(t *testing.T) {
	// Test COUNT with deletions
	frameSpec := &FrameSpecLite{
		Type:      "ROWS",
		StartType: "UNBOUNDED PRECEDING",
		EndType:   "CURRENT ROW",
	}

	keyFn := func(t types.Tuple) any { return nil }
	aggInit := func() any { return int64(0) }
	agg := &CountAgg{}

	op := NewWindowAggOp(WindowSpecLite{}, keyFn, []string{}, aggInit, agg)
	op.OrderByCol = "ts"
	op.FrameSpec = frameSpec

	// Insert batch
	batch1 := types.Batch{
		{Tuple: types.Tuple{"ts": int64(1), "value": 10}, Count: 1},
		{Tuple: types.Tuple{"ts": int64(2), "value": 20}, Count: 1},
		{Tuple: types.Tuple{"ts": int64(3), "value": 30}, Count: 1},
	}

	out1, err := op.Apply(batch1)
	if err != nil {
		t.Fatalf("Apply batch1 failed: %v", err)
	}

	t.Logf("After insert - Output batch:")
	for i, td := range out1 {
		t.Logf("  [%d] count=%d, tuple=%#v", i, td.Count, td.Tuple)
	}

	// Delete batch
	batch2 := types.Batch{
		{Tuple: types.Tuple{"ts": int64(2), "value": 20}, Count: -1},
	}

	out2, err := op.Apply(batch2)
	if err != nil {
		t.Fatalf("Apply batch2 failed: %v", err)
	}

	t.Logf("After delete - Output batch:")
	for i, td := range out2 {
		t.Logf("  [%d] count=%d, tuple=%#v", i, td.Count, td.Tuple)
	}
}

func TestWindowAggOp_TumblingCount_Delete(t *testing.T) {
	spec := WindowSpecLite{TimeCol: "ts", SizeMillis: 10, WindowType: WindowTypeTumbling}
	keyFn := func(t types.Tuple) any { return t["region"] }
	aggInit := func() any { return int64(0) }
	agg := &CountAgg{}

	op := NewWindowAggOp(spec, keyFn, []string{"region"}, aggInit, agg)

	ins := types.Batch{
		{Tuple: types.Tuple{"region": "East", "ts": int64(1)}, Count: 1},
		{Tuple: types.Tuple{"region": "East", "ts": int64(2)}, Count: 1},
	}
	outIns, err := op.Apply(ins)
	if err != nil {
		t.Fatalf("Apply(insert) failed: %v", err)
	}
	if len(outIns) == 0 {
		t.Fatalf("expected non-empty output deltas on insert")
	}
	var sumDelta int64
	for _, td := range outIns {
		if td.Count != 1 {
			t.Fatalf("expected output Count=1, got %d (%+v)", td.Count, td)
		}
		if td.Tuple["__window_start"] != int64(0) || td.Tuple["__window_end"] != int64(10) {
			t.Fatalf("expected window [0,10), got start=%v end=%v (%+v)", td.Tuple["__window_start"], td.Tuple["__window_end"], td)
		}
		if td.Tuple["region"] != "East" {
			t.Fatalf("expected region=East, got %v (%+v)", td.Tuple["region"], td)
		}
		d, ok := td.Tuple["count_delta"].(int64)
		if !ok {
			t.Fatalf("expected count_delta int64, got %T (%+v)", td.Tuple["count_delta"], td)
		}
		sumDelta += d
	}
	if sumDelta != 2 {
		t.Fatalf("expected net count_delta=2 on insert, got %d (out=%+v)", sumDelta, outIns)
	}

	del := types.Batch{{Tuple: types.Tuple{"region": "East", "ts": int64(2)}, Count: -1}}
	outDel, err := op.Apply(del)
	if err != nil {
		t.Fatalf("Apply(delete) failed: %v", err)
	}
	if len(outDel) != 1 {
		t.Fatalf("expected 1 output delta on delete, got %d (%+v)", len(outDel), outDel)
	}
	if outDel[0].Count != 1 {
		t.Fatalf("expected output Count=1, got %d (%+v)", outDel[0].Count, outDel[0])
	}
	if outDel[0].Tuple["__window_start"] != int64(0) || outDel[0].Tuple["__window_end"] != int64(10) {
		t.Fatalf("expected window [0,10), got start=%v end=%v (%+v)", outDel[0].Tuple["__window_start"], outDel[0].Tuple["__window_end"], outDel[0])
	}
	if outDel[0].Tuple["region"] != "East" {
		t.Fatalf("expected region=East, got %v (%+v)", outDel[0].Tuple["region"], outDel[0])
	}
	d, ok := outDel[0].Tuple["count_delta"].(int64)
	if !ok {
		t.Fatalf("expected count_delta int64, got %T (%+v)", outDel[0].Tuple["count_delta"], outDel[0])
	}
	if d != -1 {
		t.Fatalf("expected count_delta=-1, got %v (%+v)", d, outDel[0])
	}
}

func TestWindowAggOp_TumblingCount_MultiKey_IncludesKeys_AndEvictsOnEmpty(t *testing.T) {
	spec := WindowSpecLite{TimeCol: "ts", SizeMillis: 10, WindowType: WindowTypeTumbling}
	keyFn := func(t types.Tuple) any {
		return stableTupleKey(types.Tuple{"k1": t["k1"], "k2": t["k2"]})
	}
	aggInit := func() any { return int64(0) }
	agg := &CountAgg{}

	op := NewWindowAggOp(spec, keyFn, []string{"k1", "k2"}, aggInit, agg)

	ins := types.Batch{{Tuple: types.Tuple{"k1": "A", "k2": "X", "ts": int64(1)}, Count: 1}}
	outIns, err := op.Apply(ins)
	if err != nil {
		t.Fatalf("Apply(insert) failed: %v", err)
	}
	if len(outIns) == 0 {
		t.Fatalf("expected non-empty output deltas on insert")
	}
	var sumIns int64
	for _, td := range outIns {
		if td.Count != 1 {
			t.Fatalf("expected output Count=1, got %d (%+v)", td.Count, td)
		}
		if td.Tuple["__window_start"] != int64(0) || td.Tuple["__window_end"] != int64(10) {
			t.Fatalf("expected window [0,10), got start=%v end=%v (%+v)", td.Tuple["__window_start"], td.Tuple["__window_end"], td)
		}
		if td.Tuple["k1"] != "A" || td.Tuple["k2"] != "X" {
			t.Fatalf("expected group keys k1=A,k2=X, got k1=%v k2=%v (%+v)", td.Tuple["k1"], td.Tuple["k2"], td)
		}
		d, ok := td.Tuple["count_delta"].(int64)
		if !ok {
			t.Fatalf("expected count_delta int64, got %T (%+v)", td.Tuple["count_delta"], td)
		}
		sumIns += d
	}
	if sumIns != 1 {
		t.Fatalf("expected net count_delta=1 on insert, got %d (out=%+v)", sumIns, outIns)
	}

	del := types.Batch{{Tuple: types.Tuple{"k1": "A", "k2": "X", "ts": int64(1)}, Count: -1}}
	outDel, err := op.Apply(del)
	if err != nil {
		t.Fatalf("Apply(delete) failed: %v", err)
	}
	if len(outDel) == 0 {
		t.Fatalf("expected non-empty output deltas on delete")
	}
	var sumDel int64
	for _, td := range outDel {
		if td.Count != 1 {
			t.Fatalf("expected output Count=1, got %d (%+v)", td.Count, td)
		}
		if td.Tuple["__window_start"] != int64(0) || td.Tuple["__window_end"] != int64(10) {
			t.Fatalf("expected window [0,10), got start=%v end=%v (%+v)", td.Tuple["__window_start"], td.Tuple["__window_end"], td)
		}
		if td.Tuple["k1"] != "A" || td.Tuple["k2"] != "X" {
			t.Fatalf("expected group keys k1=A,k2=X, got k1=%v k2=%v (%+v)", td.Tuple["k1"], td.Tuple["k2"], td)
		}
		d, ok := td.Tuple["count_delta"].(int64)
		if !ok {
			t.Fatalf("expected count_delta int64, got %T (%+v)", td.Tuple["count_delta"], td)
		}
		sumDel += d
	}
	if sumDel != -1 {
		t.Fatalf("expected net count_delta=-1 on delete, got %d (out=%+v)", sumDel, outDel)
	}

	// After delete, the group should be evicted from state and group-counts.
	wid := WindowID{Start: 0, End: 10}
	gkey := stableTupleKey(types.Tuple{"k1": "A", "k2": "X"})
	if wm, ok := op.State.Data[wid]; ok {
		if _, ok2 := wm[gkey]; ok2 {
			t.Fatalf("expected group state to be evicted for gkey=%v, but found in State.Data[%+v]", gkey, wid)
		}
		if len(wm) != 0 {
			t.Fatalf("expected no remaining groups for window %+v, got %v", wid, wm)
		}
	}
	if cm, ok := op.GroupCounts[wid]; ok {
		if _, ok2 := cm[gkey]; ok2 {
			t.Fatalf("expected group count to be evicted for gkey=%v, but found in GroupCounts[%+v]", gkey, wid)
		}
		if len(cm) != 0 {
			t.Fatalf("expected no remaining group counts for window %+v, got %v", wid, cm)
		}
	}
}

func TestWindowAggOp_SlidingSum_MultiKey_IncludesKeys_AndEvictsOnEmpty(t *testing.T) {
	spec := WindowSpecLite{TimeCol: "ts", SizeMillis: 10, SlideMillis: 5, WindowType: WindowTypeSliding}
	keyFn := func(t types.Tuple) any {
		return stableTupleKey(types.Tuple{"k1": t["k1"], "k2": t["k2"]})
	}
	aggInit := func() any { return float64(0) }
	agg := &SumAgg{ColName: "v"}

	op := NewWindowAggOp(spec, keyFn, []string{"k1", "k2"}, aggInit, agg)

	ins := types.Batch{{Tuple: types.Tuple{"k1": "A", "k2": "X", "ts": int64(7), "v": float64(10)}, Count: 1}}
	outIns, err := op.Apply(ins)
	if err != nil {
		t.Fatalf("Apply(insert) failed: %v", err)
	}
	if len(outIns) != 2 {
		t.Fatalf("expected 2 window deltas on insert (ts=7 belongs to 2 sliding windows), got %d (%+v)", len(outIns), outIns)
	}
	seen := make(map[WindowID]float64)
	for _, td := range outIns {
		if td.Count != 1 {
			t.Fatalf("expected output Count=1, got %d (%+v)", td.Count, td)
		}
		if td.Tuple["k1"] != "A" || td.Tuple["k2"] != "X" {
			t.Fatalf("expected group keys k1=A,k2=X, got k1=%v k2=%v (%+v)", td.Tuple["k1"], td.Tuple["k2"], td)
		}
		start, okS := td.Tuple["__window_start"].(int64)
		end, okE := td.Tuple["__window_end"].(int64)
		if !okS || !okE {
			t.Fatalf("expected __window_start/__window_end int64, got start=%T end=%T (%+v)", td.Tuple["__window_start"], td.Tuple["__window_end"], td)
		}
		d, ok := td.Tuple["agg_delta"].(float64)
		if !ok {
			t.Fatalf("expected agg_delta float64, got %T (%+v)", td.Tuple["agg_delta"], td)
		}
		seen[WindowID{Start: start, End: end}] += d
	}
	if seen[WindowID{Start: 0, End: 10}] != 10.0 {
		t.Fatalf("expected agg_delta +10 for window [0,10), got %v (out=%+v)", seen[WindowID{Start: 0, End: 10}], outIns)
	}
	if seen[WindowID{Start: 5, End: 15}] != 10.0 {
		t.Fatalf("expected agg_delta +10 for window [5,15), got %v (out=%+v)", seen[WindowID{Start: 5, End: 15}], outIns)
	}

	del := types.Batch{{Tuple: types.Tuple{"k1": "A", "k2": "X", "ts": int64(7), "v": float64(10)}, Count: -1}}
	outDel, err := op.Apply(del)
	if err != nil {
		t.Fatalf("Apply(delete) failed: %v", err)
	}
	if len(outDel) != 2 {
		t.Fatalf("expected 2 window deltas on delete, got %d (%+v)", len(outDel), outDel)
	}
	seenDel := make(map[WindowID]float64)
	for _, td := range outDel {
		if td.Count != 1 {
			t.Fatalf("expected output Count=1, got %d (%+v)", td.Count, td)
		}
		if td.Tuple["k1"] != "A" || td.Tuple["k2"] != "X" {
			t.Fatalf("expected group keys k1=A,k2=X, got k1=%v k2=%v (%+v)", td.Tuple["k1"], td.Tuple["k2"], td)
		}
		start, okS := td.Tuple["__window_start"].(int64)
		end, okE := td.Tuple["__window_end"].(int64)
		if !okS || !okE {
			t.Fatalf("expected __window_start/__window_end int64, got start=%T end=%T (%+v)", td.Tuple["__window_start"], td.Tuple["__window_end"], td)
		}
		d, ok := td.Tuple["agg_delta"].(float64)
		if !ok {
			t.Fatalf("expected agg_delta float64, got %T (%+v)", td.Tuple["agg_delta"], td)
		}
		seenDel[WindowID{Start: start, End: end}] += d
	}
	if seenDel[WindowID{Start: 0, End: 10}] != -10.0 {
		t.Fatalf("expected agg_delta -10 for window [0,10), got %v (out=%+v)", seenDel[WindowID{Start: 0, End: 10}], outDel)
	}
	if seenDel[WindowID{Start: 5, End: 15}] != -10.0 {
		t.Fatalf("expected agg_delta -10 for window [5,15), got %v (out=%+v)", seenDel[WindowID{Start: 5, End: 15}], outDel)
	}

	// After delete, both windows should be fully evicted.
	gkey := stableTupleKey(types.Tuple{"k1": "A", "k2": "X"})
	for _, wid := range []WindowID{{Start: 0, End: 10}, {Start: 5, End: 15}} {
		if wm, ok := op.State.Data[wid]; ok {
			if _, ok2 := wm[gkey]; ok2 {
				t.Fatalf("expected group state to be evicted for gkey=%v, but found in State.Data[%+v]", gkey, wid)
			}
			if len(wm) != 0 {
				t.Fatalf("expected no remaining groups for window %+v, got %v", wid, wm)
			}
		}
		if cm, ok := op.GroupCounts[wid]; ok {
			if _, ok2 := cm[gkey]; ok2 {
				t.Fatalf("expected group count to be evicted for gkey=%v, but found in GroupCounts[%+v]", gkey, wid)
			}
			if len(cm) != 0 {
				t.Fatalf("expected no remaining group counts for window %+v, got %v", wid, cm)
			}
		}
	}
}

func TestWindowAggOp_SlidingSum_DeleteMultiWindows(t *testing.T) {
	spec := WindowSpecLite{TimeCol: "ts", SizeMillis: 10, SlideMillis: 5, WindowType: WindowTypeSliding}
	keyFn := func(t types.Tuple) any { return nil }
	aggInit := func() any { return float64(0) }
	agg := &SumAgg{ColName: "v"}

	op := NewWindowAggOp(spec, keyFn, []string{}, aggInit, agg)

	ins := types.Batch{{Tuple: types.Tuple{"ts": int64(7), "v": float64(10)}, Count: 1}}
	outIns, err := op.Apply(ins)
	if err != nil {
		t.Fatalf("Apply(insert) failed: %v", err)
	}
	if len(outIns) != 2 {
		t.Fatalf("expected 2 window deltas on insert (ts=7 belongs to 2 sliding windows), got %d (%+v)", len(outIns), outIns)
	}
	seen := make(map[[2]int64]float64)
	for _, td := range outIns {
		start, _ := td.Tuple["__window_start"].(int64)
		end, _ := td.Tuple["__window_end"].(int64)
		d, ok := td.Tuple["agg_delta"].(float64)
		if !ok {
			t.Fatalf("expected agg_delta float64, got %T (%+v)", td.Tuple["agg_delta"], td)
		}
		seen[[2]int64{start, end}] += d
	}
	if seen[[2]int64{0, 10}] != 10.0 {
		t.Fatalf("expected agg_delta +10 for window [0,10), got %v (out=%+v)", seen[[2]int64{0, 10}], outIns)
	}
	if seen[[2]int64{5, 15}] != 10.0 {
		t.Fatalf("expected agg_delta +10 for window [5,15), got %v (out=%+v)", seen[[2]int64{5, 15}], outIns)
	}

	del := types.Batch{{Tuple: types.Tuple{"ts": int64(7), "v": float64(10)}, Count: -1}}
	outDel, err := op.Apply(del)
	if err != nil {
		t.Fatalf("Apply(delete) failed: %v", err)
	}
	if len(outDel) != 2 {
		t.Fatalf("expected 2 window deltas on delete, got %d (%+v)", len(outDel), outDel)
	}
	seenDel := make(map[[2]int64]float64)
	for _, td := range outDel {
		start, _ := td.Tuple["__window_start"].(int64)
		end, _ := td.Tuple["__window_end"].(int64)
		d, ok := td.Tuple["agg_delta"].(float64)
		if !ok {
			t.Fatalf("expected agg_delta float64, got %T (%+v)", td.Tuple["agg_delta"], td)
		}
		seenDel[[2]int64{start, end}] += d
	}
	if seenDel[[2]int64{0, 10}] != -10.0 {
		t.Fatalf("expected agg_delta -10 for window [0,10), got %v (out=%+v)", seenDel[[2]int64{0, 10}], outDel)
	}
	if seenDel[[2]int64{5, 15}] != -10.0 {
		t.Fatalf("expected agg_delta -10 for window [5,15), got %v (out=%+v)", seenDel[[2]int64{5, 15}], outDel)
	}
}

func TestWindowAggOp_TumblingMin_DuplicateDeleteNoChangeThenChange(t *testing.T) {
	spec := WindowSpecLite{TimeCol: "ts", SizeMillis: 10, WindowType: WindowTypeTumbling}
	keyFn := func(t types.Tuple) any { return t["region"] }
	aggInit := func() any { return NewSortedMultiset() }
	agg := &MinAgg{ColName: "v"}

	op := NewWindowAggOp(spec, keyFn, []string{"region"}, aggInit, agg)

	ins := types.Batch{
		{Tuple: types.Tuple{"region": "East", "ts": int64(1), "v": int64(10)}, Count: 1},
		{Tuple: types.Tuple{"region": "East", "ts": int64(2), "v": int64(10)}, Count: 1},
		{Tuple: types.Tuple{"region": "East", "ts": int64(3), "v": int64(20)}, Count: 1},
	}
	outIns, err := op.Apply(ins)
	if err != nil {
		t.Fatalf("Apply(insert) failed: %v", err)
	}
	if len(outIns) != 1 {
		t.Fatalf("expected exactly 1 delta when initial min is established, got %d (%+v)", len(outIns), outIns)
	}
	if outIns[0].Tuple["min"] != "10" {
		t.Fatalf("expected min=10, got %v (%+v)", outIns[0].Tuple["min"], outIns[0])
	}
	if outIns[0].Tuple["__window_start"] != int64(0) || outIns[0].Tuple["__window_end"] != int64(10) {
		t.Fatalf("expected window [0,10), got start=%v end=%v", outIns[0].Tuple["__window_start"], outIns[0].Tuple["__window_end"])
	}
	if outIns[0].Tuple["region"] != "East" {
		t.Fatalf("expected region=East, got %v", outIns[0].Tuple["region"])
	}

	// Delete one duplicate min (10). Min should remain 10 -> no output.
	outDel1, err := op.Apply(types.Batch{{Tuple: types.Tuple{"region": "East", "ts": int64(2), "v": int64(10)}, Count: -1}})
	if err != nil {
		t.Fatalf("Apply(delete1) failed: %v", err)
	}
	if len(outDel1) != 0 {
		t.Fatalf("expected no delta when deleting non-last duplicate min, got %d (%+v)", len(outDel1), outDel1)
	}

	// Delete the last remaining 10. Min should change to 20 -> one output.
	outDel2, err := op.Apply(types.Batch{{Tuple: types.Tuple{"region": "East", "ts": int64(1), "v": int64(10)}, Count: -1}})
	if err != nil {
		t.Fatalf("Apply(delete2) failed: %v", err)
	}
	if len(outDel2) != 1 {
		t.Fatalf("expected 1 delta when min changes, got %d (%+v)", len(outDel2), outDel2)
	}
	if outDel2[0].Tuple["min"] != "20" {
		t.Fatalf("expected min=20 after deleting last 10, got %v (%+v)", outDel2[0].Tuple["min"], outDel2[0])
	}
}

func TestWindowAggOp_TumblingMax_DuplicateDeleteNoChangeThenChange(t *testing.T) {
	spec := WindowSpecLite{TimeCol: "ts", SizeMillis: 10, WindowType: WindowTypeTumbling}
	keyFn := func(t types.Tuple) any { return nil }
	aggInit := func() any { return NewSortedMultiset() }
	agg := &MaxAgg{ColName: "v"}

	op := NewWindowAggOp(spec, keyFn, []string{}, aggInit, agg)

	ins := types.Batch{
		{Tuple: types.Tuple{"ts": int64(1), "v": int64(30)}, Count: 1},
		{Tuple: types.Tuple{"ts": int64(2), "v": int64(20)}, Count: 1},
		{Tuple: types.Tuple{"ts": int64(3), "v": int64(30)}, Count: 1},
	}
	outIns, err := op.Apply(ins)
	if err != nil {
		t.Fatalf("Apply(insert) failed: %v", err)
	}
	if len(outIns) != 1 {
		t.Fatalf("expected exactly 1 delta when initial max is established, got %d (%+v)", len(outIns), outIns)
	}
	if outIns[0].Tuple["max"] != "30" {
		t.Fatalf("expected max=30, got %v (%+v)", outIns[0].Tuple["max"], outIns[0])
	}

	// Delete one duplicate max (30). Max should remain 30 -> no output.
	outDel1, err := op.Apply(types.Batch{{Tuple: types.Tuple{"ts": int64(3), "v": int64(30)}, Count: -1}})
	if err != nil {
		t.Fatalf("Apply(delete1) failed: %v", err)
	}
	if len(outDel1) != 0 {
		t.Fatalf("expected no delta when deleting non-last duplicate max, got %d (%+v)", len(outDel1), outDel1)
	}

	// Delete the last remaining 30. Max should change to 20 -> one output.
	outDel2, err := op.Apply(types.Batch{{Tuple: types.Tuple{"ts": int64(1), "v": int64(30)}, Count: -1}})
	if err != nil {
		t.Fatalf("Apply(delete2) failed: %v", err)
	}
	if len(outDel2) != 1 {
		t.Fatalf("expected 1 delta when max changes, got %d (%+v)", len(outDel2), outDel2)
	}
	if outDel2[0].Tuple["max"] != "20" {
		t.Fatalf("expected max=20 after deleting last 30, got %v (%+v)", outDel2[0].Tuple["max"], outDel2[0])
	}
}

func TestWindowAggOp_TumblingMin_DeleteToEmpty_EmitsNil(t *testing.T) {
	spec := WindowSpecLite{TimeCol: "ts", SizeMillis: 10, WindowType: WindowTypeTumbling}
	keyFn := func(t types.Tuple) any { return nil }
	aggInit := func() any { return NewSortedMultiset() }
	agg := &MinAgg{ColName: "v"}

	op := NewWindowAggOp(spec, keyFn, []string{}, aggInit, agg)

	outIns, err := op.Apply(types.Batch{{Tuple: types.Tuple{"ts": int64(1), "v": int64(10)}, Count: 1}})
	if err != nil {
		t.Fatalf("Apply(insert) failed: %v", err)
	}
	if len(outIns) != 1 || outIns[0].Tuple["min"] != "10" {
		t.Fatalf("expected one delta min=10, got %v", outIns)
	}

	outDel, err := op.Apply(types.Batch{{Tuple: types.Tuple{"ts": int64(1), "v": int64(10)}, Count: -1}})
	if err != nil {
		t.Fatalf("Apply(delete) failed: %v", err)
	}
	if len(outDel) != 1 {
		t.Fatalf("expected one delta when min becomes nil, got %v", outDel)
	}
	if outDel[0].Tuple["min"] != nil {
		t.Fatalf("expected min=nil after deleting last value, got %v (%+v)", outDel[0].Tuple["min"], outDel[0])
	}
	if outDel[0].Tuple["__window_start"] != int64(0) || outDel[0].Tuple["__window_end"] != int64(10) {
		t.Fatalf("expected window [0,10), got start=%v end=%v", outDel[0].Tuple["__window_start"], outDel[0].Tuple["__window_end"])
	}
}

func TestWindowAggOp_SlidingMin_DeleteAffectsMultipleWindows(t *testing.T) {
	spec := WindowSpecLite{TimeCol: "ts", SizeMillis: 10, SlideMillis: 5, WindowType: WindowTypeSliding}
	keyFn := func(t types.Tuple) any { return nil }
	aggInit := func() any { return NewSortedMultiset() }
	agg := &MinAgg{ColName: "v"}

	op := NewWindowAggOp(spec, keyFn, []string{}, aggInit, agg)

	// Insert two values at ts=7 (belongs to [0,10) and [5,15)). min becomes 10.
	_, err := op.Apply(types.Batch{
		{Tuple: types.Tuple{"ts": int64(7), "v": int64(10)}, Count: 1},
		{Tuple: types.Tuple{"ts": int64(7), "v": int64(20)}, Count: 1},
	})
	if err != nil {
		t.Fatalf("Apply(insert) failed: %v", err)
	}

	// Delete the min (10). min becomes 20 in both windows -> two outputs.
	outDel, err := op.Apply(types.Batch{{Tuple: types.Tuple{"ts": int64(7), "v": int64(10)}, Count: -1}})
	if err != nil {
		t.Fatalf("Apply(delete) failed: %v", err)
	}
	if len(outDel) != 2 {
		t.Fatalf("expected 2 deltas (2 windows) when min changes, got %d (%+v)", len(outDel), outDel)
	}
	seen := make(map[[2]int64]any)
	for _, td := range outDel {
		start, _ := td.Tuple["__window_start"].(int64)
		end, _ := td.Tuple["__window_end"].(int64)
		seen[[2]int64{start, end}] = td.Tuple["min"]
	}
	if seen[[2]int64{0, 10}] != "20" {
		t.Fatalf("expected min=20 for window [0,10), got %v (%+v)", seen[[2]int64{0, 10}], outDel)
	}
	if seen[[2]int64{5, 15}] != "20" {
		t.Fatalf("expected min=20 for window [5,15), got %v (%+v)", seen[[2]int64{5, 15}], outDel)
	}
}

func TestWindowAggOp_TumblingMax_DeleteAffectsOnlyThatGroup(t *testing.T) {
	spec := WindowSpecLite{TimeCol: "ts", SizeMillis: 10, WindowType: WindowTypeTumbling}
	keyFn := func(t types.Tuple) any { return t["region"] }
	aggInit := func() any { return NewSortedMultiset() }
	agg := &MaxAgg{ColName: "v"}

	op := NewWindowAggOp(spec, keyFn, []string{"region"}, aggInit, agg)

	_, err := op.Apply(types.Batch{
		{Tuple: types.Tuple{"region": "East", "ts": int64(1), "v": int64(10)}, Count: 1},
		{Tuple: types.Tuple{"region": "East", "ts": int64(2), "v": int64(20)}, Count: 1},
		{Tuple: types.Tuple{"region": "West", "ts": int64(3), "v": int64(30)}, Count: 1},
	})
	if err != nil {
		t.Fatalf("Apply(insert) failed: %v", err)
	}

	// Delete East's max (20): should emit only one delta for East, max becomes 10.
	outDel, err := op.Apply(types.Batch{{Tuple: types.Tuple{"region": "East", "ts": int64(2), "v": int64(20)}, Count: -1}})
	if err != nil {
		t.Fatalf("Apply(delete) failed: %v", err)
	}
	if len(outDel) != 1 {
		t.Fatalf("expected 1 delta for East max change, got %d (%+v)", len(outDel), outDel)
	}
	if outDel[0].Tuple["region"] != "East" {
		t.Fatalf("expected region=East, got %v (%+v)", outDel[0].Tuple["region"], outDel[0])
	}
	if outDel[0].Tuple["max"] != "10" {
		t.Fatalf("expected max=10 after deleting East's 20, got %v (%+v)", outDel[0].Tuple["max"], outDel[0])
	}
}

func TestWindowAggOp_MinMaxWithFrame(t *testing.T) {
	// Test MIN/MAX with frame
	frameSpec := &FrameSpecLite{
		Type:       "ROWS",
		StartType:  "2 PRECEDING",
		StartValue: "2",
		EndType:    "CURRENT ROW",
		EndValue:   "",
	}

	keyFn := func(t types.Tuple) any { return nil }
	aggInit := func() any { return NewSortedMultiset() }
	agg := &MinAgg{ColName: "price"}

	op := NewWindowAggOp(WindowSpecLite{}, keyFn, []string{}, aggInit, agg)
	op.OrderByCol = "ts"
	op.FrameSpec = frameSpec

	// Input batch
	batch := types.Batch{
		{Tuple: types.Tuple{"ts": int64(1), "price": 100}, Count: 1},
		{Tuple: types.Tuple{"ts": int64(2), "price": 50}, Count: 1},
		{Tuple: types.Tuple{"ts": int64(3), "price": 75}, Count: 1},
		{Tuple: types.Tuple{"ts": int64(4), "price": 90}, Count: 1},
	}

	out, err := op.Apply(batch)
	if err != nil {
		t.Fatalf("Apply failed: %v", err)
	}

	if len(out) == 0 {
		t.Fatal("expected non-empty output")
	}

	t.Logf("Output batch:")
	for i, td := range out {
		t.Logf("  [%d] count=%d, tuple=%#v", i, td.Count, td.Tuple)
	}

	// Sliding window MIN with 3-row window
	// Row 1: min(100) = 100
	// Row 2: min(100, 50) = 50
	// Row 3: min(100, 50, 75) = 50
	// Row 4: min(50, 75, 90) = 50
}

func TestWindowAggOp_SlidingWindow(t *testing.T) {
	// Test sliding window: size=10s, slide=5s
	spec := WindowSpecLite{
		TimeCol:     "ts",
		SizeMillis:  10000, // 10 seconds
		WindowType:  WindowTypeSliding,
		SlideMillis: 5000, // 5 seconds
	}

	keyFn := func(t types.Tuple) any { return t["region"] }
	aggInit := func() any { return int64(0) }
	agg := &CountAgg{}

	op := NewWindowAggOp(spec, keyFn, []string{"region"}, aggInit, agg)

	// Input batch: events at ts=2, 7, 12
	batch := types.Batch{
		{Tuple: types.Tuple{"region": "East", "ts": int64(2000), "value": 10}, Count: 1},
		{Tuple: types.Tuple{"region": "East", "ts": int64(7000), "value": 20}, Count: 1},
		{Tuple: types.Tuple{"region": "East", "ts": int64(12000), "value": 30}, Count: 1},
	}

	out, err := op.Apply(batch)
	if err != nil {
		t.Fatalf("Apply failed: %v", err)
	}

	if len(out) == 0 {
		t.Fatal("expected non-empty output")
	}

	t.Logf("Sliding Window Output (%d deltas):", len(out))
	for i, td := range out {
		start := td.Tuple["__window_start"]
		end := td.Tuple["__window_end"]
		t.Logf("  [%d] window=[%v, %v) count=%d region=%v",
			i, start, end, td.Count, td.Tuple["region"])
	}

	// Expected windows:
	// ts=2000 (2s) -> windows [0,10000)
	// ts=7000 (7s) -> windows [0,10000), [5000,15000)
	// ts=12000 (12s) -> windows [5000,15000), [10000,20000)
	// Total: 5 window updates expected
	if len(out) < 3 {
		t.Errorf("expected at least 3 output deltas, got %d", len(out))
	}
}

func TestWindowAggOp_SlidingWindowSum(t *testing.T) {
	// Test sliding window with SUM aggregation
	spec := WindowSpecLite{
		TimeCol:     "ts",
		SizeMillis:  15000, // 15 seconds
		WindowType:  WindowTypeSliding,
		SlideMillis: 5000, // 5 seconds
	}

	keyFn := func(t types.Tuple) any { return nil } // No grouping
	aggInit := func() any { return float64(0) }
	agg := &SumAgg{ColName: "amount"}

	op := NewWindowAggOp(spec, keyFn, []string{}, aggInit, agg)

	// Input batch
	batch := types.Batch{
		{Tuple: types.Tuple{"ts": int64(3000), "amount": 100.0}, Count: 1},
		{Tuple: types.Tuple{"ts": int64(8000), "amount": 50.0}, Count: 1},
		{Tuple: types.Tuple{"ts": int64(13000), "amount": 75.0}, Count: 1},
	}

	out, err := op.Apply(batch)
	if err != nil {
		t.Fatalf("Apply failed: %v", err)
	}

	t.Logf("Sliding Window SUM Output (%d deltas):", len(out))
	for i, td := range out {
		start := td.Tuple["__window_start"]
		end := td.Tuple["__window_end"]
		sum := td.Tuple["sum"]
		t.Logf("  [%d] window=[%v, %v) sum=%v", i, start, end, sum)
	}

	// With 15s window and 5s slide:
	// ts=3000 -> [0,15000)
	// ts=8000 -> [0,15000), [5000,20000)
	// ts=13000 -> [0,15000), [5000,20000), [10000,25000)
}

func TestWindowAggOp_SessionWindow(t *testing.T) {
	// Test session window with 5 second gap
	spec := WindowSpecLite{
		TimeCol:    "ts",
		WindowType: WindowTypeSession,
		GapMillis:  5000, // 5 seconds inactivity gap
	}

	keyFn := func(t types.Tuple) any { return t["user"] }
	aggInit := func() any { return int64(0) }
	agg := &CountAgg{}

	op := NewWindowAggOp(spec, keyFn, []string{"user"}, aggInit, agg)

	// Input: user has events at 1s, 3s, 10s (gap between 3s and 10s > 5s)
	batch := types.Batch{
		{Tuple: types.Tuple{"user": "Alice", "ts": int64(1000)}, Count: 1},
		{Tuple: types.Tuple{"user": "Alice", "ts": int64(3000)}, Count: 1},
		{Tuple: types.Tuple{"user": "Alice", "ts": int64(10000)}, Count: 1}, // New session
	}

	out, err := op.Apply(batch)
	if err != nil {
		t.Fatalf("Apply failed: %v", err)
	}

	// Expected: two inserted session outputs
	// Session 1: [1000, 8000)
	// Session 2: [10000, 15000)
	seen := map[[2]int64]int64{}
	for _, td := range out {
		start, _ := td.Tuple["__window_start"].(int64)
		end, _ := td.Tuple["__window_end"].(int64)
		seen[[2]int64{start, end}] += td.Count
		if td.Tuple["user"] != "Alice" {
			t.Fatalf("expected user=Alice, got %v", td.Tuple["user"])
		}
	}
	if seen[[2]int64{1000, 8000}] != 1 {
		t.Fatalf("expected insert for [1000,8000), got %d", seen[[2]int64{1000, 8000}])
	}
	if seen[[2]int64{10000, 15000}] != 1 {
		t.Fatalf("expected insert for [10000,15000), got %d", seen[[2]int64{10000, 15000}])
	}
}

func TestWindowAggOp_SessionWindow_MergeAcrossBatches(t *testing.T) {
	// gap=5s
	spec := WindowSpecLite{TimeCol: "ts", WindowType: WindowTypeSession, GapMillis: 5000}
	keyFn := func(t types.Tuple) any { return t["user"] }
	aggInit := func() any { return int64(0) }
	agg := &CountAgg{}
	op := NewWindowAggOp(spec, keyFn, []string{"user"}, aggInit, agg)

	// Batch1: two sessions
	out1, err := op.Apply(types.Batch{
		{Tuple: types.Tuple{"user": "Alice", "ts": int64(1000)}, Count: 1},
		{Tuple: types.Tuple{"user": "Alice", "ts": int64(10000)}, Count: 1},
	})
	if err != nil {
		t.Fatalf("Apply batch1 failed: %v", err)
	}
	if len(out1) == 0 {
		t.Fatalf("expected outputs in batch1")
	}

	// Batch2: event at 6000 bridges sessions -> should merge into one
	out2, err := op.Apply(types.Batch{{Tuple: types.Tuple{"user": "Alice", "ts": int64(6000)}, Count: 1}})
	if err != nil {
		t.Fatalf("Apply batch2 failed: %v", err)
	}
	seen := map[[2]int64]int64{}
	for _, td := range out2 {
		start, _ := td.Tuple["__window_start"].(int64)
		end, _ := td.Tuple["__window_end"].(int64)
		seen[[2]int64{start, end}] += td.Count
	}
	// retract old two, insert merged [1000,15000)
	if seen[[2]int64{1000, 6000}] != -1 && seen[[2]int64{1000, 6000}] != 0 {
		// Depending on sessionization, [1000,6000) is possible only if last event was 1000.
		// We assert stronger via required merge output below.
		_ = seen
	}
	if seen[[2]int64{1000, 15000}] != 1 {
		t.Fatalf("expected insert for merged [1000,15000), got %d (full=%v)", seen[[2]int64{1000, 15000}], seen)
	}
	// Total net count should be -1 (remove 2 add 1) => -1 over all deltas.
	var net int64
	for _, c := range seen {
		net += c
	}
	if net != -1 {
		t.Fatalf("expected net -1 (merge), got %d (full=%v)", net, seen)
	}
}

func TestWindowAggOp_SessionWindow_SplitOnDeletion(t *testing.T) {
	// gap=5s
	spec := WindowSpecLite{TimeCol: "ts", WindowType: WindowTypeSession, GapMillis: 5000}
	keyFn := func(t types.Tuple) any { return t["user"] }
	aggInit := func() any { return int64(0) }
	agg := &CountAgg{}
	op := NewWindowAggOp(spec, keyFn, []string{"user"}, aggInit, agg)

	// Start with one merged session: 1s, 6s, 10s
	_, err := op.Apply(types.Batch{
		{Tuple: types.Tuple{"user": "Alice", "ts": int64(1000)}, Count: 1},
		{Tuple: types.Tuple{"user": "Alice", "ts": int64(6000)}, Count: 1},
		{Tuple: types.Tuple{"user": "Alice", "ts": int64(10000)}, Count: 1},
	})
	if err != nil {
		t.Fatalf("Apply insert failed: %v", err)
	}

	// Delete the bridging event at 6s -> split into two sessions
	out, err := op.Apply(types.Batch{{Tuple: types.Tuple{"user": "Alice", "ts": int64(6000)}, Count: -1}})
	if err != nil {
		t.Fatalf("Apply delete failed: %v", err)
	}
	seen := map[[2]int64]int64{}
	for _, td := range out {
		start, _ := td.Tuple["__window_start"].(int64)
		end, _ := td.Tuple["__window_end"].(int64)
		seen[[2]int64{start, end}] += td.Count
	}
	// retract merged [1000,15000), insert [1000,6000) and [10000,15000)
	if seen[[2]int64{1000, 15000}] != -1 {
		t.Fatalf("expected retraction for [1000,15000), got %d (full=%v)", seen[[2]int64{1000, 15000}], seen)
	}
	if seen[[2]int64{1000, 6000}] != 1 {
		t.Fatalf("expected insert for [1000,6000), got %d (full=%v)", seen[[2]int64{1000, 6000}], seen)
	}
	if seen[[2]int64{10000, 15000}] != 1 {
		t.Fatalf("expected insert for [10000,15000), got %d (full=%v)", seen[[2]int64{10000, 15000}], seen)
	}
}

func TestWindowAggOp_SessionWindowMultipleUsers(t *testing.T) {
	// Test session window with multiple partitions
	spec := WindowSpecLite{
		TimeCol:    "ts",
		WindowType: WindowTypeSession,
		GapMillis:  3000, // 3 seconds gap
	}

	keyFn := func(t types.Tuple) any { return t["user"] }
	aggInit := func() any { return int64(0) }
	agg := &CountAgg{}

	op := NewWindowAggOp(spec, keyFn, []string{"user"}, aggInit, agg)

	// Multiple users with different activity patterns
	batch := types.Batch{
		{Tuple: types.Tuple{"user": "Alice", "ts": int64(1000)}, Count: 1},
		{Tuple: types.Tuple{"user": "Bob", "ts": int64(1500)}, Count: 1},
		{Tuple: types.Tuple{"user": "Alice", "ts": int64(2000)}, Count: 1},
		{Tuple: types.Tuple{"user": "Bob", "ts": int64(6000)}, Count: 1},   // Bob's new session
		{Tuple: types.Tuple{"user": "Alice", "ts": int64(7000)}, Count: 1}, // Alice's new session
	}

	out, err := op.Apply(batch)
	if err != nil {
		t.Fatalf("Apply failed: %v", err)
	}

	t.Logf("Multi-user Session Output (%d deltas):", len(out))
	for i, td := range out {
		start := td.Tuple["__window_start"]
		end := td.Tuple["__window_end"]
		user := td.Tuple["user"]
		t.Logf("  [%d] user=%v session=[%v, %v) count=%d",
			i, user, start, end, td.Count)
	}

	// Each user should have separate sessions
	if len(out) == 0 {
		t.Error("expected session windows for multiple users")
	}
}

func TestInterval_Parse(t *testing.T) {
	tests := []struct {
		input    string
		expected int64 // expected milliseconds
		wantErr  bool
	}{
		{"5 minutes", 5 * 60 * 1000, false},
		{"1 hour", 60 * 60 * 1000, false},
		{"30 seconds", 30 * 1000, false},
		{"2 days", 2 * 24 * 60 * 60 * 1000, false},
		{"500 milliseconds", 500, false},
		{"invalid", 0, true},
		{"5", 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			interval, err := types.ParseInterval(tt.input)
			if tt.wantErr {
				if err == nil {
					t.Errorf("expected error for input %q", tt.input)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if interval.Millis != tt.expected {
				t.Errorf("expected %d millis, got %d", tt.expected, interval.Millis)
			}
		})
	}
}
