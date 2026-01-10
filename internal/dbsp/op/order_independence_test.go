package op

import (
	"encoding/json"
	"testing"

	"github.com/ariyn/dbsp/internal/dbsp/types"
)

func testTupleKey(t types.Tuple) string {
	b, err := json.Marshal(t)
	if err == nil {
		return string(b)
	}
	return "<unmarshalable>"
}

func testBatchMultiset(b types.Batch) map[string]int64 {
	m := make(map[string]int64)
	for _, td := range b {
		m[testTupleKey(td.Tuple)] += td.Count
	}
	for k, v := range m {
		if v == 0 {
			delete(m, k)
		}
	}
	return m
}

func testAssertMultisetEqual(t *testing.T, got, want map[string]int64) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("multiset size mismatch: got=%v want=%v", got, want)
	}
	for k, v := range want {
		if got[k] != v {
			t.Fatalf("multiset mismatch: got=%v want=%v", got, want)
		}
	}
}

func TestOrderIndependence_Join_InsertOnly_OutputMultisetEqual(t *testing.T) {
	newJoin := func() *BinaryOp {
		return NewJoinOp(
			func(t types.Tuple) any { return t["k"] },
			func(t types.Tuple) any { return t["k"] },
			func(l, r types.Tuple) types.Tuple {
				return types.Tuple{"k": l["k"], "l": l["l"], "r": r["r"]}
			},
		)
	}

	leftA := types.Batch{
		{Tuple: types.Tuple{"k": "x", "l": 1}, Count: 1},
		{Tuple: types.Tuple{"k": "x", "l": 2}, Count: 1},
	}
	rightA := types.Batch{
		{Tuple: types.Tuple{"k": "x", "r": 10}, Count: 1},
		{Tuple: types.Tuple{"k": "x", "r": 20}, Count: 1},
	}
	leftB := types.Batch{leftA[1], leftA[0]}
	rightB := types.Batch{rightA[1], rightA[0]}

	out1, err := newJoin().ApplyBinary(leftA, rightA)
	if err != nil {
		t.Fatalf("ApplyBinary A failed: %v", err)
	}
	out2, err := newJoin().ApplyBinary(leftB, rightB)
	if err != nil {
		t.Fatalf("ApplyBinary B failed: %v", err)
	}

	m1 := testBatchMultiset(out1)
	m2 := testBatchMultiset(out2)
	testAssertMultisetEqual(t, m1, m2)
}

func TestOrderIndependence_GroupAggSum_ReorderBatch_OutputAndStateEqual(t *testing.T) {
	keyFn := func(t types.Tuple) any { return t["k"] }
	aggInit := func() any { return float64(0) }
	agg := &SumAgg{ColName: "v"}

	mk := func() *GroupAggOp {
		op := NewGroupAggOp(keyFn, aggInit, agg)
		op.SetKeyColName("k")
		return op
	}

	batchA := types.Batch{
		{Tuple: types.Tuple{"k": "A", "v": float64(10)}, Count: 1},
		{Tuple: types.Tuple{"k": "B", "v": float64(7)}, Count: 1},
		{Tuple: types.Tuple{"k": "A", "v": float64(5)}, Count: 1},
	}
	batchB := types.Batch{batchA[2], batchA[0], batchA[1]}

	op1 := mk()
	out1, err := op1.Apply(batchA)
	if err != nil {
		t.Fatalf("Apply A failed: %v", err)
	}
	op2 := mk()
	out2, err := op2.Apply(batchB)
	if err != nil {
		t.Fatalf("Apply B failed: %v", err)
	}

	testAssertMultisetEqual(t, testBatchMultiset(out1), testBatchMultiset(out2))

	st1 := op1.State()
	st2 := op2.State()
	if st1["A"].(float64) != st2["A"].(float64) || st1["B"].(float64) != st2["B"].(float64) {
		t.Fatalf("state mismatch: st1=%v st2=%v", st1, st2)
	}
}

func TestOrderIndependence_GroupAggSum_CancellationSameBatch_OrderIndependent(t *testing.T) {
	keyFn := func(t types.Tuple) any { return t["k"] }
	aggInit := func() any { return float64(0) }
	agg := &SumAgg{ColName: "v"}

	mk := func() *GroupAggOp {
		op := NewGroupAggOp(keyFn, aggInit, agg)
		op.SetKeyColName("k")
		return op
	}

	ins := types.TupleDelta{Tuple: types.Tuple{"k": "A", "v": float64(10)}, Count: 1}
	del := types.TupleDelta{Tuple: types.Tuple{"k": "A", "v": float64(10)}, Count: -1}

	op1 := mk()
	out1, err := op1.Apply(types.Batch{ins, del})
	if err != nil {
		t.Fatalf("Apply ins+del failed: %v", err)
	}
	op2 := mk()
	out2, err := op2.Apply(types.Batch{del, ins})
	if err != nil {
		t.Fatalf("Apply del+ins failed: %v", err)
	}

	// Net effect should cancel out.
	m1 := testBatchMultiset(out1)
	m2 := testBatchMultiset(out2)
	if len(m1) != 0 || len(m2) != 0 {
		t.Fatalf("expected net-zero output multiset, got m1=%v m2=%v", m1, m2)
	}

	st1 := op1.State()
	st2 := op2.State()
	if st1["A"].(float64) != 0 || st2["A"].(float64) != 0 {
		t.Fatalf("expected state A=0 after cancellation, got st1=%v st2=%v", st1, st2)
	}
}

func TestOrderIndependence_WindowTumblingSum_ReorderBatch_OutputAndStateEqual(t *testing.T) {
	spec := WindowSpecLite{TimeCol: "ts", SizeMillis: 10, WindowType: WindowTypeTumbling}
	keyFn := func(t types.Tuple) any { return t["region"] }
	aggInit := func() any { return float64(0) }
	agg := &SumAgg{ColName: "v"}

	mk := func() *WindowAggOp {
		return NewWindowAggOp(spec, keyFn, []string{"region"}, aggInit, agg)
	}

	batchA := types.Batch{
		{Tuple: types.Tuple{"region": "East", "ts": int64(1), "v": float64(10)}, Count: 1},
		{Tuple: types.Tuple{"region": "East", "ts": int64(2), "v": float64(5)}, Count: 1},
	}
	batchB := types.Batch{batchA[1], batchA[0]}

	op1 := mk()
	out1, err := op1.Apply(batchA)
	if err != nil {
		t.Fatalf("Apply A failed: %v", err)
	}
	op2 := mk()
	out2, err := op2.Apply(batchB)
	if err != nil {
		t.Fatalf("Apply B failed: %v", err)
	}

	testAssertMultisetEqual(t, testBatchMultiset(out1), testBatchMultiset(out2))

	wid := WindowID{Start: 0, End: 10}
	st1 := op1.State.Data[wid]["East"].(float64)
	st2 := op2.State.Data[wid]["East"].(float64)
	if st1 != st2 || st1 != 15 {
		t.Fatalf("unexpected state: st1=%v st2=%v", st1, st2)
	}
}

func TestOrderIndependence_WindowSlidingCount_CancellationSameBatch_OrderIndependent(t *testing.T) {
	spec := WindowSpecLite{TimeCol: "ts", SizeMillis: 10, SlideMillis: 5, WindowType: WindowTypeSliding}
	keyFn := func(t types.Tuple) any { return nil }
	aggInit := func() any { return int64(0) }
	agg := &CountAgg{}

	mk := func() *WindowAggOp {
		return NewWindowAggOp(spec, keyFn, []string{}, aggInit, agg)
	}

	ins := types.TupleDelta{Tuple: types.Tuple{"ts": int64(7)}, Count: 1}
	del := types.TupleDelta{Tuple: types.Tuple{"ts": int64(7)}, Count: -1}

	op1 := mk()
	out1, err := op1.Apply(types.Batch{ins, del})
	if err != nil {
		t.Fatalf("Apply ins+del failed: %v", err)
	}
	op2 := mk()
	out2, err := op2.Apply(types.Batch{del, ins})
	if err != nil {
		t.Fatalf("Apply del+ins failed: %v", err)
	}

	m1 := testBatchMultiset(out1)
	m2 := testBatchMultiset(out2)
	if len(m1) != 0 || len(m2) != 0 {
		t.Fatalf("expected net-zero output multiset, got m1=%v m2=%v", m1, m2)
	}

	// Both windows that would have been affected should end at 0.
	if got := op1.State.Data[WindowID{Start: 0, End: 10}][nil].(int64); got != 0 {
		t.Fatalf("expected state [0,10) = 0, got %v", got)
	}
	if got := op1.State.Data[WindowID{Start: 5, End: 15}][nil].(int64); got != 0 {
		t.Fatalf("expected state [5,15) = 0, got %v", got)
	}
	if got := op2.State.Data[WindowID{Start: 0, End: 10}][nil].(int64); got != 0 {
		t.Fatalf("expected state [0,10) = 0, got %v", got)
	}
	if got := op2.State.Data[WindowID{Start: 5, End: 15}][nil].(int64); got != 0 {
		t.Fatalf("expected state [5,15) = 0, got %v", got)
	}
}
