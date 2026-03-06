package op

import (
	"fmt"
	"testing"

	"github.com/ariyn/dbsp/internal/dbsp/types"
)

type batchRecorderOp struct {
	received []types.Batch
}

func (b *batchRecorderOp) Apply(batch types.Batch) (types.Batch, error) {
	b.received = append(b.received, types.CloneBatch(batch))
	return types.CloneBatch(batch), nil
}

func TestExecuteTickExpandsMultiSourceBatchesRoundRobin(t *testing.T) {
	recorder := &batchRecorderOp{}
	root := &Node{
		Op: recorder,
		Inputs: []*Node{{
			Op: NewUnionOp(),
			Inputs: []*Node{
				{Source: "left"},
				{Source: "right"},
			},
		}},
	}

	out, err := ExecuteTick(root, map[string]types.Batch{
		"left": {
			{Tuple: types.Tuple{"source": "left", "seq": int64(1)}, Count: 1},
			{Tuple: types.Tuple{"source": "left", "seq": int64(2)}, Count: 1},
		},
		"right": {
			{Tuple: types.Tuple{"source": "right", "seq": int64(1)}, Count: 1},
		},
	})
	if err != nil {
		t.Fatalf("ExecuteTick failed: %v", err)
	}

	if len(recorder.received) != 3 {
		t.Fatalf("expected 3 micro-ticks, got %d", len(recorder.received))
	}
	for idx, batch := range recorder.received {
		if len(batch) != 1 {
			t.Fatalf("expected micro-tick %d to have exactly 1 delta, got %d", idx, len(batch))
		}
	}

	got := make([]string, 0, len(out))
	for _, td := range out {
		got = append(got, fmt.Sprintf("%v:%v", td.Tuple["source"], td.Tuple["seq"]))
	}
	want := []string{"left:1", "right:1", "left:2"}
	if fmt.Sprintf("%v", got) != fmt.Sprintf("%v", want) {
		t.Fatalf("unexpected output order: got %v want %v", got, want)
	}
}

func TestExecuteTickExpandedBatchesMatchManualMicroTicksForStatefulChain(t *testing.T) {
	buildRoot := func() *Node {
		group := NewGroupAggOp(
			func(t types.Tuple) any { return fmt.Sprintf("%v|%v", t["id"], t["bucket"]) },
			func() any { return float64(0) },
			&SumAgg{ColName: "energy", DeltaCol: "energy"},
		)
		group.EmitValue = true
		group.SetGroupKeyColNames([]string{"id", "bucket"})

		window := NewWindowAggOp(
			WindowSpecLite{},
			func(t types.Tuple) any { return t["id"] },
			[]string{"id"},
			func() any { return float64(0) },
			&SumAgg{ColName: "energy"},
		)
		window.OrderByCol = "bucket"
		window.FrameSpec = &FrameSpecLite{Type: "ROWS", StartType: "UNBOUNDED PRECEDING", EndType: "CURRENT ROW"}
		window.KeepInput = true
		window.EmitValue = true

		union := &Node{Op: NewUnionOp(), Inputs: []*Node{{Source: "left"}, {Source: "right"}}}
		groupNode := &Node{Op: group, Inputs: []*Node{union}}
		return &Node{Op: window, Inputs: []*Node{groupNode}}
	}

	sources := map[string]types.Batch{
		"left": {
			{Tuple: types.Tuple{"id": "a", "bucket": int64(1), "energy": 1.0}, Count: 1},
			{Tuple: types.Tuple{"id": "a", "bucket": int64(2), "energy": 3.0}, Count: 1},
		},
		"right": {
			{Tuple: types.Tuple{"id": "a", "bucket": int64(1), "energy": 2.0}, Count: 1},
		},
	}

	root := buildRoot()
	out, err := ExecuteTick(root, sources)
	if err != nil {
		t.Fatalf("ExecuteTick failed: %v", err)
	}
	gotSnapshot := materializeSnapshotByKey(out, "id", "bucket")

	manualRoot := buildRoot()
	var manualOut types.Batch
	steps := []map[string]types.Batch{
		{"left": {{Tuple: types.Tuple{"id": "a", "bucket": int64(1), "energy": 1.0}, Count: 1}}},
		{"right": {{Tuple: types.Tuple{"id": "a", "bucket": int64(1), "energy": 2.0}, Count: 1}}},
		{"left": {{Tuple: types.Tuple{"id": "a", "bucket": int64(2), "energy": 3.0}, Count: 1}}},
	}
	for _, step := range steps {
		stepOut, err := ExecuteTick(manualRoot, step)
		if err != nil {
			t.Fatalf("manual ExecuteTick failed: %v", err)
		}
		manualOut = append(manualOut, stepOut...)
	}
	wantSnapshot := materializeSnapshotByKey(manualOut, "id", "bucket")

	if !types.EqualAny(gotSnapshot, wantSnapshot) {
		t.Fatalf("expanded tick snapshot mismatch: got=%v want=%v", gotSnapshot, wantSnapshot)
	}

	row1, ok := gotSnapshot["a|1"]
	if !ok {
		t.Fatalf("missing bucket 1 snapshot row: %v", gotSnapshot)
	}
	if got := types.ToFloat64(row1["agg_result"]); got != 3.0 {
		t.Fatalf("expected bucket 1 cumulative 3.0, got %v (row=%v)", got, row1)
	}
	row2, ok := gotSnapshot["a|2"]
	if !ok {
		t.Fatalf("missing bucket 2 snapshot row: %v", gotSnapshot)
	}
	if got := types.ToFloat64(row2["agg_result"]); got != 6.0 {
		t.Fatalf("expected bucket 2 cumulative 6.0, got %v (row=%v)", got, row2)
	}
}

func TestExecuteExpandsSingleSourceBatchIntoMicroSteps(t *testing.T) {
	recorder := &batchRecorderOp{}
	root := &Node{Op: recorder, Inputs: []*Node{{Source: "events"}}}

	out, err := Execute(root, types.Batch{
		{Tuple: types.Tuple{"seq": int64(1)}, Count: 1},
		{Tuple: types.Tuple{"seq": int64(2)}, Count: 1},
		{Tuple: types.Tuple{"seq": int64(3)}, Count: 1},
	})
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	if len(recorder.received) != 3 {
		t.Fatalf("expected 3 micro-steps, got %d", len(recorder.received))
	}
	for idx, batch := range recorder.received {
		if len(batch) != 1 {
			t.Fatalf("expected step %d to receive 1 delta, got %d", idx, len(batch))
		}
	}

	got := make([]string, 0, len(out))
	for _, td := range out {
		got = append(got, fmt.Sprintf("%v", td.Tuple["seq"]))
	}
	want := []string{"1", "2", "3"}
	if fmt.Sprintf("%v", got) != fmt.Sprintf("%v", want) {
		t.Fatalf("unexpected output order: got %v want %v", got, want)
	}
}

func TestExecuteTickCyclicExpandsBatchPerMicroTick(t *testing.T) {
	root := &Node{
		Op:     NewDelayOp(types.Batch{{Tuple: types.Tuple{"v": int64(0)}, Count: 1}}),
		Inputs: []*Node{{Source: "events"}},
	}

	out, err := ExecuteTick(root, map[string]types.Batch{
		"events": {
			{Tuple: types.Tuple{"v": int64(10)}, Count: 1},
			{Tuple: types.Tuple{"v": int64(20)}, Count: 1},
			{Tuple: types.Tuple{"v": int64(30)}, Count: 1},
		},
	})
	if err != nil {
		t.Fatalf("ExecuteTick failed: %v", err)
	}

	if len(out) != 3 {
		t.Fatalf("expected 3 delay outputs, got %d", len(out))
	}
	got := make([]int64, 0, len(out))
	for _, td := range out {
		got = append(got, types.ToInt64(td.Tuple["v"]))
	}
	want := []int64{0, 10, 20}
	if fmt.Sprintf("%v", got) != fmt.Sprintf("%v", want) {
		t.Fatalf("unexpected cyclic micro-tick outputs: got %v want %v", got, want)
	}

	next, err := ExecuteTick(root, map[string]types.Batch{"events": {{Tuple: types.Tuple{"v": int64(40)}, Count: 1}}})
	if err != nil {
		t.Fatalf("ExecuteTick second tick failed: %v", err)
	}
	if len(next) != 1 || types.ToInt64(next[0].Tuple["v"]) != 30 {
		t.Fatalf("expected next tick to observe committed prev=30, got %v", next)
	}
}

func TestGroupAggCompactsBatchDeltasPerKey(t *testing.T) {
	g := NewGroupAggOp(
		func(t types.Tuple) any { return t["id"] },
		func() any { return float64(0) },
		&SumAgg{ColName: "v"},
	)
	g.SetGroupKeyColNames([]string{"id"})

	out, err := g.Apply(types.Batch{
		{Tuple: types.Tuple{"id": "a", "v": 1.0}, Count: 1},
		{Tuple: types.Tuple{"id": "a", "v": 2.0}, Count: 1},
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
	if got := types.ToFloat64(g.State()["a"]); got != 3.0 {
		t.Fatalf("expected aggregate state 3.0, got %v", got)
	}
}

func materializeSnapshotByKey(batch types.Batch, keyCols ...string) map[string]types.Tuple {
	snapshot := make(map[string]types.Tuple)
	for _, td := range batch {
		key := tupleKeyForCols(td.Tuple, keyCols...)
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

func tupleKeyForCols(tuple types.Tuple, cols ...string) string {
	if tuple == nil || len(cols) == 0 {
		return ""
	}
	key := ""
	for idx, col := range cols {
		if idx > 0 {
			key += "|"
		}
		key += fmt.Sprintf("%v", tuple[col])
	}
	return key
}
