package ir

import (
	"sort"
	"testing"

	"github.com/ariyn/dbsp/internal/dbsp/op"
	"github.com/ariyn/dbsp/internal/dbsp/types"
)

type aggSnapshot struct {
	sum   float64
	count int64
}

func applyAggDeltas(out types.Batch, snap map[string]aggSnapshot) {
	for _, td := range out {
		k, _ := td.Tuple["a.k"].(string)
		if k == "" {
			k, _ = td.Tuple["k"].(string)
		}
		if k == "" {
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

func assertAggSnapshotsEqual(t *testing.T, got, want map[string]aggSnapshot) {
	t.Helper()

	if len(got) != len(want) {
		t.Fatalf("snapshot key count mismatch: got=%d want=%d (gotKeys=%v wantKeys=%v)", len(got), len(want), mapKeys(got), mapKeys(want))
	}
	for k, w := range want {
		g, ok := got[k]
		if !ok {
			t.Fatalf("missing key %q in got snapshot (gotKeys=%v)", k, mapKeys(got))
		}
		if g.sum != w.sum || g.count != w.count {
			t.Fatalf("snapshot mismatch for key %q: got(sum=%v,count=%v) want(sum=%v,count=%v)", k, g.sum, g.count, w.sum, w.count)
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

func TestLogicalToDBSP_Join_Structure(t *testing.T) {
	join := &LogicalJoin{
		LeftTable:  "a",
		RightTable: "b",
		Conditions: []JoinCondition{{LeftCol: "a.id", RightCol: "b.id"}},
		Left:       &LogicalScan{Table: "a"},
		Right:      &LogicalScan{Table: "b"},
	}

	n, err := LogicalToDBSP(join)
	if err != nil {
		t.Fatalf("LogicalToDBSP: %v", err)
	}
	bop, ok := n.Op.(*op.BinaryOp)
	if !ok {
		t.Fatalf("expected *op.BinaryOp, got %T", n.Op)
	}
	if bop.Type != op.BinaryJoin {
		t.Fatalf("expected BinaryJoin, got %v", bop.Type)
	}
	if len(n.Inputs) != 2 {
		t.Fatalf("expected 2 inputs for join node, got %d", len(n.Inputs))
	}
	if n.Inputs[0].Source != "a" || n.Inputs[1].Source != "b" {
		t.Fatalf("unexpected join sources: left=%q right=%q", n.Inputs[0].Source, n.Inputs[1].Source)
	}
}

func TestLogicalToDBSP_JoinGroupAgg_MultiAgg_StructureAndExecute(t *testing.T) {
	join := &LogicalJoin{
		LeftTable:  "a",
		RightTable: "b",
		Conditions: []JoinCondition{{LeftCol: "a.id", RightCol: "b.id"}},
		Left:       &LogicalScan{Table: "a"},
		Right:      &LogicalScan{Table: "b"},
	}
	g := &LogicalGroupAgg{
		Keys:  []string{"a.k"},
		Aggs:  []AggSpec{{Name: "SUM", Col: "b.v"}, {Name: "COUNT", Col: "b.id"}},
		Input: join,
	}

	root, err := LogicalToDBSP(g)
	if err != nil {
		t.Fatalf("LogicalToDBSP: %v", err)
	}
	gop, ok := root.Op.(*op.GroupAggOp)
	if !ok {
		t.Fatalf("expected *op.GroupAggOp, got %T", root.Op)
	}
	if len(gop.Aggs) != 2 {
		t.Fatalf("expected 2 aggregate slots, got %d", len(gop.Aggs))
	}
	if len(root.Inputs) != 1 {
		t.Fatalf("expected root to have join input, got %d", len(root.Inputs))
	}
	if _, ok := root.Inputs[0].Op.(*op.BinaryOp); !ok {
		t.Fatalf("expected join node op to be *op.BinaryOp, got %T", root.Inputs[0].Op)
	}

	snap := make(map[string]aggSnapshot)

	ticks := []map[string]types.Batch{
		{"a": {{Tuple: types.Tuple{"a.id": int64(1), "a.k": "A"}, Count: 1}}},
		{"a": {{Tuple: types.Tuple{"a.id": int64(2), "a.k": "B"}, Count: 1}}},
		{"b": {{Tuple: types.Tuple{"b.id": int64(1), "b.v": 10.0}, Count: 1}}},
		{"b": {{Tuple: types.Tuple{"b.id": int64(1), "b.v": 5.0}, Count: 1}}},
		{"b": {{Tuple: types.Tuple{"b.id": int64(2), "b.v": 7.0}, Count: 1}}},
		{"b": {{Tuple: types.Tuple{"b.id": int64(1), "b.v": 5.0}, Count: -1}}},
	}
	for _, in := range ticks {
		out, err := op.ExecuteTick(root, in)
		if err != nil {
			t.Fatalf("ExecuteTick: %v", err)
		}
		applyAggDeltas(out, snap)
	}

	want := map[string]aggSnapshot{
		"A": {sum: 10.0, count: 1},
		"B": {sum: 7.0, count: 1},
	}
	assertAggSnapshotsEqual(t, snap, want)
}

func TestLogicalToDBSP_JoinGroupAgg_MultiAgg_CountStarAllowed(t *testing.T) {
	join := &LogicalJoin{
		LeftTable:  "a",
		RightTable: "b",
		Conditions: []JoinCondition{{LeftCol: "a.id", RightCol: "b.id"}},
		Left:       &LogicalScan{Table: "a"},
		Right:      &LogicalScan{Table: "b"},
	}
	g := &LogicalGroupAgg{
		Keys:  []string{"a.k"},
		Aggs:  []AggSpec{{Name: "SUM", Col: "b.v"}, {Name: "COUNT", Col: ""}},
		Input: join,
	}

	root, err := LogicalToDBSP(g)
	if err != nil {
		t.Fatalf("LogicalToDBSP: %v", err)
	}
	gop, ok := root.Op.(*op.GroupAggOp)
	if !ok {
		t.Fatalf("expected *op.GroupAggOp, got %T", root.Op)
	}
	if len(gop.Aggs) != 2 {
		t.Fatalf("expected 2 aggregate slots, got %d", len(gop.Aggs))
	}

	snap := make(map[string]aggSnapshot)
	ticks := []map[string]types.Batch{
		{"a": {{Tuple: types.Tuple{"a.id": int64(1), "a.k": "A"}, Count: 1}}},
		{"a": {{Tuple: types.Tuple{"a.id": int64(2), "a.k": "B"}, Count: 1}}},
		{"b": {{Tuple: types.Tuple{"b.id": int64(1), "b.v": 10.0}, Count: 1}}},
		{"b": {{Tuple: types.Tuple{"b.id": int64(2), "b.v": 7.0}, Count: 1}}},
		{"b": {{Tuple: types.Tuple{"b.id": int64(1), "b.v": 5.0}, Count: 1}}},
		{"b": {{Tuple: types.Tuple{"b.id": int64(1), "b.v": 5.0}, Count: -1}}},
	}
	for _, in := range ticks {
		out, err := op.ExecuteTick(root, in)
		if err != nil {
			t.Fatalf("ExecuteTick: %v", err)
		}
		applyAggDeltas(out, snap)
	}

	want := map[string]aggSnapshot{
		"A": {sum: 10.0, count: 1},
		"B": {sum: 7.0, count: 1},
	}
	assertAggSnapshotsEqual(t, snap, want)
}

func TestLogicalToDBSP_FilterOverJoinGroupAgg_StructureAndExecute(t *testing.T) {
	join := &LogicalJoin{
		LeftTable:  "a",
		RightTable: "b",
		Conditions: []JoinCondition{{LeftCol: "a.id", RightCol: "b.id"}},
		Left:       &LogicalScan{Table: "a"},
		Right:      &LogicalScan{Table: "b"},
	}
	filter := &LogicalFilter{PredicateSQL: "b.v >= 10", Input: join}
	g := &LogicalGroupAgg{
		Keys:  []string{"a.k"},
		Aggs:  []AggSpec{{Name: "SUM", Col: "b.v"}, {Name: "COUNT", Col: "b.id"}},
		Input: filter,
	}

	root, err := LogicalToDBSP(g)
	if err != nil {
		t.Fatalf("LogicalToDBSP: %v", err)
	}
	ch, ok := root.Op.(*op.ChainedOp)
	if !ok {
		t.Fatalf("expected *op.ChainedOp, got %T", root.Op)
	}
	if len(ch.Ops) != 2 {
		t.Fatalf("expected 2 chained ops (filter, agg), got %d", len(ch.Ops))
	}
	if len(root.Inputs) != 1 {
		t.Fatalf("expected join as single input, got %d", len(root.Inputs))
	}
	if _, ok := root.Inputs[0].Op.(*op.BinaryOp); !ok {
		t.Fatalf("expected join node op to be *op.BinaryOp, got %T", root.Inputs[0].Op)
	}

	snap := make(map[string]aggSnapshot)
	// Insert both sides; only b.v>=10 should contribute.
	ticks := []map[string]types.Batch{
		{"a": {{Tuple: types.Tuple{"a.id": int64(1), "a.k": "A"}, Count: 1}}},
		{"b": {{Tuple: types.Tuple{"b.id": int64(1), "b.v": 9.0}, Count: 1}}},
		{"b": {{Tuple: types.Tuple{"b.id": int64(1), "b.v": 10.0}, Count: 1}}},
	}
	for _, in := range ticks {
		out, err := op.ExecuteTick(root, in)
		if err != nil {
			t.Fatalf("ExecuteTick: %v", err)
		}
		applyAggDeltas(out, snap)
	}

	want := map[string]aggSnapshot{"A": {sum: 10.0, count: 1}}
	assertAggSnapshotsEqual(t, snap, want)
}

func TestLogicalToDBSP_ProjectOverJoin_StructureAndExecute(t *testing.T) {
	join := &LogicalJoin{
		LeftTable:  "a",
		RightTable: "b",
		Conditions: []JoinCondition{{LeftCol: "a.id", RightCol: "b.id"}},
		Left:       &LogicalScan{Table: "a"},
		Right:      &LogicalScan{Table: "b"},
	}
	proj := &LogicalProject{Columns: []string{"a.k", "b.v"}, Input: join}

	root, err := LogicalToDBSP(proj)
	if err != nil {
		t.Fatalf("LogicalToDBSP: %v", err)
	}
	ch, ok := root.Op.(*op.ChainedOp)
	if !ok {
		t.Fatalf("expected *op.ChainedOp, got %T", root.Op)
	}
	if len(ch.Ops) != 1 {
		t.Fatalf("expected 1 chained op (project), got %d", len(ch.Ops))
	}
	if len(root.Inputs) != 1 {
		t.Fatalf("expected join as input, got %d", len(root.Inputs))
	}

	out1, err := op.ExecuteTick(root, map[string]types.Batch{"a": {{Tuple: types.Tuple{"a.id": int64(1), "a.k": "A"}, Count: 1}}})
	if err != nil {
		t.Fatalf("ExecuteTick(a): %v", err)
	}
	if len(out1) != 0 {
		t.Fatalf("expected no output before b arrives, got %v", out1)
	}

	out2, err := op.ExecuteTick(root, map[string]types.Batch{"b": {{Tuple: types.Tuple{"b.id": int64(1), "b.v": 10.0}, Count: 1}}})
	if err != nil {
		t.Fatalf("ExecuteTick(b): %v", err)
	}
	if len(out2) == 0 {
		t.Fatalf("expected join output after b arrives")
	}
	for _, td := range out2 {
		if _, ok := td.Tuple["a.k"]; !ok {
			t.Fatalf("expected projected tuple to include a.k, got %v", td.Tuple)
		}
		if _, ok := td.Tuple["b.v"]; !ok {
			t.Fatalf("expected projected tuple to include b.v, got %v", td.Tuple)
		}
		// should not carry join key column a.id/b.id unless explicitly projected
		if _, ok := td.Tuple["a.id"]; ok {
			t.Fatalf("did not expect a.id in projected tuple, got %v", td.Tuple)
		}
		if _, ok := td.Tuple["b.id"]; ok {
			t.Fatalf("did not expect b.id in projected tuple, got %v", td.Tuple)
		}
	}
}
