package sqlconv

import (
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/ariyn/dbsp/internal/dbsp/ir"
	"github.com/ariyn/dbsp/internal/dbsp/op"
	"github.com/ariyn/dbsp/internal/dbsp/types"
)

func batchToCountMap(b types.Batch) map[string]int64 {
	m := make(map[string]int64)
	for _, td := range b {
		m[tupleKey(td.Tuple)] += td.Count
	}
	return m
}

func tupleKey(tup types.Tuple) string {
	keys := make([]string, 0, len(tup))
	for k := range tup {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var sb strings.Builder
	for i, k := range keys {
		if i > 0 {
			sb.WriteString("|")
		}
		sb.WriteString(k)
		sb.WriteString("=")
		sb.WriteString(fmt.Sprintf("%v", tup[k]))
	}
	return sb.String()
}

// ============================================================================
// Query Parsing and Conversion Tests
// ============================================================================

func TestParseAndExecuteSumGroupBy(t *testing.T) {
	q := "SELECT k, SUM(v) FROM t GROUP BY k"
	node, err := ParseQueryToDBSP(q)
	if err != nil {
		t.Fatalf("ParseQueryToDBSP failed: %v", err)
	}
	if node == nil || node.Op == nil {
		t.Fatalf("expected node with op, got nil")
	}

	// feed a delta batch and run
	batch := types.Batch{
		{Tuple: types.Tuple{"k": "A", "v": 2}, Count: 1},
		{Tuple: types.Tuple{"k": "A", "v": 3}, Count: 1},
		{Tuple: types.Tuple{"k": "B", "v": 5}, Count: 1},
	}
	out, err := op.Execute(node, batch)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}
	if len(out) == 0 {
		t.Fatalf("expected non-empty output deltas")
	}

	// run a delete to ensure negative counts handled
	del := types.Batch{{Tuple: types.Tuple{"k": "A", "v": 2}, Count: -1}}
	_, err = op.Execute(node, del)
	if err != nil {
		t.Fatalf("Execute(delete) failed: %v", err)
	}
}

func TestParseAndExecuteSumGroupBy_MultiKey(t *testing.T) {
	q := "SELECT k1, k2, SUM(v) FROM t GROUP BY k1, k2"
	node, err := ParseQueryToDBSP(q)
	if err != nil {
		t.Fatalf("ParseQueryToDBSP failed: %v", err)
	}
	if node == nil || node.Op == nil {
		t.Fatalf("expected node with op, got nil")
	}

	batch := types.Batch{
		{Tuple: types.Tuple{"k1": "A", "k2": int64(1), "v": 2}, Count: 1},
		{Tuple: types.Tuple{"k1": "A", "k2": int64(2), "v": 3}, Count: 1},
		{Tuple: types.Tuple{"k1": "A", "k2": int64(1), "v": 4}, Count: 1},
	}
	out, err := op.Execute(node, batch)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}
	if len(out) == 0 {
		t.Fatalf("expected non-empty output deltas")
	}
	for _, td := range out {
		if _, ok := td.Tuple["k1"]; !ok {
			t.Fatalf("expected output to include k1, got %v", td.Tuple)
		}
		if _, ok := td.Tuple["k2"]; !ok {
			t.Fatalf("expected output to include k2, got %v", td.Tuple)
		}
		if _, ok := td.Tuple["agg_delta"]; !ok {
			t.Fatalf("expected output to include agg_delta, got %v", td.Tuple)
		}
	}

	// Delete one row to ensure multi-key delete path doesn't error.
	del := types.Batch{{Tuple: types.Tuple{"k1": "A", "k2": int64(1), "v": 2}, Count: -1}}
	_, err = op.Execute(node, del)
	if err != nil {
		t.Fatalf("Execute(delete) failed: %v", err)
	}
}

func TestParseAndExecuteMultiAgg_SumAndCount_WithDelete(t *testing.T) {
	q := "SELECT k, SUM(v), COUNT(id) FROM t GROUP BY k"
	node, err := ParseQueryToDBSP(q)
	if err != nil {
		t.Fatalf("ParseQueryToDBSP failed: %v", err)
	}

	ins := types.Batch{
		{Tuple: types.Tuple{"k": "A", "id": int64(1), "v": int64(10)}, Count: 1},
		{Tuple: types.Tuple{"k": "A", "id": int64(2), "v": int64(5)}, Count: 1},
		{Tuple: types.Tuple{"k": "B", "id": int64(3), "v": int64(7)}, Count: 1},
	}
	out1, err := op.Execute(node, ins)
	if err != nil {
		t.Fatalf("Execute(insert) failed: %v", err)
	}
	if len(out1) == 0 {
		t.Fatalf("expected output deltas")
	}

	// Delete one row from A.
	del := types.Batch{{Tuple: types.Tuple{"k": "A", "id": int64(1), "v": int64(10)}, Count: -1}}
	out2, err := op.Execute(node, del)
	if err != nil {
		t.Fatalf("Execute(delete) failed: %v", err)
	}
	if len(out2) != 1 {
		t.Fatalf("expected 1 compacted output delta, got %d (%v)", len(out2), out2)
	}
	if out2[0].Tuple["k"] != "A" {
		t.Fatalf("expected k=A, got %v", out2[0].Tuple["k"])
	}
	if out2[0].Tuple["agg_delta"] != float64(-10) {
		t.Fatalf("expected agg_delta=-10, got %v", out2[0].Tuple["agg_delta"])
	}
	if out2[0].Tuple["count_delta"] != int64(-1) {
		t.Fatalf("expected count_delta=-1, got %v", out2[0].Tuple["count_delta"])
	}
}

func TestParseQueryToIncrementalDBSP(t *testing.T) {
	q := "SELECT k, SUM(v) FROM t GROUP BY k"
	incNode, err := ParseQueryToIncrementalDBSP(q)
	if err != nil {
		t.Fatalf("ParseQueryToIncrementalDBSP failed: %v", err)
	}
	if incNode == nil || incNode.Op == nil {
		t.Fatalf("expected node with op, got nil")
	}

	// The incremental version should handle delta batches
	batch1 := types.Batch{
		{Tuple: types.Tuple{"k": "X", "v": 10}, Count: 1},
		{Tuple: types.Tuple{"k": "Y", "v": 20}, Count: 1},
	}
	out1, err := op.Execute(incNode, batch1)
	if err != nil {
		t.Fatalf("Execute batch1 failed: %v", err)
	}
	if len(out1) == 0 {
		t.Fatalf("expected non-empty output deltas")
	}

	// Apply another batch incrementally
	batch2 := types.Batch{
		{Tuple: types.Tuple{"k": "X", "v": 5}, Count: 1},
		{Tuple: types.Tuple{"k": "Y", "v": 10}, Count: -1},
	}
	out2, err := op.Execute(incNode, batch2)
	if err != nil {
		t.Fatalf("Execute batch2 failed: %v", err)
	}
	if len(out2) == 0 {
		t.Fatalf("expected non-empty output deltas")
	}

	// Verify incremental state
	gop, ok := incNode.Op.(*op.GroupAggOp)
	if !ok {
		t.Fatalf("expected GroupAggOp, got %T", incNode.Op)
	}
	state := gop.State()
	// X: 10 + 5 = 15
	if state["X"] != 15.0 {
		t.Errorf("expected X=15, got %v", state["X"])
	}
	// Y: 20 - 10 = 10
	if state["Y"] != 10.0 {
		t.Errorf("expected Y=10, got %v", state["Y"])
	}
}

// ============================================================================
// JOIN Tests
// ============================================================================

func TestParseQueryJoinSimple(t *testing.T) {
	q := "SELECT a.id, a.k, b.v FROM a JOIN b ON a.id = b.id"
	node, err := ParseQueryToDBSP(q)
	if err != nil {
		t.Fatalf("ParseQueryToDBSP with JOIN failed: %v", err)
	}
	if node == nil || node.Op == nil {
		t.Fatalf("expected node with op, got nil")
	}

	// Drive the join as a true 2-input DAG: feed left and right deltas separately.
	aBatch := types.Batch{
		{Tuple: types.Tuple{"a.id": 1, "a.k": "A"}, Count: 1},
		{Tuple: types.Tuple{"a.id": 2, "a.k": "B"}, Count: 1},
	}
	bBatch := types.Batch{
		{Tuple: types.Tuple{"b.id": 1, "b.v": 10}, Count: 1},
		{Tuple: types.Tuple{"b.id": 3, "b.v": 30}, Count: 1},
	}

	_, err = op.ExecuteTick(node, map[string]types.Batch{"a": aBatch})
	if err != nil {
		t.Fatalf("ExecuteTick(a) failed: %v", err)
	}
	out, err := op.ExecuteTick(node, map[string]types.Batch{"b": bBatch})
	if err != nil {
		t.Fatalf("ExecuteTick(b) failed: %v", err)
	}

	// Only id=1 should join
	if len(out) != 1 {
		t.Errorf("expected 1 joined row, got %d", len(out))
	}
}

func TestParseQueryJoinSimple_DeleteLeft(t *testing.T) {
	q := "SELECT a.id, a.k, b.v FROM a JOIN b ON a.id = b.id"
	node, err := ParseQueryToDBSP(q)
	if err != nil {
		t.Fatalf("ParseQueryToDBSP with JOIN failed: %v", err)
	}
	if node == nil || node.Op == nil {
		t.Fatalf("expected node with op, got nil")
	}

	// Insert both sides to create one joined row.
	aIns := types.Batch{{Tuple: types.Tuple{"a.id": 1, "a.k": "A"}, Count: 1}}
	bIns := types.Batch{{Tuple: types.Tuple{"b.id": 1, "b.v": 10}, Count: 1}}

	_, err = op.ExecuteTick(node, map[string]types.Batch{"a": aIns})
	if err != nil {
		t.Fatalf("ExecuteTick(a insert) failed: %v", err)
	}
	outIns, err := op.ExecuteTick(node, map[string]types.Batch{"b": bIns})
	if err != nil {
		t.Fatalf("ExecuteTick(b insert) failed: %v", err)
	}
	gotIns := batchToCountMap(outIns)
	wantIns := map[string]int64{tupleKey(types.Tuple{"a.id": 1, "a.k": "A", "b.v": 10}): 1}
	if len(gotIns) != len(wantIns) {
		t.Fatalf("insert: expected %d distinct tuples, got %d (%+v)", len(wantIns), len(gotIns), outIns)
	}
	for k, want := range wantIns {
		if got := gotIns[k]; got != want {
			t.Fatalf("insert: tuple %q count mismatch: got %d, want %d (out=%+v)", k, got, want, outIns)
		}
	}

	// Delete left row; should retract the joined output.
	aDel := types.Batch{{Tuple: types.Tuple{"a.id": 1, "a.k": "A"}, Count: -1}}
	outDel, err := op.ExecuteTick(node, map[string]types.Batch{"a": aDel})
	if err != nil {
		t.Fatalf("ExecuteTick(a delete) failed: %v", err)
	}
	gotDel := batchToCountMap(outDel)
	wantDel := map[string]int64{tupleKey(types.Tuple{"a.id": 1, "a.k": "A", "b.v": 10}): -1}
	if len(gotDel) != len(wantDel) {
		t.Fatalf("delete: expected %d distinct tuples, got %d (%+v)", len(wantDel), len(gotDel), outDel)
	}
	for k, want := range wantDel {
		if got := gotDel[k]; got != want {
			t.Fatalf("delete: tuple %q count mismatch: got %d, want %d (out=%+v)", k, got, want, outDel)
		}
	}
}

func TestParseQueryJoinSimple_DeleteRightMultiplicity(t *testing.T) {
	q := "SELECT a.id, a.k, b.v FROM a JOIN b ON a.id = b.id"
	node, err := ParseQueryToDBSP(q)
	if err != nil {
		t.Fatalf("ParseQueryToDBSP with JOIN failed: %v", err)
	}
	if node == nil || node.Op == nil {
		t.Fatalf("expected node with op, got nil")
	}

	// Insert two identical left rows (multiplicity 2) and one right row.
	aIns := types.Batch{
		{Tuple: types.Tuple{"a.id": 1, "a.k": "A"}, Count: 1},
		{Tuple: types.Tuple{"a.id": 1, "a.k": "A"}, Count: 1},
	}
	bIns := types.Batch{{Tuple: types.Tuple{"b.id": 1, "b.v": 10}, Count: 1}}

	_, err = op.ExecuteTick(node, map[string]types.Batch{"a": aIns})
	if err != nil {
		t.Fatalf("ExecuteTick(a insert) failed: %v", err)
	}
	outIns, err := op.ExecuteTick(node, map[string]types.Batch{"b": bIns})
	if err != nil {
		t.Fatalf("ExecuteTick(b insert) failed: %v", err)
	}
	gotIns := batchToCountMap(outIns)
	wantKey := tupleKey(types.Tuple{"a.id": 1, "a.k": "A", "b.v": 10})
	if got := gotIns[wantKey]; got != 2 {
		t.Fatalf("insert: expected joined multiplicity 2, got %d (out=%+v)", got, outIns)
	}

	// Delete right row; should retract both joined rows (count -2).
	bDel := types.Batch{{Tuple: types.Tuple{"b.id": 1, "b.v": 10}, Count: -1}}
	outDel, err := op.ExecuteTick(node, map[string]types.Batch{"b": bDel})
	if err != nil {
		t.Fatalf("ExecuteTick(b delete) failed: %v", err)
	}
	gotDel := batchToCountMap(outDel)
	if got := gotDel[wantKey]; got != -2 {
		t.Fatalf("delete: expected joined retraction -2, got %d (out=%+v)", got, outDel)
	}
}

func TestParseQueryJoinWithWhere(t *testing.T) {
	q := "SELECT a.id, a.k, b.v FROM a JOIN b ON a.id = b.id WHERE b.v > 10"
	node, err := ParseQueryToDBSP(q)
	if err != nil {
		t.Fatalf("ParseQueryToDBSP with JOIN+WHERE failed: %v", err)
	}
	if node == nil || node.Op == nil {
		t.Fatalf("expected node with op, got nil")
	}

	aBatch := types.Batch{
		{Tuple: types.Tuple{"a.id": 1, "a.k": "A"}, Count: 1},
		{Tuple: types.Tuple{"a.id": 2, "a.k": "B"}, Count: 1},
	}
	bBatch := types.Batch{
		{Tuple: types.Tuple{"b.id": 1, "b.v": 5}, Count: 1},
		{Tuple: types.Tuple{"b.id": 2, "b.v": 20}, Count: 1},
	}

	_, err = op.ExecuteTick(node, map[string]types.Batch{"a": aBatch})
	if err != nil {
		t.Fatalf("ExecuteTick(a) failed: %v", err)
	}
	out, err := op.ExecuteTick(node, map[string]types.Batch{"b": bBatch})
	if err != nil {
		t.Fatalf("ExecuteTick(b) failed: %v", err)
	}

	// Only id=2, v=20 should remain after filter
	if len(out) != 1 {
		t.Errorf("expected 1 joined+filtered row, got %d", len(out))
	}
}

func TestParseQueryJoinGroupBy(t *testing.T) {

	q := "SELECT a.k, SUM(b.v) FROM a JOIN b ON a.id = b.id GROUP BY a.k"
	node, err := ParseQueryToDBSP(q)
	if err != nil {
		t.Fatalf("ParseQueryToDBSP with JOIN+GROUP BY failed: %v", err)
	}
	if node == nil || node.Op == nil {
		t.Fatalf("expected node with op, got nil")
	}

	// a: (id=1,k=A), (id=2,k=A), (id=3,k=B)
	// b: (id=1,v=10), (id=2,v=20), (id=3,v=5)
	aBatch := types.Batch{
		{Tuple: types.Tuple{"a.id": 1, "a.k": "A"}, Count: 1},
		{Tuple: types.Tuple{"a.id": 2, "a.k": "A"}, Count: 1},
		{Tuple: types.Tuple{"a.id": 3, "a.k": "B"}, Count: 1},
	}
	bBatch := types.Batch{
		{Tuple: types.Tuple{"b.id": 1, "b.v": 10}, Count: 1},
		{Tuple: types.Tuple{"b.id": 2, "b.v": 20}, Count: 1},
		{Tuple: types.Tuple{"b.id": 3, "b.v": 5}, Count: 1},
	}

	_, err = op.ExecuteTick(node, map[string]types.Batch{"a": aBatch})
	if err != nil {
		t.Fatalf("ExecuteTick(a) failed: %v", err)
	}
	out, err := op.ExecuteTick(node, map[string]types.Batch{"b": bBatch})
	if err != nil {
		t.Fatalf("ExecuteTick(b) failed: %v", err)
	}
	if len(out) == 0 {
		t.Fatalf("expected non-empty output")
	}

	// Find GroupAggOp in chain to verify state
	chainedOp, ok := node.Op.(*op.ChainedOp)
	if !ok {
		t.Fatalf("expected ChainedOp, got %T", node.Op)
	}

	var gop *op.GroupAggOp
	for _, o := range chainedOp.Ops {
		if gg, ok := o.(*op.GroupAggOp); ok {
			gop = gg
			break
		}
	}
	if gop == nil {
		t.Fatalf("expected GroupAggOp in chain")
	}

	state := gop.State()

	// A: 10 + 20 = 30
	if state["A"] != 30.0 {
		t.Errorf("expected A=30, got %v", state["A"])
	}
	// B: 5
	if state["B"] != 5.0 {
		t.Errorf("expected B=5, got %v", state["B"])
	}

	// Delete a joined row contribution (b.id=2, v=20) and verify retraction.
	bDel := types.Batch{{Tuple: types.Tuple{"b.id": 2, "b.v": 20}, Count: -1}}
	outDel, err := op.ExecuteTick(node, map[string]types.Batch{"b": bDel})
	if err != nil {
		t.Fatalf("ExecuteTick(b delete) failed: %v", err)
	}
	if len(outDel) != 1 {
		t.Fatalf("expected 1 aggregate delta on delete, got %d (%+v)", len(outDel), outDel)
	}
	if outDel[0].Count != 1 {
		t.Fatalf("expected output delta Count=1, got %d (%+v)", outDel[0].Count, outDel[0])
	}
	if outDel[0].Tuple["a.k"] != "A" {
		t.Fatalf("expected key a.k='A', got %v (%+v)", outDel[0].Tuple["a.k"], outDel[0])
	}
	delta, ok := outDel[0].Tuple["agg_delta"].(float64)
	if !ok {
		t.Fatalf("expected agg_delta float64, got %T (%+v)", outDel[0].Tuple["agg_delta"], outDel[0])
	}
	if delta != -20.0 {
		t.Fatalf("expected agg_delta=-20, got %v (%+v)", delta, outDel[0])
	}

	state2 := gop.State()
	if state2["A"] != 10.0 {
		t.Errorf("expected A=10 after delete, got %v", state2["A"])
	}
}

func TestParseQueryJoinGroupBy_MultiKey(t *testing.T) {
	q := "SELECT a.k1, a.k2, SUM(b.v) FROM a JOIN b ON a.id = b.id GROUP BY a.k1, a.k2"
	node, err := ParseQueryToDBSP(q)
	if err != nil {
		t.Fatalf("ParseQueryToDBSP with JOIN+GROUP BY failed: %v", err)
	}
	if node == nil || node.Op == nil {
		t.Fatalf("expected node with op, got nil")
	}

	// a: (id=1,k1=A,k2=X), (id=2,k1=A,k2=X), (id=3,k1=B,k2=Y)
	// b: (id=1,v=10), (id=2,v=20), (id=3,v=5)
	aBatch := types.Batch{
		{Tuple: types.Tuple{"a.id": 1, "a.k1": "A", "a.k2": "X"}, Count: 1},
		{Tuple: types.Tuple{"a.id": 2, "a.k1": "A", "a.k2": "X"}, Count: 1},
		{Tuple: types.Tuple{"a.id": 3, "a.k1": "B", "a.k2": "Y"}, Count: 1},
	}
	bBatch := types.Batch{
		{Tuple: types.Tuple{"b.id": 1, "b.v": 10}, Count: 1},
		{Tuple: types.Tuple{"b.id": 2, "b.v": 20}, Count: 1},
		{Tuple: types.Tuple{"b.id": 3, "b.v": 5}, Count: 1},
	}

	_, err = op.ExecuteTick(node, map[string]types.Batch{"a": aBatch})
	if err != nil {
		t.Fatalf("ExecuteTick(a) failed: %v", err)
	}
	out, err := op.ExecuteTick(node, map[string]types.Batch{"b": bBatch})
	if err != nil {
		t.Fatalf("ExecuteTick(b) failed: %v", err)
	}
	if len(out) == 0 {
		t.Fatalf("expected non-empty output")
	}

	got := make(map[string]float64)
	for _, td := range out {
		if td.Count != 1 {
			t.Fatalf("expected output delta Count=1, got %d (%+v)", td.Count, td)
		}
		k1 := td.Tuple["a.k1"]
		k2 := td.Tuple["a.k2"]
		if k1 == nil || k2 == nil {
			t.Fatalf("expected output to include a.k1 and a.k2, got tuple=%+v", td.Tuple)
		}
		d, ok := td.Tuple["agg_delta"].(float64)
		if !ok {
			t.Fatalf("expected agg_delta float64, got %T (%+v)", td.Tuple["agg_delta"], td)
		}
		got[fmt.Sprintf("%v|%v", k1, k2)] += d
	}
	if got["A|X"] != 30.0 {
		t.Fatalf("expected net agg_delta=30 for group (A,X), got %v (out=%+v)", got["A|X"], out)
	}
	if got["B|Y"] != 5.0 {
		t.Fatalf("expected net agg_delta=5 for group (B,Y), got %v (out=%+v)", got["B|Y"], out)
	}

	// Delete contribution b.id=2 (v=20) and verify retraction for group (A,X).
	bDel := types.Batch{{Tuple: types.Tuple{"b.id": 2, "b.v": 20}, Count: -1}}
	outDel, err := op.ExecuteTick(node, map[string]types.Batch{"b": bDel})
	if err != nil {
		t.Fatalf("ExecuteTick(b delete) failed: %v", err)
	}
	if len(outDel) != 1 {
		t.Fatalf("expected 1 aggregate delta on delete, got %d (%+v)", len(outDel), outDel)
	}
	if outDel[0].Count != 1 {
		t.Fatalf("expected output delta Count=1, got %d (%+v)", outDel[0].Count, outDel[0])
	}
	if outDel[0].Tuple["a.k1"] != "A" || outDel[0].Tuple["a.k2"] != "X" {
		t.Fatalf("expected group key (A,X), got k1=%v k2=%v (%+v)", outDel[0].Tuple["a.k1"], outDel[0].Tuple["a.k2"], outDel[0])
	}
	delta, ok := outDel[0].Tuple["agg_delta"].(float64)
	if !ok {
		t.Fatalf("expected agg_delta float64, got %T (%+v)", outDel[0].Tuple["agg_delta"], outDel[0])
	}
	if delta != -20.0 {
		t.Fatalf("expected agg_delta=-20, got %v (%+v)", delta, outDel[0])
	}
}

// ============================================================================
// WHERE Clause Tests
// ============================================================================

func TestParseQueryWithWhere(t *testing.T) {
	q := "SELECT k, SUM(v) FROM t WHERE status = 'active' GROUP BY k"
	node, err := ParseQueryToDBSP(q)
	if err != nil {
		t.Fatalf("ParseQueryToDBSP with WHERE failed: %v", err)
	}
	if node == nil || node.Op == nil {
		t.Fatalf("expected node with op, got nil")
	}

	// Feed batch with mixed status
	batch := types.Batch{
		{Tuple: types.Tuple{"k": "A", "v": 10, "status": "active"}, Count: 1},
		{Tuple: types.Tuple{"k": "A", "v": 20, "status": "inactive"}, Count: 1},
		{Tuple: types.Tuple{"k": "B", "v": 5, "status": "active"}, Count: 1},
	}

	// Test filter separately first
	chainedOp, ok := node.Op.(*op.ChainedOp)
	if !ok {
		t.Fatalf("expected ChainedOp, got %T", node.Op)
	}
	if len(chainedOp.Ops) < 2 {
		t.Fatalf("expected at least 2 ops in chain, got %d", len(chainedOp.Ops))
	}

	out, err := op.Execute(node, batch)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	// Extract GroupAggOp from chain (it's the second operator)
	gop, ok := chainedOp.Ops[1].(*op.GroupAggOp)
	if !ok {
		t.Fatalf("expected second op to be GroupAggOp, got %T", chainedOp.Ops[1])
	}

	state := gop.State()

	// A should have only the active row (10)
	if state["A"] != 10.0 {
		t.Errorf("expected A=10 (only active), got %v", state["A"])
	}
	// B should have 5
	if state["B"] != 5.0 {
		t.Errorf("expected B=5, got %v", state["B"])
	}

	if len(out) == 0 {
		t.Fatal("expected non-empty output")
	}
}

func TestParseQueryWithWhereNoGroupBy(t *testing.T) {
	q := "SELECT * FROM t WHERE amount > 100"
	lp, err := ParseQueryToLogicalPlan(q)
	if err != nil {
		t.Fatalf("ParseQueryToLogicalPlan failed: %v", err)
	}

	// Should return LogicalFilter
	filter, ok := lp.(*ir.LogicalFilter)
	if !ok {
		t.Fatalf("expected LogicalFilter, got %T", lp)
	}
	if filter.PredicateSQL == "" {
		t.Error("expected non-empty predicate SQL")
	}
}

func TestParseQueryWhereGreaterThan(t *testing.T) {
	q := "SELECT k, COUNT(id) FROM t WHERE amount > 50 GROUP BY k"
	node, err := ParseQueryToDBSP(q)
	if err != nil {
		t.Fatalf("ParseQueryToDBSP with WHERE > failed: %v", err)
	}

	batch := types.Batch{
		{Tuple: types.Tuple{"k": "X", "id": 1, "amount": 100}, Count: 1},
		{Tuple: types.Tuple{"k": "X", "id": 2, "amount": 30}, Count: 1},
		{Tuple: types.Tuple{"k": "Y", "id": 3, "amount": 60}, Count: 1},
	}
	out, err := op.Execute(node, batch)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	chainedOp, ok := node.Op.(*op.ChainedOp)
	if !ok {
		t.Fatalf("expected ChainedOp, got %T", node.Op)
	}

	gop, ok := chainedOp.Ops[1].(*op.GroupAggOp)
	if !ok {
		t.Fatalf("expected GroupAggOp in chain, got %T", chainedOp.Ops[1])
	}
	state := gop.State()

	// X: only 1 row with amount > 50
	if state["X"] != int64(1) {
		t.Errorf("expected X count=1, got %v", state["X"])
	}
	// Y: 1 row with amount > 50
	if state["Y"] != int64(1) {
		t.Errorf("expected Y count=1, got %v", state["Y"])
	}

	if len(out) == 0 {
		t.Fatal("expected non-empty output")
	}
}

func TestParseQueryWhereNotEqual(t *testing.T) {
	q := "SELECT id, name FROM users WHERE status != 'deleted'"
	node, err := ParseQueryToDBSP(q)
	if err != nil {
		t.Fatalf("ParseQueryToDBSP failed: %v", err)
	}

	batch := types.Batch{
		{Tuple: types.Tuple{"id": 1, "name": "Alice", "status": "active"}, Count: 1},
		{Tuple: types.Tuple{"id": 2, "name": "Bob", "status": "deleted"}, Count: 1},
		{Tuple: types.Tuple{"id": 3, "name": "Charlie", "status": "pending"}, Count: 1},
	}

	out, err := op.Execute(node, batch)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	if len(out) != 2 {
		t.Errorf("expected 2 results, got %d", len(out))
	}
}

func TestParseQueryWhereAND(t *testing.T) {
	q := "SELECT * FROM orders WHERE amount > 100 AND status = 'paid'"
	node, err := ParseQueryToDBSP(q)
	if err != nil {
		t.Fatalf("ParseQueryToDBSP failed: %v", err)
	}

	batch := types.Batch{
		{Tuple: types.Tuple{"id": 1, "amount": 150, "status": "paid"}, Count: 1},
		{Tuple: types.Tuple{"id": 2, "amount": 50, "status": "paid"}, Count: 1},
		{Tuple: types.Tuple{"id": 3, "amount": 150, "status": "pending"}, Count: 1},
		{Tuple: types.Tuple{"id": 4, "amount": 200, "status": "paid"}, Count: 1},
	}

	out, err := op.Execute(node, batch)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	// Should have 2 results (id 1 and 4)
	if len(out) != 2 {
		t.Errorf("expected 2 results, got %d", len(out))
	}
}

func TestParseQueryWhereOR(t *testing.T) {
	q := "SELECT * FROM users WHERE age < 18 OR age > 65"
	node, err := ParseQueryToDBSP(q)
	if err != nil {
		t.Fatalf("ParseQueryToDBSP failed: %v", err)
	}

	batch := types.Batch{
		{Tuple: types.Tuple{"id": 1, "age": 16}, Count: 1},
		{Tuple: types.Tuple{"id": 2, "age": 30}, Count: 1},
		{Tuple: types.Tuple{"id": 3, "age": 70}, Count: 1},
		{Tuple: types.Tuple{"id": 4, "age": 25}, Count: 1},
	}

	out, err := op.Execute(node, batch)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	// Should have 2 results (id 1 and 3)
	if len(out) != 2 {
		t.Errorf("expected 2 results, got %d", len(out))
	}
}

func TestParseQueryWithParentheses(t *testing.T) {
	q := "SELECT * FROM customers WHERE (age > 50 OR status = 'vip') AND amount > 100"
	node, err := ParseQueryToDBSP(q)
	if err != nil {
		t.Fatalf("ParseQueryToDBSP failed: %v", err)
	}

	batch := types.Batch{
		{Tuple: types.Tuple{"id": 1, "age": 60, "status": "normal", "amount": 50}, Count: 1},
		{Tuple: types.Tuple{"id": 2, "age": 55, "status": "normal", "amount": 150}, Count: 1},
		{Tuple: types.Tuple{"id": 3, "age": 30, "status": "vip", "amount": 200}, Count: 1},
		{Tuple: types.Tuple{"id": 4, "age": 30, "status": "normal", "amount": 150}, Count: 1},
	}

	out, err := op.Execute(node, batch)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	// Should have 2 results (id 2 and 3)
	if len(out) != 2 {
		t.Errorf("expected 2 results, got %d: %+v", len(out), out)
	}
}

func TestParseQueryWithNestedParentheses(t *testing.T) {
	q := "SELECT * FROM users WHERE ((age >= 18 AND age <= 30) OR (age >= 60 AND age <= 70)) AND status = 'active'"
	node, err := ParseQueryToDBSP(q)
	if err != nil {
		t.Fatalf("ParseQueryToDBSP failed: %v", err)
	}

	batch := types.Batch{
		{Tuple: types.Tuple{"id": 1, "age": 25, "status": "active"}, Count: 1},
		{Tuple: types.Tuple{"id": 2, "age": 65, "status": "active"}, Count: 1},
		{Tuple: types.Tuple{"id": 3, "age": 45, "status": "active"}, Count: 1},
		{Tuple: types.Tuple{"id": 4, "age": 25, "status": "inactive"}, Count: 1},
	}

	out, err := op.Execute(node, batch)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	// Should have 2 results (id 1 and 2)
	if len(out) != 2 {
		t.Errorf("expected 2 results, got %d: %+v", len(out), out)
	}
}

// ============================================================================
// Projection Tests
// ============================================================================

func TestParseQueryWithProjection(t *testing.T) {
	q := "SELECT name, age FROM users"
	node, err := ParseQueryToDBSP(q)
	if err != nil {
		t.Fatalf("ParseQueryToDBSP with projection failed: %v", err)
	}

	// Should return a MapOp for projection
	mapOp, ok := node.Op.(*op.MapOp)
	if !ok {
		t.Fatalf("expected MapOp for projection, got %T", node.Op)
	}

	// Test with batch
	batch := types.Batch{
		{Tuple: types.Tuple{"name": "Alice", "age": 30, "city": "NYC"}, Count: 1},
		{Tuple: types.Tuple{"name": "Bob", "age": 25, "city": "LA"}, Count: 1},
	}

	out, err := mapOp.Apply(batch)
	if err != nil {
		t.Fatalf("Apply failed: %v", err)
	}

	if len(out) != 2 {
		t.Fatalf("expected 2 output tuples, got %d", len(out))
	}

	// Verify projection - should only have name and age
	if len(out[0].Tuple) != 2 {
		t.Errorf("expected 2 fields, got %d: %+v", len(out[0].Tuple), out[0].Tuple)
	}
	if out[0].Tuple["name"] != "Alice" {
		t.Errorf("expected name=Alice, got %v", out[0].Tuple["name"])
	}
	if _, exists := out[0].Tuple["city"]; exists {
		t.Error("city should not exist in projection")
	}
}

func TestParseQueryProjectionWithWhere(t *testing.T) {
	q := "SELECT id, status FROM orders WHERE amount > 100"
	lp, err := ParseQueryToLogicalPlan(q)
	if err != nil {
		t.Fatalf("ParseQueryToLogicalPlan failed: %v", err)
	}

	// Should return LogicalProject with LogicalFilter as input
	proj, ok := lp.(*ir.LogicalProject)
	if !ok {
		t.Fatalf("expected LogicalProject, got %T", lp)
	}

	if len(proj.Columns) != 2 || proj.Columns[0] != "id" || proj.Columns[1] != "status" {
		t.Errorf("expected columns [id, status], got %v", proj.Columns)
	}

	filter, ok := proj.Input.(*ir.LogicalFilter)
	if !ok {
		t.Fatalf("expected LogicalFilter as input, got %T", proj.Input)
	}

	if filter.PredicateSQL == "" {
		t.Error("expected non-empty predicate")
	}
}

func TestParseQuerySelectStar(t *testing.T) {
	q := "SELECT * FROM users WHERE age > 20"
	lp, err := ParseQueryToLogicalPlan(q)
	if err != nil {
		t.Fatalf("ParseQueryToLogicalPlan failed: %v", err)
	}

	// SELECT * should not create LogicalProject
	filter, ok := lp.(*ir.LogicalFilter)
	if !ok {
		t.Fatalf("expected LogicalFilter (no projection for SELECT *), got %T", lp)
	}

	if filter.PredicateSQL == "" {
		t.Error("expected non-empty predicate")
	}
}

// ============================================================================
// Window Function Tests
// ============================================================================

func TestParseQueryWithWindowAggregate(t *testing.T) {
	// DuckDB standard window aggregate syntax
	// Note: Frame specification is not yet supported by the parser
	q := "SELECT SUM(amount) OVER (PARTITION BY region ORDER BY ts) AS rolling_sum FROM t"
	lp, err := ParseQueryToLogicalPlan(q)
	if err != nil {
		t.Fatalf("ParseQueryToLogicalPlan with window aggregate failed: %v", err)
	}

	wa, ok := lp.(*ir.LogicalWindowAgg)
	if !ok {
		t.Fatalf("expected LogicalWindowAgg, got %T", lp)
	}
	if wa.AggName != "SUM" {
		t.Errorf("expected AggName=SUM, got %s", wa.AggName)
	}
	if wa.AggCol != "amount" {
		t.Errorf("expected AggCol=amount, got %s", wa.AggCol)
	}
	if len(wa.PartitionBy) != 1 || wa.PartitionBy[0] != "region" {
		t.Errorf("expected PartitionBy=[region], got %v", wa.PartitionBy)
	}
	if wa.OrderBy != "ts" {
		t.Errorf("expected OrderBy=ts, got %s", wa.OrderBy)
	}

	// Frame specification parsing will be added when parser supports it
	t.Logf("Window aggregate parsed successfully: %+v", wa)
}

func TestParseQueryWithWindowCOUNT(t *testing.T) {
	q := "SELECT COUNT(id) OVER (PARTITION BY dept ORDER BY ts) AS running_count FROM employees"
	lp, err := ParseQueryToLogicalPlan(q)
	if err != nil {
		t.Fatalf("ParseQueryToLogicalPlan with window COUNT failed: %v", err)
	}

	wa, ok := lp.(*ir.LogicalWindowAgg)
	if !ok {
		t.Fatalf("expected LogicalWindowAgg, got %T", lp)
	}
	if wa.AggName != "COUNT" {
		t.Errorf("expected AggName=COUNT, got %s", wa.AggName)
	}
	if wa.AggCol != "id" {
		t.Errorf("expected AggCol=id, got %s", wa.AggCol)
	}
	if len(wa.PartitionBy) != 1 || wa.PartitionBy[0] != "dept" {
		t.Errorf("expected PartitionBy=[dept], got %v", wa.PartitionBy)
	}
	if wa.OrderBy != "ts" {
		t.Errorf("expected OrderBy=ts, got %s", wa.OrderBy)
	}

	t.Logf("Window COUNT parsed successfully: %+v", wa)
}

func TestParseQueryWithWindowAVG(t *testing.T) {
	q := "SELECT AVG(salary) OVER (PARTITION BY dept ORDER BY hire_date) AS avg_salary FROM employees"
	lp, err := ParseQueryToLogicalPlan(q)
	if err != nil {
		t.Fatalf("ParseQueryToLogicalPlan with window AVG failed: %v", err)
	}

	wa, ok := lp.(*ir.LogicalWindowAgg)
	if !ok {
		t.Fatalf("expected LogicalWindowAgg, got %T", lp)
	}
	if wa.AggName != "AVG" {
		t.Errorf("expected AggName=AVG, got %s", wa.AggName)
	}
	if wa.AggCol != "salary" {
		t.Errorf("expected AggCol=salary, got %s", wa.AggCol)
	}
	if len(wa.PartitionBy) != 1 || wa.PartitionBy[0] != "dept" {
		t.Errorf("expected PartitionBy=[dept], got %v", wa.PartitionBy)
	}
	if wa.OrderBy != "hire_date" {
		t.Errorf("expected OrderBy=hire_date, got %s", wa.OrderBy)
	}

	t.Logf("Window AVG parsed successfully: %+v", wa)
}

func TestParseQueryWithWindowMINMAX(t *testing.T) {
	t.Run("MIN", func(t *testing.T) {
		q := "SELECT MIN(price) OVER (PARTITION BY category ORDER BY ts) AS min_price FROM products"
		lp, err := ParseQueryToLogicalPlan(q)
		if err != nil {
			t.Fatalf("ParseQueryToLogicalPlan with window MIN failed: %v", err)
		}

		wa, ok := lp.(*ir.LogicalWindowAgg)
		if !ok {
			t.Fatalf("expected LogicalWindowAgg, got %T", lp)
		}
		if wa.AggName != "MIN" {
			t.Errorf("expected AggName=MIN, got %s", wa.AggName)
		}
		if wa.AggCol != "price" {
			t.Errorf("expected AggCol=price, got %s", wa.AggCol)
		}
		if len(wa.PartitionBy) != 1 || wa.PartitionBy[0] != "category" {
			t.Errorf("expected PartitionBy=[category], got %v", wa.PartitionBy)
		}
		if wa.OrderBy != "ts" {
			t.Errorf("expected OrderBy=ts, got %s", wa.OrderBy)
		}

		t.Logf("Window MIN parsed successfully: %+v", wa)
	})

	t.Run("MAX", func(t *testing.T) {
		q := "SELECT MAX(price) OVER (PARTITION BY category ORDER BY ts) AS max_price FROM products"
		lp, err := ParseQueryToLogicalPlan(q)
		if err != nil {
			t.Fatalf("ParseQueryToLogicalPlan with window MAX failed: %v", err)
		}

		wa, ok := lp.(*ir.LogicalWindowAgg)
		if !ok {
			t.Fatalf("expected LogicalWindowAgg, got %T", lp)
		}
		if wa.AggName != "MAX" {
			t.Errorf("expected AggName=MAX, got %s", wa.AggName)
		}
		if wa.AggCol != "price" {
			t.Errorf("expected AggCol=price, got %s", wa.AggCol)
		}
		if len(wa.PartitionBy) != 1 || wa.PartitionBy[0] != "category" {
			t.Errorf("expected PartitionBy=[category], got %v", wa.PartitionBy)
		}
		if wa.OrderBy != "ts" {
			t.Errorf("expected OrderBy=ts, got %s", wa.OrderBy)
		}

		t.Logf("Window MAX parsed successfully: %+v", wa)
	})
}

func TestParseQueryWithWindowNoPartition(t *testing.T) {
	q := "SELECT SUM(amount) OVER (ORDER BY ts) AS cumulative_sum FROM transactions"
	lp, err := ParseQueryToLogicalPlan(q)
	if err != nil {
		t.Fatalf("ParseQueryToLogicalPlan with window no partition failed: %v", err)
	}

	wa, ok := lp.(*ir.LogicalWindowAgg)
	if !ok {
		t.Fatalf("expected LogicalWindowAgg, got %T", lp)
	}
	if wa.AggName != "SUM" {
		t.Errorf("expected AggName=SUM, got %s", wa.AggName)
	}
	if wa.AggCol != "amount" {
		t.Errorf("expected AggCol=amount, got %s", wa.AggCol)
	}
	if len(wa.PartitionBy) != 0 {
		t.Errorf("expected empty PartitionBy, got %v", wa.PartitionBy)
	}
	if wa.OrderBy != "ts" {
		t.Errorf("expected OrderBy=ts, got %s", wa.OrderBy)
	}

	t.Logf("Window without partition parsed successfully: %+v", wa)
}

func TestParseQueryWithWindowMultiplePartitions(t *testing.T) {
	q := "SELECT SUM(amount) OVER (PARTITION BY region, category ORDER BY ts) AS regional_sum FROM sales"
	lp, err := ParseQueryToLogicalPlan(q)
	if err != nil {
		t.Fatalf("ParseQueryToLogicalPlan with multiple partitions failed: %v", err)
	}

	wa, ok := lp.(*ir.LogicalWindowAgg)
	if !ok {
		t.Fatalf("expected LogicalWindowAgg, got %T", lp)
	}
	if wa.AggName != "SUM" {
		t.Errorf("expected AggName=SUM, got %s", wa.AggName)
	}
	if len(wa.PartitionBy) != 2 || wa.PartitionBy[0] != "region" || wa.PartitionBy[1] != "category" {
		t.Errorf("expected PartitionBy=[region, category], got %v", wa.PartitionBy)
	}
	if wa.OrderBy != "ts" {
		t.Errorf("expected OrderBy=ts, got %s", wa.OrderBy)
	}

	t.Logf("Window with multiple partitions parsed successfully: %+v", wa)
}

// ============================================================================
// Window Ranking Functions Tests
// ============================================================================

func TestParseQueryWithWindowROW_NUMBER(t *testing.T) {
	q := "SELECT ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) AS rank FROM employees"
	lp, err := ParseQueryToLogicalPlan(q)
	if err != nil {
		t.Fatalf("ParseQueryToLogicalPlan with ROW_NUMBER failed: %v", err)
	}

	t.Logf("ROW_NUMBER parsed to: %T, %+v", lp, lp)
}

func TestParseQueryWithWindowRANK(t *testing.T) {
	q := "SELECT RANK() OVER (PARTITION BY category ORDER BY score DESC) AS rank FROM products"
	lp, err := ParseQueryToLogicalPlan(q)
	if err != nil {
		t.Fatalf("ParseQueryToLogicalPlan with RANK failed: %v", err)
	}

	t.Logf("RANK parsed to: %T, %+v", lp, lp)
}

func TestParseQueryWithWindowDENSE_RANK(t *testing.T) {
	q := "SELECT DENSE_RANK() OVER (ORDER BY score DESC) AS dense_rank FROM scores"
	lp, err := ParseQueryToLogicalPlan(q)
	if err != nil {
		t.Fatalf("ParseQueryToLogicalPlan with DENSE_RANK failed: %v", err)
	}

	t.Logf("DENSE_RANK parsed to: %T, %+v", lp, lp)
}

func TestParseQueryWithWindowNTILE(t *testing.T) {
	q := "SELECT NTILE(4) OVER (ORDER BY salary DESC) AS quartile FROM employees"
	lp, err := ParseQueryToLogicalPlan(q)
	if err != nil {
		t.Fatalf("ParseQueryToLogicalPlan with NTILE failed: %v", err)
	}

	t.Logf("NTILE parsed to: %T, %+v", lp, lp)
}

func TestParseQueryWithWindowPERCENT_RANK(t *testing.T) {
	q := "SELECT PERCENT_RANK() OVER (PARTITION BY dept ORDER BY salary) AS pct_rank FROM employees"
	lp, err := ParseQueryToLogicalPlan(q)
	if err != nil {
		t.Fatalf("ParseQueryToLogicalPlan with PERCENT_RANK failed: %v", err)
	}

	t.Logf("PERCENT_RANK parsed to: %T, %+v", lp, lp)
}

func TestParseQueryWithWindowCUME_DIST(t *testing.T) {
	q := "SELECT CUME_DIST() OVER (ORDER BY score) AS cumulative_dist FROM results"
	lp, err := ParseQueryToLogicalPlan(q)
	if err != nil {
		t.Fatalf("ParseQueryToLogicalPlan with CUME_DIST failed: %v", err)
	}

	t.Logf("CUME_DIST parsed to: %T, %+v", lp, lp)
}

// ============================================================================
// Window Value Functions Tests
// ============================================================================

func TestParseQueryWithWindowLAG(t *testing.T) {
	t.Skip("LAG window function parsing has parser issues")

	q := "SELECT amount, LAG(amount) OVER (ORDER BY date) AS prev_amount FROM sales"
	lp, err := ParseQueryToLogicalPlan(q)
	if err != nil {
		t.Fatalf("ParseQueryToLogicalPlan with LAG failed: %v", err)
	}

	t.Logf("LAG parsed to: %T, %+v", lp, lp)
}

func TestParseQueryWithWindowLAGOffset(t *testing.T) {
	t.Skip("LAG with offset/default not yet supported by parser")

	q := "SELECT amount, LAG(amount, 3, 0) OVER (ORDER BY date) AS prev_3_amount FROM sales"
	lp, err := ParseQueryToLogicalPlan(q)
	if err != nil {
		t.Fatalf("ParseQueryToLogicalPlan with LAG(offset, default) failed: %v", err)
	}

	t.Logf("LAG with offset and default parsed to: %T, %+v", lp, lp)
}

func TestParseQueryWithWindowLEAD(t *testing.T) {
	t.Skip("LEAD window function not yet supported by parser")

	q := "SELECT amount, LEAD(amount) OVER (ORDER BY date) AS next_amount FROM sales"
	lp, err := ParseQueryToLogicalPlan(q)
	if err != nil {
		t.Fatalf("ParseQueryToLogicalPlan with LEAD failed: %v", err)
	}

	t.Logf("LEAD parsed to: %T, %+v", lp, lp)
}

func TestParseQueryWithWindowLEADOffset(t *testing.T) {
	t.Skip("LEAD with offset/default not yet supported by parser")

	q := "SELECT amount, LEAD(amount, 2, 0) OVER (PARTITION BY region ORDER BY date) AS next_2_amount FROM sales"
	lp, err := ParseQueryToLogicalPlan(q)
	if err != nil {
		t.Fatalf("ParseQueryToLogicalPlan with LEAD(offset, default) failed: %v", err)
	}

	t.Logf("LEAD with offset and default parsed to: %T, %+v", lp, lp)
}

func TestParseQueryWithWindowFIRST_VALUE(t *testing.T) {
	t.Skip("FIRST_VALUE window function not yet supported by parser")

	q := "SELECT FIRST_VALUE(price) OVER (PARTITION BY category ORDER BY date) AS first_price FROM products"
	lp, err := ParseQueryToLogicalPlan(q)
	if err != nil {
		t.Fatalf("ParseQueryToLogicalPlan with FIRST_VALUE failed: %v", err)
	}

	t.Logf("FIRST_VALUE parsed to: %T, %+v", lp, lp)
}

func TestParseQueryWithWindowLAST_VALUE(t *testing.T) {
	t.Skip("LAST_VALUE window function not yet supported by parser")

	q := "SELECT LAST_VALUE(price) OVER (PARTITION BY category ORDER BY date) AS last_price FROM products"
	lp, err := ParseQueryToLogicalPlan(q)
	if err != nil {
		t.Fatalf("ParseQueryToLogicalPlan with LAST_VALUE failed: %v", err)
	}

	t.Logf("LAST_VALUE parsed to: %T, %+v", lp, lp)
}

func TestParseQueryWithWindowNTH_VALUE(t *testing.T) {
	t.Skip("NTH_VALUE window function not yet supported by parser")

	q := "SELECT NTH_VALUE(price, 2) OVER (PARTITION BY category ORDER BY date) AS second_price FROM products"
	lp, err := ParseQueryToLogicalPlan(q)
	if err != nil {
		t.Fatalf("ParseQueryToLogicalPlan with NTH_VALUE failed: %v", err)
	}

	t.Logf("NTH_VALUE parsed to: %T, %+v", lp, lp)
}

// ============================================================================
// Window Frame Specification Tests
// ============================================================================

func TestParseQueryWithWindowROWSFrame(t *testing.T) {
	t.Skip("ROWS frame specification not yet supported by parser")

	q := "SELECT SUM(amount) OVER (ORDER BY date ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS moving_sum FROM sales"
	lp, err := ParseQueryToLogicalPlan(q)
	if err != nil {
		t.Fatalf("ParseQueryToLogicalPlan with ROWS frame failed: %v", err)
	}

	t.Logf("ROWS frame parsed to: %T, %+v", lp, lp)
}

func TestParseQueryWithWindowRANGEFrame(t *testing.T) {
	t.Skip("RANGE frame specification not yet supported by parser")

	q := "SELECT AVG(amount) OVER (ORDER BY date RANGE BETWEEN INTERVAL 3 DAYS PRECEDING AND INTERVAL 3 DAYS FOLLOWING) AS moving_avg FROM sales"
	lp, err := ParseQueryToLogicalPlan(q)
	if err != nil {
		t.Fatalf("ParseQueryToLogicalPlan with RANGE frame failed: %v", err)
	}

	t.Logf("RANGE frame parsed to: %T, %+v", lp, lp)
}

func TestParseQueryWithWindowGROUPSFrame(t *testing.T) {
	t.Skip("GROUPS frame specification not yet supported by parser")

	q := "SELECT SUM(amount) OVER (ORDER BY date GROUPS BETWEEN 2 PRECEDING AND 2 FOLLOWING) AS moving_sum FROM sales"
	lp, err := ParseQueryToLogicalPlan(q)
	if err != nil {
		t.Fatalf("ParseQueryToLogicalPlan with GROUPS frame failed: %v", err)
	}

	t.Logf("GROUPS frame parsed to: %T, %+v", lp, lp)
}

func TestParseQueryWithWindowUnboundedFrame(t *testing.T) {
	t.Skip("UNBOUNDED frame specification not yet supported by parser")

	q := "SELECT SUM(amount) OVER (ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_sum FROM sales"
	lp, err := ParseQueryToLogicalPlan(q)
	if err != nil {
		t.Fatalf("ParseQueryToLogicalPlan with UNBOUNDED frame failed: %v", err)
	}

	t.Logf("UNBOUNDED frame parsed to: %T, %+v", lp, lp)
}

func TestParseQueryWithWindowEXCLUDEClause(t *testing.T) {
	t.Skip("EXCLUDE clause not yet supported by parser")

	q := "SELECT AVG(time) OVER (PARTITION BY event ORDER BY date RANGE BETWEEN INTERVAL 10 DAYS PRECEDING AND INTERVAL 10 DAYS FOLLOWING EXCLUDE CURRENT ROW) AS avg_time FROM results"
	lp, err := ParseQueryToLogicalPlan(q)
	if err != nil {
		t.Fatalf("ParseQueryToLogicalPlan with EXCLUDE CURRENT ROW failed: %v", err)
	}

	t.Logf("EXCLUDE clause parsed to: %T, %+v", lp, lp)
}

// ============================================================================
// Window DISTINCT and ORDER BY Arguments Tests
// ============================================================================

func TestParseQueryWithWindowDISTINCT(t *testing.T) {
	q := "SELECT COUNT(DISTINCT name) OVER (ORDER BY time) AS distinct_count FROM sales"
	lp, err := ParseQueryToLogicalPlan(q)
	if err != nil {
		t.Fatalf("ParseQueryToLogicalPlan with DISTINCT in window failed: %v", err)
	}

	t.Logf("DISTINCT in window parsed to: %T, %+v", lp, lp)
}

func TestParseQueryWithWindowOrderByArgument(t *testing.T) {
	q := "SELECT LIST(name ORDER BY time DESC) OVER (PARTITION BY region ORDER BY time) AS ordered_list FROM sales"
	lp, err := ParseQueryToLogicalPlan(q)
	if err != nil {
		t.Fatalf("ParseQueryToLogicalPlan with ORDER BY in aggregate failed: %v", err)
	}

	t.Logf("ORDER BY in aggregate argument parsed to: %T, %+v", lp, lp)
}

func TestParseQueryWithWindowIGNORE_NULLS(t *testing.T) {
	t.Skip("IGNORE NULLS not yet supported by parser")

	q := "SELECT FIRST_VALUE(price IGNORE NULLS) OVER (PARTITION BY category ORDER BY date) AS first_non_null_price FROM products"
	lp, err := ParseQueryToLogicalPlan(q)
	if err != nil {
		t.Fatalf("ParseQueryToLogicalPlan with IGNORE NULLS failed: %v", err)
	}

	t.Logf("IGNORE NULLS parsed to: %T, %+v", lp, lp)
}

// ============================================================================
// Window QUALIFY Tests
// ============================================================================

func TestParseQueryWithQUALIFY(t *testing.T) {
	t.Skip("QUALIFY clause not yet supported by parser")

	q := "SELECT id, name, score, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY score DESC) AS rank FROM employees QUALIFY rank <= 3"
	lp, err := ParseQueryToLogicalPlan(q)
	if err != nil {
		t.Fatalf("ParseQueryToLogicalPlan with QUALIFY failed: %v", err)
	}

	t.Logf("QUALIFY parsed to: %T, %+v", lp, lp)
}

func TestParseQueryWithQUALIFYComplex(t *testing.T) {
	t.Skip("QUALIFY clause not yet supported by parser")

	q := "SELECT product, sales, RANK() OVER (ORDER BY sales DESC) AS sales_rank FROM products QUALIFY sales_rank <= 10 OR sales > 1000000"
	lp, err := ParseQueryToLogicalPlan(q)
	if err != nil {
		t.Fatalf("ParseQueryToLogicalPlan with complex QUALIFY failed: %v", err)
	}

	t.Logf("Complex QUALIFY parsed to: %T, %+v", lp, lp)
}

// ============================================================================
// Window Named Window (WINDOW clause) Tests
// ============================================================================

func TestParseQueryWithNamedWindow(t *testing.T) {
	t.Skip("Named WINDOW clause not yet supported by parser")

	q := `SELECT 
		MIN(amount) OVER w AS min_amount,
		AVG(amount) OVER w AS avg_amount,
		MAX(amount) OVER w AS max_amount
	FROM sales
	WINDOW w AS (PARTITION BY region ORDER BY date)`
	lp, err := ParseQueryToLogicalPlan(q)
	if err != nil {
		t.Fatalf("ParseQueryToLogicalPlan with named WINDOW failed: %v", err)
	}

	t.Logf("Named WINDOW parsed to: %T, %+v", lp, lp)
}

func TestParseQueryWithMultipleNamedWindows(t *testing.T) {
	t.Skip("Multiple named WINDOWs not yet supported by parser")

	q := `SELECT 
		AVG(amount) OVER w7 AS avg_7day,
		AVG(amount) OVER w3 AS avg_3day
	FROM sales
	WINDOW 
		w7 AS (PARTITION BY region ORDER BY date RANGE BETWEEN INTERVAL 3 DAYS PRECEDING AND INTERVAL 3 DAYS FOLLOWING),
		w3 AS (PARTITION BY region ORDER BY date RANGE BETWEEN INTERVAL 1 DAYS PRECEDING AND INTERVAL 1 DAYS FOLLOWING)`
	lp, err := ParseQueryToLogicalPlan(q)
	if err != nil {
		t.Fatalf("ParseQueryToLogicalPlan with multiple named WINDOWs failed: %v", err)
	}

	t.Logf("Multiple named WINDOWs parsed to: %T, %+v", lp, lp)
}

// ============================================================================
// ORDER BY and LIMIT Tests
// ============================================================================

func TestParseQueryWithOrderBy(t *testing.T) {
	q := "SELECT id, name, age FROM users ORDER BY age"
	lp, err := ParseQueryToLogicalPlan(q)
	if err != nil {
		t.Fatalf("ParseQueryToLogicalPlan with ORDER BY failed: %v", err)
	}

	// Check if LogicalSort or similar is created
	// The actual type depends on implementation
	t.Logf("ORDER BY parsed to: %T, %+v", lp, lp)
}

func TestParseQueryWithOrderByDesc(t *testing.T) {
	q := "SELECT id, name, salary FROM employees ORDER BY salary DESC"
	lp, err := ParseQueryToLogicalPlan(q)
	if err != nil {
		t.Fatalf("ParseQueryToLogicalPlan with ORDER BY DESC failed: %v", err)
	}

	t.Logf("ORDER BY DESC parsed to: %T, %+v", lp, lp)
}

func TestParseQueryWithOrderByMultipleColumns(t *testing.T) {
	q := "SELECT id, dept, salary FROM employees ORDER BY dept ASC, salary DESC"
	lp, err := ParseQueryToLogicalPlan(q)
	if err != nil {
		t.Fatalf("ParseQueryToLogicalPlan with multiple ORDER BY failed: %v", err)
	}

	t.Logf("ORDER BY multiple columns parsed to: %T, %+v", lp, lp)
}

func TestParseQueryWithLimit(t *testing.T) {
	q := "SELECT id, name FROM users LIMIT 10"
	lp, err := ParseQueryToLogicalPlan(q)
	if err != nil {
		t.Fatalf("ParseQueryToLogicalPlan with LIMIT failed: %v", err)
	}

	t.Logf("LIMIT parsed to: %T, %+v", lp, lp)
}

func TestParseQueryWithOrderByAndLimit(t *testing.T) {
	q := "SELECT id, name, score FROM students ORDER BY score DESC LIMIT 5"
	lp, err := ParseQueryToLogicalPlan(q)
	if err != nil {
		t.Fatalf("ParseQueryToLogicalPlan with ORDER BY+LIMIT failed: %v", err)
	}

	t.Logf("ORDER BY+LIMIT parsed to: %T, %+v", lp, lp)
}

func TestParseQueryWithWhereOrderByLimit(t *testing.T) {
	q := "SELECT id, name, age FROM users WHERE age >= 18 ORDER BY age DESC LIMIT 20"
	lp, err := ParseQueryToLogicalPlan(q)
	if err != nil {
		t.Fatalf("ParseQueryToLogicalPlan with WHERE+ORDER BY+LIMIT failed: %v", err)
	}

	t.Logf("WHERE+ORDER BY+LIMIT parsed to: %T, %+v", lp, lp)
}

func TestParseQueryWithLimitOffset(t *testing.T) {
	q := "SELECT id, title FROM articles ORDER BY created_at DESC LIMIT 10 OFFSET 20"
	lp, err := ParseQueryToLogicalPlan(q)
	if err != nil {
		t.Fatalf("ParseQueryToLogicalPlan with LIMIT OFFSET failed: %v", err)
	}

	t.Logf("LIMIT OFFSET parsed to: %T, %+v", lp, lp)
}

// ============================================================================
// NULL Handling Tests
// ============================================================================

func TestParseQueryWhereIsNull(t *testing.T) {
	q := "SELECT id, name FROM users WHERE email IS NULL"
	lp, err := ParseQueryToLogicalPlan(q)
	if err != nil {
		t.Fatalf("ParseQueryToLogicalPlan with IS NULL failed: %v", err)
	}

	// Can be either LogicalFilter or LogicalProject wrapping LogicalFilter
	var filter *ir.LogicalFilter
	if proj, ok := lp.(*ir.LogicalProject); ok {
		filter, _ = proj.Input.(*ir.LogicalFilter)
	} else {
		filter, _ = lp.(*ir.LogicalFilter)
	}

	if filter == nil {
		t.Fatalf("expected LogicalFilter in plan, got %T", lp)
	}
	if filter.PredicateSQL == "" {
		t.Error("expected non-empty predicate SQL")
	}

	t.Logf("IS NULL predicate: %s", filter.PredicateSQL)
}

func TestParseQueryWhereIsNotNull(t *testing.T) {
	q := "SELECT id, name, phone FROM customers WHERE phone IS NOT NULL"
	lp, err := ParseQueryToLogicalPlan(q)
	if err != nil {
		t.Fatalf("ParseQueryToLogicalPlan with IS NOT NULL failed: %v", err)
	}

	// Can be either LogicalFilter or LogicalProject wrapping LogicalFilter
	var filter *ir.LogicalFilter
	if proj, ok := lp.(*ir.LogicalProject); ok {
		filter, _ = proj.Input.(*ir.LogicalFilter)
	} else {
		filter, _ = lp.(*ir.LogicalFilter)
	}

	if filter == nil {
		t.Fatalf("expected LogicalFilter in plan, got %T", lp)
	}
	if filter.PredicateSQL == "" {
		t.Error("expected non-empty predicate SQL")
	}

	t.Logf("IS NOT NULL predicate: %s", filter.PredicateSQL)
}

func TestExecuteWhereIsNull(t *testing.T) {
	q := "SELECT id, name FROM users WHERE status IS NULL"
	node, err := ParseQueryToDBSP(q)
	if err != nil {
		t.Fatalf("ParseQueryToDBSP with IS NULL failed: %v", err)
	}

	batch := types.Batch{
		{Tuple: types.Tuple{"id": 1, "name": "Alice", "status": "active"}, Count: 1},
		{Tuple: types.Tuple{"id": 2, "name": "Bob", "status": nil}, Count: 1},
		{Tuple: types.Tuple{"id": 3, "name": "Charlie", "status": nil}, Count: 1},
	}

	out, err := op.Execute(node, batch)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	// Should have 2 results (id 2 and 3)
	if len(out) != 2 {
		t.Errorf("expected 2 results with NULL status, got %d", len(out))
	}
}

func TestExecuteWhereIsNotNull(t *testing.T) {
	q := "SELECT id, name FROM users WHERE status IS NOT NULL"
	node, err := ParseQueryToDBSP(q)
	if err != nil {
		t.Fatalf("ParseQueryToDBSP with IS NOT NULL failed: %v", err)
	}

	batch := types.Batch{
		{Tuple: types.Tuple{"id": 1, "name": "Alice", "status": "active"}, Count: 1},
		{Tuple: types.Tuple{"id": 2, "name": "Bob", "status": nil}, Count: 1},
		{Tuple: types.Tuple{"id": 3, "name": "Charlie", "status": "pending"}, Count: 1},
	}

	out, err := op.Execute(node, batch)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	// Should have 2 results (id 1 and 3)
	if len(out) != 2 {
		t.Errorf("expected 2 results with non-NULL status, got %d", len(out))
	}
}

func TestAggregateWithNull(t *testing.T) {
	t.Run("SUM ignores NULL", func(t *testing.T) {
		q := "SELECT k, SUM(v) FROM t GROUP BY k"
		node, err := ParseQueryToIncrementalDBSP(q)
		if err != nil {
			t.Fatalf("ParseQueryToIncrementalDBSP failed: %v", err)
		}

		batch := types.Batch{
			{Tuple: types.Tuple{"k": "A", "v": 10}, Count: 1},
			{Tuple: types.Tuple{"k": "A", "v": nil}, Count: 1},
			{Tuple: types.Tuple{"k": "A", "v": 20}, Count: 1},
			{Tuple: types.Tuple{"k": "B", "v": 5}, Count: 1},
		}

		out, err := node.Op.Apply(batch)
		if err != nil {
			t.Fatalf("Apply failed: %v", err)
		}

		gop, ok := node.Op.(*op.GroupAggOp)
		if !ok {
			t.Fatalf("expected GroupAggOp, got %T", node.Op)
		}
		state := gop.State()

		// A: 10 + 20 = 30 (NULL ignored)
		if state["A"] != 30.0 {
			t.Errorf("expected A=30 (NULL ignored), got %v", state["A"])
		}

		t.Logf("SUM with NULL: %+v", out)
	})

	t.Run("COUNT ignores NULL", func(t *testing.T) {
		q := "SELECT k, COUNT(v) FROM t GROUP BY k"
		node, err := ParseQueryToIncrementalDBSP(q)
		if err != nil {
			t.Fatalf("ParseQueryToIncrementalDBSP failed: %v", err)
		}

		batch := types.Batch{
			{Tuple: types.Tuple{"k": "A", "v": 10}, Count: 1},
			{Tuple: types.Tuple{"k": "A", "v": nil}, Count: 1},
			{Tuple: types.Tuple{"k": "A", "v": 20}, Count: 1},
		}

		out, err := node.Op.Apply(batch)
		if err != nil {
			t.Fatalf("Apply failed: %v", err)
		}

		gop, ok := node.Op.(*op.GroupAggOp)
		if !ok {
			t.Fatalf("expected GroupAggOp, got %T", node.Op)
		}
		state := gop.State()

		// A: count=2 (NULL not counted)
		if state["A"] != int64(2) {
			t.Errorf("expected A count=2 (NULL ignored), got %v", state["A"])
		}

		t.Logf("COUNT with NULL: %+v", out)
	})

	t.Run("MIN/MAX ignores NULL", func(t *testing.T) {
		q := "SELECT k, MIN(v) FROM t GROUP BY k"
		node, err := ParseQueryToIncrementalDBSP(q)
		if err != nil {
			t.Fatalf("ParseQueryToIncrementalDBSP failed: %v", err)
		}

		batch := types.Batch{
			{Tuple: types.Tuple{"k": "A", "v": 30}, Count: 1},
			{Tuple: types.Tuple{"k": "A", "v": nil}, Count: 1},
			{Tuple: types.Tuple{"k": "A", "v": 10}, Count: 1},
		}

		out, err := node.Op.Apply(batch)
		if err != nil {
			t.Fatalf("Apply failed: %v", err)
		}

		t.Logf("MIN with NULL: %+v", out)
	})
}

func TestJoinWithNull(t *testing.T) {

	q := "SELECT a.id, a.k, b.v FROM a JOIN b ON a.id = b.id"
	node, err := ParseQueryToDBSP(q)
	if err != nil {
		t.Fatalf("ParseQueryToDBSP with JOIN failed: %v", err)
	}

	// NULL keys should not match
	aBatch := types.Batch{
		{Tuple: types.Tuple{"a.id": 1, "a.k": "A"}, Count: 1},
		{Tuple: types.Tuple{"a.id": nil, "a.k": "B"}, Count: 1},
	}
	bBatch := types.Batch{
		{Tuple: types.Tuple{"b.id": 1, "b.v": 10}, Count: 1},
		{Tuple: types.Tuple{"b.id": nil, "b.v": 20}, Count: 1},
	}

	_, err = op.ExecuteTick(node, map[string]types.Batch{"a": aBatch})
	if err != nil {
		t.Fatalf("ExecuteTick(a) failed: %v", err)
	}
	out, err := op.ExecuteTick(node, map[string]types.Batch{"b": bBatch})
	if err != nil {
		t.Fatalf("ExecuteTick(b) failed: %v", err)
	}

	// Only id=1 should join, NULL keys don't match
	if len(out) != 1 {
		t.Errorf("expected 1 joined row (NULL keys excluded), got %d", len(out))
	}

	t.Logf("JOIN with NULL keys: %+v", out)
}

func TestWhereNullComparison(t *testing.T) {
	t.Run("NULL = value is false", func(t *testing.T) {
		q := "SELECT id, name FROM users WHERE status = 'active'"
		node, err := ParseQueryToDBSP(q)
		if err != nil {
			t.Fatalf("ParseQueryToDBSP failed: %v", err)
		}

		batch := types.Batch{
			{Tuple: types.Tuple{"id": 1, "name": "Alice", "status": "active"}, Count: 1},
			{Tuple: types.Tuple{"id": 2, "name": "Bob", "status": nil}, Count: 1},
			{Tuple: types.Tuple{"id": 3, "name": "Charlie", "status": "inactive"}, Count: 1},
		}

		out, err := op.Execute(node, batch)
		if err != nil {
			t.Fatalf("Execute failed: %v", err)
		}

		// Only id=1 should pass (NULL != 'active')
		if len(out) != 1 {
			t.Errorf("expected 1 result (NULL excluded from equality), got %d", len(out))
		}
	})
}

// ============================================================================
// MIN/MAX Aggregation Tests
// ============================================================================

func TestMinAgg_SQL(t *testing.T) {
	query := "SELECT k, MIN(v) FROM t GROUP BY k"

	node, err := ParseQueryToIncrementalDBSP(query)
	if err != nil {
		t.Fatalf("ParseQueryToIncrementalDBSP failed: %v", err)
	}

	// Insert: A: [30, 10, 20], B: [5]
	batch := types.Batch{
		{Tuple: types.Tuple{"k": "A", "v": 30}, Count: 1},
		{Tuple: types.Tuple{"k": "A", "v": 10}, Count: 1},
		{Tuple: types.Tuple{"k": "A", "v": 20}, Count: 1},
		{Tuple: types.Tuple{"k": "B", "v": 5}, Count: 1},
	}

	out, err := node.Op.Apply(batch)
	if err != nil {
		t.Fatalf("Apply failed: %v", err)
	}

	t.Logf("Output after inserts: %+v", out)

	// Delete min value from A (10) → new min = 20
	del := types.Batch{
		{Tuple: types.Tuple{"k": "A", "v": 10}, Count: -1},
	}

	out2, err := node.Op.Apply(del)
	if err != nil {
		t.Fatalf("Apply(delete) failed: %v", err)
	}

	t.Logf("Output after delete: %+v", out2)

	if len(out2) == 0 {
		t.Error("expected output delta when min changes")
	}
}

func TestMaxAgg_SQL(t *testing.T) {
	query := "SELECT k, MAX(v) FROM t GROUP BY k"

	node, err := ParseQueryToIncrementalDBSP(query)
	if err != nil {
		t.Fatalf("ParseQueryToIncrementalDBSP failed: %v", err)
	}

	// Insert: A: [30, 10, 20], B: [100]
	batch := types.Batch{
		{Tuple: types.Tuple{"k": "A", "v": 30}, Count: 1},
		{Tuple: types.Tuple{"k": "A", "v": 10}, Count: 1},
		{Tuple: types.Tuple{"k": "A", "v": 20}, Count: 1},
		{Tuple: types.Tuple{"k": "B", "v": 100}, Count: 1},
	}

	out, err := node.Op.Apply(batch)
	if err != nil {
		t.Fatalf("Apply failed: %v", err)
	}

	t.Logf("Output after inserts: %+v", out)

	// Delete max value from A (30) → new max = 20
	del := types.Batch{
		{Tuple: types.Tuple{"k": "A", "v": 30}, Count: -1},
	}

	out2, err := node.Op.Apply(del)
	if err != nil {
		t.Fatalf("Apply(delete) failed: %v", err)
	}

	t.Logf("Output after delete: %+v", out2)

	if len(out2) == 0 {
		t.Error("expected output delta when max changes")
	}
}

func TestMinMax_WithFilter(t *testing.T) {
	t.Run("MIN with WHERE", func(t *testing.T) {
		query := "SELECT k, MIN(v) FROM t WHERE v > 5 GROUP BY k"

		node, err := ParseQueryToIncrementalDBSP(query)
		if err != nil {
			t.Fatalf("ParseQueryToIncrementalDBSP failed: %v", err)
		}

		// Insert values, some filtered out
		batch := types.Batch{
			{Tuple: types.Tuple{"k": "A", "v": 3}, Count: 1},
			{Tuple: types.Tuple{"k": "A", "v": 10}, Count: 1},
			{Tuple: types.Tuple{"k": "A", "v": 20}, Count: 1},
		}

		out, err := node.Op.Apply(batch)
		if err != nil {
			t.Fatalf("Apply failed: %v", err)
		}

		t.Logf("Output with filter: %+v", out)
	})

	t.Run("MAX with WHERE", func(t *testing.T) {
		query := "SELECT k, MAX(v) FROM t WHERE v < 100 GROUP BY k"

		node, err := ParseQueryToIncrementalDBSP(query)
		if err != nil {
			t.Fatalf("ParseQueryToIncrementalDBSP failed: %v", err)
		}

		batch := types.Batch{
			{Tuple: types.Tuple{"k": "A", "v": 150}, Count: 1},
			{Tuple: types.Tuple{"k": "A", "v": 50}, Count: 1},
			{Tuple: types.Tuple{"k": "A", "v": 80}, Count: 1},
		}

		out, err := node.Op.Apply(batch)
		if err != nil {
			t.Fatalf("Apply failed: %v", err)
		}

		t.Logf("Output with filter: %+v", out)
	})
}

func TestMinMax_EmptyGroup(t *testing.T) {
	query := "SELECT k, MIN(v) FROM t GROUP BY k"

	node, err := ParseQueryToIncrementalDBSP(query)
	if err != nil {
		t.Fatalf("ParseQueryToIncrementalDBSP failed: %v", err)
	}

	// Insert and immediately delete
	batch := types.Batch{
		{Tuple: types.Tuple{"k": "A", "v": 10}, Count: 1},
		{Tuple: types.Tuple{"k": "A", "v": 10}, Count: -1},
	}

	out, err := node.Op.Apply(batch)
	if err != nil {
		t.Fatalf("Apply failed: %v", err)
	}

	t.Logf("Output for empty group: %+v", out)
}

// ============================================================================
// Sorting Utility Tests
// ============================================================================

func TestSortBatchByOrderColumn_Int(t *testing.T) {
	batch := types.Batch{
		{Tuple: types.Tuple{"id": int64(3)}, Count: 1},
		{Tuple: types.Tuple{"id": int64(1)}, Count: 1},
		{Tuple: types.Tuple{"id": int64(2)}, Count: 1},
	}

	sorted := SortBatchByOrderColumn(batch, "id")

	if len(sorted) != 3 {
		t.Fatalf("expected 3 elements, got %d", len(sorted))
	}

	if sorted[0].Tuple["id"] != int64(1) ||
		sorted[1].Tuple["id"] != int64(2) ||
		sorted[2].Tuple["id"] != int64(3) {
		t.Fatalf("unexpected sort order: %#v", sorted)
	}
}

func TestSortBatchByOrderColumn_String(t *testing.T) {
	batch := types.Batch{
		{Tuple: types.Tuple{"ts": "2025-11-26T10:00:00Z"}, Count: 1},
		{Tuple: types.Tuple{"ts": "2025-11-26T08:00:00Z"}, Count: 1},
		{Tuple: types.Tuple{"ts": "2025-11-26T09:00:00Z"}, Count: 1},
	}

	sorted := SortBatchByOrderColumn(batch, "ts")

	if sorted[0].Tuple["ts"] != "2025-11-26T08:00:00Z" ||
		sorted[1].Tuple["ts"] != "2025-11-26T09:00:00Z" ||
		sorted[2].Tuple["ts"] != "2025-11-26T10:00:00Z" {
		t.Fatalf("unexpected sort order: %#v", sorted)
	}
}

func TestSortBatchByOrderColumn_EmptyOrNoColumn(t *testing.T) {
	batch := types.Batch{
		{Tuple: types.Tuple{"id": int64(2)}, Count: 1},
		{Tuple: types.Tuple{"id": int64(1)}, Count: 1},
	}

	// orderCol 비어 있으면 그대로 반환
	sorted := SortBatchByOrderColumn(batch, "")
	if &sorted[0] == &batch[0] {
		// 포인터 비교가 아니라 내용 비교: 순서가 바뀌지 않았는지만 본다
		if sorted[0].Tuple["id"] != batch[0].Tuple["id"] ||
			sorted[1].Tuple["id"] != batch[1].Tuple["id"] {
			t.Fatalf("batch should not be modified when orderCol is empty")
		}
	}

	// 존재하지 않는 컬럼이면 모두 nil이므로 순서 유지 (stable sort)
	sorted2 := SortBatchByOrderColumn(batch, "unknown")
	if sorted2[0].Tuple["id"] != batch[0].Tuple["id"] ||
		sorted2[1].Tuple["id"] != batch[1].Tuple["id"] {
		t.Fatalf("batch should not change when order column is missing")
	}
}

// ============================================================================
// Time Window Tests
// ============================================================================

func TestParseTimeWindowSQL_Tumble(t *testing.T) {
	spec, err := ParseTimeWindowSQL("TUMBLE(ts, INTERVAL '5' MINUTE)")
	if err != nil {
		t.Fatalf("failed to parse: %v", err)
	}

	if spec.WindowType != "TUMBLING" {
		t.Errorf("expected TUMBLING, got %s", spec.WindowType)
	}
	if spec.TimeCol != "ts" {
		t.Errorf("expected TimeCol=ts, got %s", spec.TimeCol)
	}
	if spec.SizeMillis != 300000 {
		t.Errorf("expected 300000ms, got %d", spec.SizeMillis)
	}
}

func TestParseTimeWindowSQL_Hop(t *testing.T) {
	spec, err := ParseTimeWindowSQL("HOP(ts, INTERVAL '5' MINUTE, INTERVAL '10' MINUTE)")
	if err != nil {
		t.Fatalf("failed to parse: %v", err)
	}

	if spec.WindowType != "SLIDING" {
		t.Errorf("expected SLIDING, got %s", spec.WindowType)
	}
	if spec.SlideMillis != 300000 {
		t.Errorf("expected slide=300000ms, got %d", spec.SlideMillis)
	}
	if spec.SizeMillis != 600000 {
		t.Errorf("expected size=600000ms, got %d", spec.SizeMillis)
	}
}

func TestParseTimeWindowSQL_Session(t *testing.T) {
	spec, err := ParseTimeWindowSQL("SESSION(ts, INTERVAL '5' MINUTE)")
	if err != nil {
		t.Fatalf("failed to parse: %v", err)
	}

	if spec.WindowType != "SESSION" {
		t.Errorf("expected SESSION, got %s", spec.WindowType)
	}
	if spec.GapMillis != 300000 {
		t.Errorf("expected gap=300000ms, got %d", spec.GapMillis)
	}
}

func TestTimeWindowIntegration(t *testing.T) {
	t.Skip("Requires manual TimeWindowSpec creation until parser supports it")

	// Create a manual LogicalWindowAgg with TimeWindowSpec
	timeWindowSpec := &ir.TimeWindowSpec{
		WindowType:  "TUMBLING",
		TimeCol:     "ts",
		SizeMillis:  300000, // 5 minutes
		SlideMillis: 0,
		GapMillis:   0,
	}

	scan := &ir.LogicalScan{Table: "events"}

	wa := &ir.LogicalWindowAgg{
		AggName:        "SUM",
		AggCol:         "amount",
		PartitionBy:    []string{"region"},
		TimeWindowSpec: timeWindowSpec,
		OutputCol:      "total",
		Input:          scan,
	}

	// Convert to DBSP
	node, err := ir.LogicalToDBSP(wa)
	if err != nil {
		t.Fatalf("LogicalToDBSP failed: %v", err)
	}

	// Verify WindowAggOp was created
	windowOp, ok := node.Op.(*op.WindowAggOp)
	if !ok {
		t.Fatalf("expected WindowAggOp, got %T", node.Op)
	}

	if windowOp.Spec.WindowType != op.WindowTypeTumbling {
		t.Errorf("expected TUMBLING window type")
	}

	if windowOp.Spec.TimeCol != "ts" {
		t.Errorf("expected TimeCol=ts, got %s", windowOp.Spec.TimeCol)
	}

	// Test with batch
	batch := types.Batch{
		{Tuple: types.Tuple{"ts": int64(100000), "region": "East", "amount": 100.0}, Count: 1},
		{Tuple: types.Tuple{"ts": int64(350000), "region": "East", "amount": 200.0}, Count: 1},
		{Tuple: types.Tuple{"ts": int64(100000), "region": "West", "amount": 50.0}, Count: 1},
	}

	out, err := windowOp.Apply(batch)
	if err != nil {
		t.Fatalf("Apply failed: %v", err)
	}

	// Should have multiple window results
	if len(out) == 0 {
		t.Error("expected window results")
	}

	t.Logf("Window results: %d deltas", len(out))
	for i, td := range out {
		t.Logf("  [%d] %+v", i, td.Tuple)
	}
}

func TestTimeWindowIntegration_MultiKey_Tumbling_DeleteEvict(t *testing.T) {
	// Manual LogicalWindowAgg construction until parser supports TimeWindowSpec end-to-end.
	timeWindowSpec := &ir.TimeWindowSpec{
		WindowType:  "TUMBLING",
		TimeCol:     "ts",
		SizeMillis:  10,
		SlideMillis: 0,
		GapMillis:   0,
	}

	scan := &ir.LogicalScan{Table: "events"}
	wa := &ir.LogicalWindowAgg{
		AggName:        "SUM",
		AggCol:         "amount",
		PartitionBy:    []string{"k1", "k2"},
		TimeWindowSpec: timeWindowSpec,
		OutputCol:      "total",
		Input:          scan,
	}

	node, err := ir.LogicalToDBSP(wa)
	if err != nil {
		t.Fatalf("LogicalToDBSP failed: %v", err)
	}

	windowOp, ok := node.Op.(*op.WindowAggOp)
	if !ok {
		t.Fatalf("expected WindowAggOp, got %T", node.Op)
	}

	ins := types.Batch{
		{Tuple: types.Tuple{"ts": int64(1), "k1": "A", "k2": "X", "amount": 10.0}, Count: 1},
		{Tuple: types.Tuple{"ts": int64(2), "k1": "A", "k2": "X", "amount": 20.0}, Count: 1},
		{Tuple: types.Tuple{"ts": int64(1), "k1": "B", "k2": "Y", "amount": 5.0}, Count: 1},
	}
	outIns, err := windowOp.Apply(ins)
	if err != nil {
		t.Fatalf("Apply(insert) failed: %v", err)
	}
	if len(outIns) == 0 {
		t.Fatalf("expected non-empty output deltas on insert")
	}
	got := make(map[string]float64)
	for _, td := range outIns {
		if td.Count != 1 {
			t.Fatalf("expected output delta Count=1, got %d (%+v)", td.Count, td)
		}
		if td.Tuple["__window_start"] != int64(0) || td.Tuple["__window_end"] != int64(10) {
			t.Fatalf("expected window [0,10), got start=%v end=%v (%+v)", td.Tuple["__window_start"], td.Tuple["__window_end"], td)
		}
		k1 := td.Tuple["k1"]
		k2 := td.Tuple["k2"]
		if k1 == nil || k2 == nil {
			t.Fatalf("expected output to include k1 and k2, got tuple=%+v", td.Tuple)
		}
		d, ok := td.Tuple["agg_delta"].(float64)
		if !ok {
			t.Fatalf("expected agg_delta float64, got %T (%+v)", td.Tuple["agg_delta"], td)
		}
		got[fmt.Sprintf("%v|%v", k1, k2)] += d
	}
	if got["A|X"] != 30.0 {
		t.Fatalf("expected net agg_delta=30 for group (A,X), got %v (out=%+v)", got["A|X"], outIns)
	}
	if got["B|Y"] != 5.0 {
		t.Fatalf("expected net agg_delta=5 for group (B,Y), got %v (out=%+v)", got["B|Y"], outIns)
	}

	// Delete one contribution in group (A,X).
	del := types.Batch{{Tuple: types.Tuple{"ts": int64(2), "k1": "A", "k2": "X", "amount": 20.0}, Count: -1}}
	outDel, err := windowOp.Apply(del)
	if err != nil {
		t.Fatalf("Apply(delete) failed: %v", err)
	}
	if len(outDel) != 1 {
		t.Fatalf("expected 1 output delta on delete, got %d (%+v)", len(outDel), outDel)
	}
	if outDel[0].Count != 1 {
		t.Fatalf("expected output delta Count=1, got %d (%+v)", outDel[0].Count, outDel[0])
	}
	if outDel[0].Tuple["__window_start"] != int64(0) || outDel[0].Tuple["__window_end"] != int64(10) {
		t.Fatalf("expected window [0,10), got start=%v end=%v (%+v)", outDel[0].Tuple["__window_start"], outDel[0].Tuple["__window_end"], outDel[0])
	}
	if outDel[0].Tuple["k1"] != "A" || outDel[0].Tuple["k2"] != "X" {
		t.Fatalf("expected group key (A,X), got k1=%v k2=%v (%+v)", outDel[0].Tuple["k1"], outDel[0].Tuple["k2"], outDel[0])
	}
	delta, ok := outDel[0].Tuple["agg_delta"].(float64)
	if !ok {
		t.Fatalf("expected agg_delta float64, got %T (%+v)", outDel[0].Tuple["agg_delta"], outDel[0])
	}
	if delta != -20.0 {
		t.Fatalf("expected agg_delta=-20, got %v (%+v)", delta, outDel[0])
	}

	// Delete remaining contributions so group (A,X) evicts.
	del2 := types.Batch{{Tuple: types.Tuple{"ts": int64(1), "k1": "A", "k2": "X", "amount": 10.0}, Count: -1}}
	outDel2, err := windowOp.Apply(del2)
	if err != nil {
		t.Fatalf("Apply(delete2) failed: %v", err)
	}
	if len(outDel2) != 1 {
		t.Fatalf("expected 1 output delta on delete2, got %d (%+v)", len(outDel2), outDel2)
	}
	if outDel2[0].Tuple["k1"] != "A" || outDel2[0].Tuple["k2"] != "X" {
		t.Fatalf("expected group key (A,X) on delete2, got k1=%v k2=%v (%+v)", outDel2[0].Tuple["k1"], outDel2[0].Tuple["k2"], outDel2[0])
	}
	delta2, ok := outDel2[0].Tuple["agg_delta"].(float64)
	if !ok {
		t.Fatalf("expected agg_delta float64 on delete2, got %T (%+v)", outDel2[0].Tuple["agg_delta"], outDel2[0])
	}
	if delta2 != -10.0 {
		t.Fatalf("expected agg_delta=-10 on delete2, got %v (%+v)", delta2, outDel2[0])
	}

	// Now delete (B,Y) too and require full eviction.
	del3 := types.Batch{{Tuple: types.Tuple{"ts": int64(1), "k1": "B", "k2": "Y", "amount": 5.0}, Count: -1}}
	_, err = windowOp.Apply(del3)
	if err != nil {
		t.Fatalf("Apply(delete3) failed: %v", err)
	}

	wid := op.WindowID{Start: 0, End: 10}
	if wm, ok := windowOp.State.Data[wid]; ok {
		if len(wm) != 0 {
			t.Fatalf("expected window state fully evicted for %+v, got %v", wid, wm)
		}
	}
	if cm, ok := windowOp.GroupCounts[wid]; ok {
		if len(cm) != 0 {
			t.Fatalf("expected window group-counts fully evicted for %+v, got %v", wid, cm)
		}
	}
}

func TestTimeWindowIntegration_MultiKey_Sliding_DeleteEvict(t *testing.T) {
	// Manual LogicalWindowAgg construction (SLIDING/HOP style) until parser supports TimeWindowSpec end-to-end.
	timeWindowSpec := &ir.TimeWindowSpec{
		WindowType:  "SLIDING",
		TimeCol:     "ts",
		SizeMillis:  10,
		SlideMillis: 5,
		GapMillis:   0,
	}

	scan := &ir.LogicalScan{Table: "events"}
	wa := &ir.LogicalWindowAgg{
		AggName:        "SUM",
		AggCol:         "amount",
		PartitionBy:    []string{"k1", "k2"},
		TimeWindowSpec: timeWindowSpec,
		OutputCol:      "total",
		Input:          scan,
	}

	node, err := ir.LogicalToDBSP(wa)
	if err != nil {
		t.Fatalf("LogicalToDBSP failed: %v", err)
	}

	windowOp, ok := node.Op.(*op.WindowAggOp)
	if !ok {
		t.Fatalf("expected WindowAggOp, got %T", node.Op)
	}

	ins := types.Batch{{Tuple: types.Tuple{"ts": int64(7), "k1": "A", "k2": "X", "amount": 10.0}, Count: 1}}
	outIns, err := windowOp.Apply(ins)
	if err != nil {
		t.Fatalf("Apply(insert) failed: %v", err)
	}
	if len(outIns) != 2 {
		t.Fatalf("expected 2 window deltas on insert (ts=7 belongs to 2 sliding windows), got %d (%+v)", len(outIns), outIns)
	}
	seen := make(map[op.WindowID]float64)
	for _, td := range outIns {
		if td.Count != 1 {
			t.Fatalf("expected output delta Count=1, got %d (%+v)", td.Count, td)
		}
		if td.Tuple["k1"] != "A" || td.Tuple["k2"] != "X" {
			t.Fatalf("expected output to include k1=A and k2=X, got tuple=%+v", td.Tuple)
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
		seen[op.WindowID{Start: start, End: end}] += d
	}
	if seen[op.WindowID{Start: 0, End: 10}] != 10.0 {
		t.Fatalf("expected agg_delta +10 for window [0,10), got %v (out=%+v)", seen[op.WindowID{Start: 0, End: 10}], outIns)
	}
	if seen[op.WindowID{Start: 5, End: 15}] != 10.0 {
		t.Fatalf("expected agg_delta +10 for window [5,15), got %v (out=%+v)", seen[op.WindowID{Start: 5, End: 15}], outIns)
	}

	del := types.Batch{{Tuple: types.Tuple{"ts": int64(7), "k1": "A", "k2": "X", "amount": 10.0}, Count: -1}}
	outDel, err := windowOp.Apply(del)
	if err != nil {
		t.Fatalf("Apply(delete) failed: %v", err)
	}
	if len(outDel) != 2 {
		t.Fatalf("expected 2 window deltas on delete, got %d (%+v)", len(outDel), outDel)
	}
	seenDel := make(map[op.WindowID]float64)
	for _, td := range outDel {
		if td.Count != 1 {
			t.Fatalf("expected output delta Count=1, got %d (%+v)", td.Count, td)
		}
		if td.Tuple["k1"] != "A" || td.Tuple["k2"] != "X" {
			t.Fatalf("expected output to include k1=A and k2=X, got tuple=%+v", td.Tuple)
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
		seenDel[op.WindowID{Start: start, End: end}] += d
	}
	if seenDel[op.WindowID{Start: 0, End: 10}] != -10.0 {
		t.Fatalf("expected agg_delta -10 for window [0,10), got %v (out=%+v)", seenDel[op.WindowID{Start: 0, End: 10}], outDel)
	}
	if seenDel[op.WindowID{Start: 5, End: 15}] != -10.0 {
		t.Fatalf("expected agg_delta -10 for window [5,15), got %v (out=%+v)", seenDel[op.WindowID{Start: 5, End: 15}], outDel)
	}

	// After delete, both windows should be fully evicted (only one group existed).
	for _, wid := range []op.WindowID{{Start: 0, End: 10}, {Start: 5, End: 15}} {
		if wm, ok := windowOp.State.Data[wid]; ok {
			if len(wm) != 0 {
				t.Fatalf("expected window state fully evicted for %+v, got %v", wid, wm)
			}
		}
		if cm, ok := windowOp.GroupCounts[wid]; ok {
			if len(cm) != 0 {
				t.Fatalf("expected window group-counts fully evicted for %+v, got %v", wid, cm)
			}
		}
	}
}
