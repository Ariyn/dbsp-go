package sqlconv

import (
	"testing"

	"github.com/ariyn/dbsp/internal/dbsp/ir"
	"github.com/ariyn/dbsp/internal/dbsp/op"
	"github.com/ariyn/dbsp/internal/dbsp/types"
)

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
	// Test precedence override with parentheses
	// Without parens: age > 50 OR status = 'vip' AND amount > 100
	// Natural: age > 50 OR (status = 'vip' AND amount > 100)
	// With parens: (age > 50 OR status = 'vip') AND amount > 100

	q := "SELECT * FROM customers WHERE (age > 50 OR status = 'vip') AND amount > 100"
	node, err := ParseQueryToDBSP(q)
	if err != nil {
		t.Fatalf("ParseQueryToDBSP failed: %v", err)
	}

	batch := types.Batch{
		// age > 50, OR is true, but amount <= 100 -> false
		{Tuple: types.Tuple{"id": 1, "age": 60, "status": "normal", "amount": 50}, Count: 1},
		// age > 50, OR is true, and amount > 100 -> true
		{Tuple: types.Tuple{"id": 2, "age": 55, "status": "normal", "amount": 150}, Count: 1},
		// status = 'vip', OR is true, and amount > 100 -> true
		{Tuple: types.Tuple{"id": 3, "age": 30, "status": "vip", "amount": 200}, Count: 1},
		// neither condition, OR is false -> false
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
	// ((age >= 18 AND age <= 30) OR (age >= 60 AND age <= 70)) AND status = 'active'
	q := "SELECT * FROM users WHERE ((age >= 18 AND age <= 30) OR (age >= 60 AND age <= 70)) AND status = 'active'"
	node, err := ParseQueryToDBSP(q)
	if err != nil {
		t.Fatalf("ParseQueryToDBSP failed: %v", err)
	}

	batch := types.Batch{
		{Tuple: types.Tuple{"id": 1, "age": 25, "status": "active"}, Count: 1},   // young active -> true
		{Tuple: types.Tuple{"id": 2, "age": 65, "status": "active"}, Count: 1},   // old active -> true
		{Tuple: types.Tuple{"id": 3, "age": 45, "status": "active"}, Count: 1},   // middle active -> false
		{Tuple: types.Tuple{"id": 4, "age": 25, "status": "inactive"}, Count: 1}, // young inactive -> false
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

func TestParseQueryWithTumbleWindow(t *testing.T) {
	q := "SELECT TUMBLE(ts, INTERVAL '5' MINUTE), SUM(amount) FROM t GROUP BY TUMBLE(ts, INTERVAL '5' MINUTE)"
	lp, err := ParseQueryToLogicalPlan(q)
	if err != nil {
		t.Fatalf("ParseQueryToLogicalPlan with TUMBLE failed: %v", err)
	}

	ga, ok := lp.(*ir.LogicalGroupAgg)
	if !ok {
		t.Fatalf("expected LogicalGroupAgg, got %T", lp)
	}
	if ga.WindowSpec == nil {
		t.Fatalf("expected non-nil WindowSpec for TUMBLE query")
	}
	if ga.WindowSpec.TimeCol != "ts" {
		t.Errorf("expected TimeCol=ts, got %s", ga.WindowSpec.TimeCol)
	}
	// 5 minutes in millis
	if ga.WindowSpec.SizeMillis != 5*60*1000 {
		t.Errorf("expected SizeMillis=300000, got %d", ga.WindowSpec.SizeMillis)
	}

	// Also ensure that executing the DBSP node groups by window correctly.
	node, err := ParseQueryToDBSP(q)
	if err != nil {
		t.Fatalf("ParseQueryToDBSP with TUMBLE failed: %v", err)
	}

	// ts is in millis: 0ms, 2min, 7min (so windows [0,5), [5,10))
	batch := types.Batch{
		{Tuple: types.Tuple{"ts": int64(0), "amount": 100}, Count: 1},             // window 0-5
		{Tuple: types.Tuple{"ts": int64(2 * 60 * 1000), "amount": 50}, Count: 1},  // window 0-5
		{Tuple: types.Tuple{"ts": int64(7 * 60 * 1000), "amount": 200}, Count: 1}, // window 5-10
	}

	out, err := op.Execute(node, batch)
	if err != nil {
		t.Fatalf("Execute with TUMBLE failed: %v", err)
	}
	if len(out) == 0 {
		t.Fatalf("expected non-empty output for TUMBLE aggregation")
	}

	// Inspect internal state: keys should be window starts 0 and 5*60*1000
	gop, ok := node.Op.(*op.GroupAggOp)
	if !ok {
		t.Fatalf("expected GroupAggOp, got %T", node.Op)
	}
	state := gop.State()
	w0 := int64(0)
	w1 := int64(5 * 60 * 1000)
	if state[w0] != float64(150) {
		t.Errorf("expected window[0] sum=150, got %v", state[w0])
	}
	if state[w1] != float64(200) {
		t.Errorf("expected window[5min] sum=200, got %v", state[w1])
	}
}
