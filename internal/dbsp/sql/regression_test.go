package sqlconv

import (
	"reflect"
	"testing"

	"github.com/ariyn/dbsp/internal/dbsp/ir"
	"github.com/ariyn/dbsp/internal/dbsp/op"
)

func TestAutoGroupingRegression(t *testing.T) {
	tests := []struct {
		name    string
		query   string
		wantAgg bool
	}{
		{
			name:    "TIME_BUCKET with aggregates should trigger Auto-Grouping",
			query:   "SELECT key_a, TIME_BUCKET(INTERVAL '5 min', ts::TIMESTAMP) as bucket, AVG(value_a) as avg_a FROM source_table",
			wantAgg: true,
		},
		{
			name:    "Aggregates without GROUP BY should trigger Auto-Grouping",
			query:   "SELECT AVG(value_a) as avg_a, SUM(value_b) as sum_b FROM source_table",
			wantAgg: true,
		},
		{
			name:    "Normal SELECT without aggregates should NOT trigger Auto-Grouping",
			query:   "SELECT key_a, key_b FROM source_table",
			wantAgg: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lp, err := ParseQueryToLogicalPlan(tt.query)
			if err != nil {
				t.Fatalf("ParseQueryToLogicalPlan failed: %v", err)
			}
			hasAgg := false
			curr := lp
			for curr != nil {
				if _, ok := curr.(*ir.LogicalGroupAgg); ok {
					hasAgg = true
					break
				}
				switch v := curr.(type) {
				case *ir.LogicalProject:
					curr = v.Input
				case *ir.LogicalView:
					curr = v.Input
				default:
					curr = nil
				}
			}
			if hasAgg != tt.wantAgg {
				t.Errorf("Auto-Grouping trigger mismatch: got hasAgg=%v, want %v", hasAgg, tt.wantAgg)
			}
		})
	}
}

func TestNilTimeRegression(t *testing.T) {
	query := "SELECT TIME_BUCKET(INTERVAL '5 min', ts::TIMESTAMP) AS bucket FROM source_table"
	_, err := ParseQueryToLogicalPlan(query)
	if err != nil {
		t.Errorf("Failed to parse time bucket query: %v", err)
	}
}

func TestAggregateProjectionMapping(t *testing.T) {
	query := "SELECT ROUND(AVG(value_a), 2) AS avg_a FROM source_table"
	lp, err := ParseQueryToLogicalPlan(query)
	if err != nil {
		t.Fatalf("Failed to parse aggregate projection: %v", err)
	}
	if lp == nil {
		t.Fatal("Logical plan is nil")
	}
}

func TestAutoGroupingIncludesTimeBucketAliasAndAggAliases(t *testing.T) {
	query := "SELECT key_a, key_b, TIME_BUCKET(INTERVAL '5 min', ts::TIMESTAMP) AS bucket, ROUND(AVG(value_a), 2) AS avg_a, SUM(value_b) AS sum_b FROM source_table"
	lp, err := ParseQueryToLogicalPlan(query)
	if err != nil {
		t.Fatalf("ParseQueryToLogicalPlan failed: %v", err)
	}
	ga := findFirstGroupAgg(lp)
	if ga == nil {
		t.Fatal("Expected LogicalGroupAgg but got nil")
	}
	keys := make(map[string]struct{}, len(ga.Keys))
	for _, k := range ga.Keys {
		keys[k] = struct{}{}
	}
	for _, want := range []string{"key_a", "key_b", "bucket"} {
		if _, ok := keys[want]; !ok {
			t.Fatalf("Expected group key %q to be present, got keys=%v", want, ga.Keys)
		}
	}
	if len(ga.Aggs) == 0 {
		t.Fatal("Expected multi-aggregate specs")
	}
	aliases := make(map[string]struct{}, len(ga.Aggs))
	for _, a := range ga.Aggs {
		if a.As != "" {
			aliases[a.As] = struct{}{}
		}
	}
	for _, want := range []string{"avg_a", "sum_b"} {
		if _, ok := aliases[want]; !ok {
			t.Fatalf("Expected aggregate alias %q to be present, got aliases=%v", want, aliases)
		}
	}
}

func TestWindowAggregateDoesNotTriggerGroupAgg(t *testing.T) {
	query := "SELECT SUM(value_b) OVER (PARTITION BY key_a ORDER BY bucket) AS running_sum, key_a FROM source_table"
	lp, err := ParseQueryToLogicalPlan(query)
	if err != nil {
		t.Fatalf("ParseQueryToLogicalPlan failed: %v", err)
	}
	if ga := findFirstGroupAgg(lp); ga != nil {
		t.Fatalf("Did not expect LogicalGroupAgg for window aggregate query, got keys=%v", ga.Keys)
	}
}

func TestGroupByBeforeWindowAgg(t *testing.T) {
	query := "SELECT key_a, ts, SUM(value_b) AS total, SUM(value_b) OVER (PARTITION BY key_a ORDER BY ts) AS running_total FROM source_table GROUP BY key_a, ts"
	lp, err := ParseQueryToLogicalPlan(query)
	if err != nil {
		t.Fatalf("ParseQueryToLogicalPlan failed: %v", err)
	}
	chain := collectLogicalChain(lp)
	waIdx := indexOfNode(chain, "*ir.LogicalWindowAgg")
	gaIdx := indexOfNode(chain, "*ir.LogicalGroupAgg")
	if waIdx == -1 {
		t.Fatalf("Expected LogicalWindowAgg but got nil; chain=%v", chain)
	}
	if gaIdx == -1 {
		t.Fatalf("Expected LogicalGroupAgg but got nil; chain=%v", chain)
	}
	if waIdx > gaIdx {
		t.Fatalf("Expected window aggregate to run after group by (window above group); chain=%v", chain)
	}
}

func TestIncrementalPlanEmitsValueDeltas(t *testing.T) {
	query := "SELECT key_a, SUM(value_b) AS total FROM source_table GROUP BY key_a"
	root, err := ParseQueryToIncrementalDBSP(query)
	if err != nil {
		t.Fatalf("ParseQueryToIncrementalDBSP failed: %v", err)
	}
	if root == nil {
		t.Fatal("Expected non-nil root operator")
	}
	g := findFirstGroupAggOp(root)
	if g == nil {
		t.Fatal("Expected GroupAggOp in incremental plan")
	}
	if !g.EmitValue {
		t.Fatal("Expected GroupAggOp to emit value deltas")
	}
}

func TestWindowAggEmitsValueDeltas(t *testing.T) {
	query := "SELECT key_a, SUM(value_b) OVER (PARTITION BY key_a ORDER BY ts) AS running_sum FROM source_table"
	root, err := ParseQueryToIncrementalDBSP(query)
	if err != nil {
		t.Fatalf("ParseQueryToIncrementalDBSP failed: %v", err)
	}
	if root == nil {
		t.Fatal("Expected non-nil root operator")
	}
	wa := findFirstWindowAggOp(root)
	if wa == nil {
		t.Fatal("Expected WindowAggOp in incremental plan")
	}
	if !wa.EmitValue {
		t.Fatal("Expected WindowAggOp to emit value deltas")
	}
}

func TestWindowFuncAliasPreservedInLogicalPlan(t *testing.T) {
	query := "WITH lagged_data AS (SELECT timestamp, panel_position, LAG(timestamp) OVER (PARTITION BY panel_position ORDER BY timestamp) AS timestamp_last FROM events) SELECT * FROM lagged_data"
	lp, err := ParseQueryToLogicalPlan(query)
	if err != nil {
		t.Fatalf("ParseQueryToLogicalPlan failed: %v", err)
	}
	wf := findWindowFuncByOutput(lp, "timestamp_last")
	if wf == nil {
		t.Fatal("Expected LogicalWindowFunc with output timestamp_last")
	}
}

func TestWindowFuncAliasPreservedInDBSP(t *testing.T) {
	query := "WITH lagged_data AS (SELECT timestamp, panel_position, LAG(timestamp) OVER (PARTITION BY panel_position ORDER BY timestamp) AS timestamp_last FROM events) SELECT * FROM lagged_data"
	root, err := ParseQueryToIncrementalDBSP(query)
	if err != nil {
		t.Fatalf("ParseQueryToIncrementalDBSP failed: %v", err)
	}
	lag := findFirstLagAgg(root)
	if lag == nil {
		t.Fatal("Expected LagAgg in incremental plan")
	}
	if lag.OutputCol != "timestamp_last" {
		t.Fatalf("Expected LagAgg OutputCol=timestamp_last, got %q", lag.OutputCol)
	}
}

func findFirstGroupAgg(n ir.LogicalNode) *ir.LogicalGroupAgg {
	for n != nil {
		if ga, ok := n.(*ir.LogicalGroupAgg); ok {
			return ga
		}
		n = nextLogicalInput(n)
	}
	return nil
}

func findFirstWindowAgg(n ir.LogicalNode) *ir.LogicalWindowAgg {
	for n != nil {
		if wa, ok := n.(*ir.LogicalWindowAgg); ok {
			return wa
		}
		n = nextLogicalInput(n)
	}
	return nil
}

func collectLogicalChain(n ir.LogicalNode) []string {
	var out []string
	for n != nil {
		out = append(out, reflect.TypeOf(n).String())
		n = nextLogicalInput(n)
	}
	return out
}

func indexOfNode(chain []string, name string) int {
	for i, v := range chain {
		if v == name {
			return i
		}
	}
	return -1
}

func nextLogicalInput(n ir.LogicalNode) ir.LogicalNode {
	switch v := n.(type) {
	case *ir.LogicalProject:
		return v.Input
	case *ir.LogicalView:
		return v.Input
	case *ir.LogicalFilter:
		return v.Input
	case *ir.LogicalGroupAgg:
		return v.Input
	case *ir.LogicalWindowAgg:
		return v.Input
	case *ir.LogicalWindowFunc:
		return v.Input
	case *ir.LogicalSort:
		return v.Input
	case *ir.LogicalLimit:
		return v.Input
	case *ir.LogicalWith:
		return v.Body
	default:
		return nil
	}
}

func findFirstGroupAggOp(n *op.Node) *op.GroupAggOp {
	if n == nil {
		return nil
	}
	if g, ok := n.Op.(*op.GroupAggOp); ok {
		return g
	}
	for _, in := range n.Inputs {
		if g := findFirstGroupAggOp(in); g != nil {
			return g
		}
	}
	return nil
}

func findFirstWindowAggOp(n *op.Node) *op.WindowAggOp {
	if n == nil {
		return nil
	}
	if w, ok := n.Op.(*op.WindowAggOp); ok {
		return w
	}
	if chained, ok := n.Op.(*op.ChainedOp); ok {
		for _, inner := range chained.Ops {
			if w, ok := inner.(*op.WindowAggOp); ok {
				return w
			}
		}
	}
	for _, in := range n.Inputs {
		if w := findFirstWindowAggOp(in); w != nil {
			return w
		}
	}
	return nil
}

func findWindowFuncByOutput(n ir.LogicalNode, output string) *ir.LogicalWindowFunc {
	for n != nil {
		if wf, ok := n.(*ir.LogicalWindowFunc); ok {
			if wf.OutputCol == output {
				return wf
			}
		}
		n = nextLogicalInput(n)
	}
	return nil
}

func findFirstLagAgg(n *op.Node) *op.LagAgg {
	if n == nil {
		return nil
	}
	if g, ok := n.Op.(*op.GroupAggOp); ok {
		if lag, ok := g.AggFn.(*op.LagAgg); ok {
			return lag
		}
	}
	if chained, ok := n.Op.(*op.ChainedOp); ok {
		for _, inner := range chained.Ops {
			if g, ok := inner.(*op.GroupAggOp); ok {
				if lag, ok := g.AggFn.(*op.LagAgg); ok {
					return lag
				}
			}
		}
	}
	for _, in := range n.Inputs {
		if lag := findFirstLagAgg(in); lag != nil {
			return lag
		}
	}
	return nil
}
