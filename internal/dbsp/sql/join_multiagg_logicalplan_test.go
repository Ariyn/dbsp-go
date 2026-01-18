package sqlconv

import (
	"testing"

	"github.com/ariyn/dbsp/internal/dbsp/ir"
)

func TestParseQueryToLogicalPlan_JoinMultiAggPopulatesAggs(t *testing.T) {
	q := "SELECT a.k AS k, SUM(b.v), COUNT(b.id) FROM a JOIN b ON a.id = b.id GROUP BY a.k"
	lp, err := ParseQueryToLogicalPlan(q)
	if err != nil {
		t.Fatalf("ParseQueryToLogicalPlan: %v", err)
	}
	g, ok := lp.(*ir.LogicalGroupAgg)
	if !ok {
		t.Fatalf("expected *ir.LogicalGroupAgg, got %T", lp)
	}
	if len(g.Aggs) != 2 {
		t.Fatalf("expected Aggs len=2, got %d (AggName=%q AggCol=%q Aggs=%+v)", len(g.Aggs), g.AggName, g.AggCol, g.Aggs)
	}
	if g.Aggs[0].Name == "" || g.Aggs[1].Name == "" {
		t.Fatalf("expected AggSpec names to be set: %+v", g.Aggs)
	}
}
