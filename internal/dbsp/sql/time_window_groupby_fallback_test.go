package sqlconv

import (
	"testing"

	"github.com/ariyn/dbsp/internal/dbsp/ir"
	"github.com/ariyn/dbsp/internal/dbsp/op"
)

func TestParseQueryToLogicalPlan_GroupByTumble_BuildsLogicalWindowAgg(t *testing.T) {
	query := "SELECT k, COUNT(*) AS c FROM events GROUP BY k, TUMBLE(ts, INTERVAL '5' SECOND)"
	lp, err := ParseQueryToLogicalPlan(query)
	if err != nil {
		t.Fatalf("ParseQueryToLogicalPlan: %v", err)
	}
	wa, ok := lp.(*ir.LogicalWindowAgg)
	if !ok {
		t.Fatalf("expected *ir.LogicalWindowAgg, got %T", lp)
	}
	if wa.TimeWindowSpec == nil {
		t.Fatalf("expected TimeWindowSpec to be non-nil")
	}
	if wa.TimeWindowSpec.WindowType != "TUMBLING" {
		t.Fatalf("expected WindowType=TUMBLING, got %s", wa.TimeWindowSpec.WindowType)
	}
	if wa.TimeWindowSpec.TimeCol != "ts" {
		t.Fatalf("expected TimeCol=ts, got %s", wa.TimeWindowSpec.TimeCol)
	}
	if wa.TimeWindowSpec.SizeMillis != 5000 {
		t.Fatalf("expected SizeMillis=5000, got %d", wa.TimeWindowSpec.SizeMillis)
	}
	if len(wa.PartitionBy) != 1 || wa.PartitionBy[0] != "k" {
		t.Fatalf("expected PartitionBy=[k], got %v", wa.PartitionBy)
	}
	if wa.AggName != "COUNT" {
		t.Fatalf("expected AggName=COUNT, got %s", wa.AggName)
	}
	if wa.AggCol != "" {
		t.Fatalf("expected AggCol empty for COUNT(*), got %q", wa.AggCol)
	}
}

func TestParseQueryToDBSP_GroupByTumble_BuildsWindowAggOp(t *testing.T) {
	query := "SELECT k, COUNT(*) AS c FROM events GROUP BY k, TUMBLE(ts, INTERVAL '5' SECOND)"
	n, err := ParseQueryToDBSP(query)
	if err != nil {
		t.Fatalf("ParseQueryToDBSP: %v", err)
	}
	if n == nil || n.Op == nil {
		t.Fatalf("expected non-nil op node")
	}
	wa, ok := n.Op.(*op.WindowAggOp)
	if !ok {
		// Allow for chaining in the future; for now, keep the assertion explicit.
		t.Fatalf("expected *op.WindowAggOp, got %T", n.Op)
	}
	if wa.Spec.WindowType != op.WindowTypeTumbling {
		t.Fatalf("expected tumbling window type, got %s", wa.Spec.WindowType)
	}
	if wa.Spec.TimeCol != "ts" {
		t.Fatalf("expected TimeCol=ts, got %s", wa.Spec.TimeCol)
	}
	if wa.Spec.SizeMillis != 5000 {
		t.Fatalf("expected SizeMillis=5000, got %d", wa.Spec.SizeMillis)
	}
}
