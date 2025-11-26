package ir

import (
	"testing"

	"github.com/ariyn/dbsp/internal/dbsp/op"
	"github.com/ariyn/dbsp/internal/dbsp/types"
)

func TestLogicalToDBSP_SumGroupAgg(t *testing.T) {
	scan := &LogicalScan{Table: "orders"}
	agg := &LogicalGroupAgg{
		Keys:    []string{"customer_id"},
		AggName: "SUM",
		AggCol:  "amount",
		Input:   scan,
	}

	node, err := LogicalToDBSP(agg)
	if err != nil {
		t.Fatalf("LogicalToDBSP failed: %v", err)
	}
	if node == nil || node.Op == nil {
		t.Fatal("expected non-nil node with operator")
	}

	// Verify it's a GroupAggOp
	_, ok := node.Op.(*op.GroupAggOp)
	if !ok {
		t.Fatalf("expected GroupAggOp, got %T", node.Op)
	}

	// Test execution with sample data
	batch := types.Batch{
		{Tuple: types.Tuple{"customer_id": "C1", "amount": 100}, Count: 1},
		{Tuple: types.Tuple{"customer_id": "C1", "amount": 50}, Count: 1},
		{Tuple: types.Tuple{"customer_id": "C2", "amount": 75}, Count: 1},
	}

	out, err := op.Execute(node, batch)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}
	if len(out) == 0 {
		t.Fatal("expected non-empty output")
	}
}

func TestLogicalToDBSP_CountGroupAgg(t *testing.T) {
	scan := &LogicalScan{Table: "events"}
	agg := &LogicalGroupAgg{
		Keys:    []string{"event_type"},
		AggName: "COUNT",
		AggCol:  "id",
		Input:   scan,
	}

	node, err := LogicalToDBSP(agg)
	if err != nil {
		t.Fatalf("LogicalToDBSP failed: %v", err)
	}

	// Verify it's a GroupAggOp with CountAgg
	gop, ok := node.Op.(*op.GroupAggOp)
	if !ok {
		t.Fatalf("expected GroupAggOp, got %T", node.Op)
	}
	if gop.AggFn == nil {
		t.Fatal("expected non-nil AggFn")
	}

	// Test execution
	batch := types.Batch{
		{Tuple: types.Tuple{"event_type": "click"}, Count: 1},
		{Tuple: types.Tuple{"event_type": "click"}, Count: 1},
		{Tuple: types.Tuple{"event_type": "view"}, Count: 1},
	}

	out, err := op.Execute(node, batch)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}
	if len(out) == 0 {
		t.Fatal("expected non-empty output")
	}

	// Verify state
	st := gop.State()
	if st["click"] != int64(2) {
		t.Errorf("expected click count=2, got %v", st["click"])
	}
	if st["view"] != int64(1) {
		t.Errorf("expected view count=1, got %v", st["view"])
	}
}

func TestLogicalToDBSP_MultipleKeys_Error(t *testing.T) {
	scan := &LogicalScan{Table: "sales"}
	agg := &LogicalGroupAgg{
		Keys:    []string{"region", "product"}, // Multiple keys not supported
		AggName: "SUM",
		AggCol:  "revenue",
		Input:   scan,
	}

	_, err := LogicalToDBSP(agg)
	if err == nil {
		t.Fatal("expected error for multiple group keys")
	}
}

func TestLogicalToDBSP_UnsupportedAgg_Error(t *testing.T) {
	scan := &LogicalScan{Table: "data"}
	agg := &LogicalGroupAgg{
		Keys:    []string{"key"},
		AggName: "AVG", // Not supported
		AggCol:  "value",
		Input:   scan,
	}

	_, err := LogicalToDBSP(agg)
	if err == nil {
		t.Fatal("expected error for unsupported aggregate")
	}
}

func TestLogicalToDBSP_NonScanInput_Error(t *testing.T) {
	// Create a GroupAgg with another GroupAgg as input (not supported)
	innerScan := &LogicalScan{Table: "t"}
	innerAgg := &LogicalGroupAgg{
		Keys:    []string{"k"},
		AggName: "SUM",
		AggCol:  "v",
		Input:   innerScan,
	}
	outerAgg := &LogicalGroupAgg{
		Keys:    []string{"k"},
		AggName: "COUNT",
		AggCol:  "v",
		Input:   innerAgg, // Non-scan input
	}

	_, err := LogicalToDBSP(outerAgg)
	if err == nil {
		t.Fatal("expected error for non-scan input to GroupAgg")
	}
}
