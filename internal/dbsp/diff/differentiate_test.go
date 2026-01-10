package diff

import (
	"testing"

	"github.com/ariyn/dbsp/internal/dbsp/op"
	"github.com/ariyn/dbsp/internal/dbsp/types"
)

func TestDifferentiate_MapOp(t *testing.T) {
	filterFn := func(td types.TupleDelta) []types.TupleDelta {
		if val, ok := td.Tuple["status"]; ok && val == "active" {
			return []types.TupleDelta{td}
		}
		return nil
	}

	mapOp := &op.MapOp{F: filterFn}
	node := &op.Node{Op: mapOp}

	dNode, err := Differentiate(node)
	if err != nil {
		t.Fatalf("Differentiate failed: %v", err)
	}

	_, ok := dNode.Op.(*op.MapOp)
	if !ok {
		t.Fatalf("expected MapOp after differentiation, got %T", dNode.Op)
	}

	batch := types.Batch{
		{Tuple: types.Tuple{"status": "active", "id": 1}, Count: 1},
		{Tuple: types.Tuple{"status": "inactive", "id": 2}, Count: 1},
	}

	out, err := dNode.Op.Apply(batch)
	if err != nil {
		t.Fatalf("Apply failed: %v", err)
	}

	if len(out) != 1 {
		t.Fatalf("expected 1 output, got %d", len(out))
	}
	if out[0].Tuple["id"] != 1 {
		t.Errorf("expected id=1, got %v", out[0].Tuple["id"])
	}
}

func TestDifferentiate_GroupAggOp(t *testing.T) {
	keyFn := func(tu types.Tuple) any { return tu["region"] }
	aggInit := func() any { return float64(0) }
	sumAgg := &op.SumAgg{ColName: "sales"}

	groupOp := op.NewGroupAggOp(keyFn, aggInit, sumAgg)
	node := &op.Node{Op: groupOp}

	dNode, err := Differentiate(node)
	if err != nil {
		t.Fatalf("Differentiate failed: %v", err)
	}

	_, ok := dNode.Op.(*op.GroupAggOp)
	if !ok {
		t.Fatalf("expected GroupAggOp after differentiation, got %T", dNode.Op)
	}

	batch1 := types.Batch{
		{Tuple: types.Tuple{"region": "East", "sales": 100}, Count: 1},
		{Tuple: types.Tuple{"region": "West", "sales": 200}, Count: 1},
	}

	out1, err := dNode.Op.Apply(batch1)
	if err != nil {
		t.Fatalf("Apply batch1 failed: %v", err)
	}
	if len(out1) != 2 {
		t.Fatalf("expected 2 outputs, got %d", len(out1))
	}

	batch2 := types.Batch{
		{Tuple: types.Tuple{"region": "East", "sales": 50}, Count: 1},
		{Tuple: types.Tuple{"region": "East", "sales": 30}, Count: -1},
	}

	out2, err := dNode.Op.Apply(batch2)
	if err != nil {
		t.Fatalf("Apply batch2 failed: %v", err)
	}

	gop := dNode.Op.(*op.GroupAggOp)
	state := gop.State()

	if state["East"] != 120.0 {
		t.Errorf("expected East=120, got %v", state["East"])
	}
	if state["West"] != 200.0 {
		t.Errorf("expected West=200, got %v", state["West"])
	}

	if len(out2) == 0 {
		t.Fatalf("expected non-empty output deltas")
	}
	var net float64
	for _, td := range out2 {
		d, ok := td.Tuple["agg_delta"].(float64)
		if ok {
			net += d
		}
	}
	if net != 20.0 {
		t.Fatalf("expected net agg_delta=20, got %v (out=%+v)", net, out2)
	}
}

func TestDifferentiate_NilNode(t *testing.T) {
	_, err := Differentiate(nil)
	if err == nil {
		t.Fatal("expected error for nil node")
	}
}

func TestDifferentiateGraph_SingleNode(t *testing.T) {
	keyFn := func(tu types.Tuple) any { return tu["k"] }
	aggInit := func() any { return int64(0) }
	countAgg := &op.CountAgg{}

	groupOp := op.NewGroupAggOp(keyFn, aggInit, countAgg)
	node := &op.Node{Op: groupOp}

	dGraph, err := DifferentiateGraph(node)
	if err != nil {
		t.Fatalf("DifferentiateGraph failed: %v", err)
	}

	if dGraph == nil || dGraph.Op == nil {
		t.Fatal("expected non-nil differentiated graph")
	}

	batch := types.Batch{
		{Tuple: types.Tuple{"k": "A"}, Count: 1},
		{Tuple: types.Tuple{"k": "A"}, Count: 1},
		{Tuple: types.Tuple{"k": "B"}, Count: 1},
	}

	out, err := op.Execute(dGraph, batch)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	if len(out) == 0 {
		t.Fatal("expected non-empty output")
	}
}
