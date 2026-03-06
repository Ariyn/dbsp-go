package ir

import (
	"testing"

	"github.com/ariyn/dbsp/internal/dbsp/op"
	"github.com/ariyn/dbsp/internal/dbsp/types"
)

func TestWrapWindowAggOutputAliasAddsAliasWithoutDroppingAggregate(t *testing.T) {
	node := &op.Node{
		Op: &op.MapOp{F: func(td types.TupleDelta) []types.TupleDelta {
			return []types.TupleDelta{{Tuple: types.CloneTuple(td.Tuple), Count: td.Count}}
		}},
		Inputs: []*op.Node{{Source: "events"}},
	}
	wrapped := wrapWindowAggOutputAlias(node, "cumulative_energy")

	out, err := op.ExecuteTick(wrapped, map[string]types.Batch{
		"events": {{Tuple: types.Tuple{"id": "a", "agg_result": 3.5}, Count: 1}},
	})
	if err != nil {
		t.Fatalf("execute tick: %v", err)
	}
	if len(out) != 1 {
		t.Fatalf("expected 1 output row, got %d", len(out))
	}
	if got := types.ToFloat64(out[0].Tuple["agg_result"]); got != 3.5 {
		t.Fatalf("expected agg_result 3.5, got %v", out[0].Tuple)
	}
	if got := types.ToFloat64(out[0].Tuple["cumulative_energy"]); got != 3.5 {
		t.Fatalf("expected cumulative_energy 3.5, got %v", out[0].Tuple)
	}
}

func TestWrapWindowAggOutputAliasSurvivesPackedProjection(t *testing.T) {
	node := &op.Node{
		Op: &op.MapOp{F: func(td types.TupleDelta) []types.TupleDelta {
			return []types.TupleDelta{td}
		}},
		Inputs: []*op.Node{{Source: "events"}},
	}
	wrapped := wrapWindowAggOutputAlias(node, "cumulative_energy")
	root := &op.Node{
		Op:     &op.ProjectOp{Columns: []string{"id", "agg_result", "cumulative_energy"}},
		Inputs: []*op.Node{wrapped},
	}

	schema := types.NewPackedSchema([]string{"id"})
	out, err := op.ExecuteTick(root, map[string]types.Batch{
		"events": {{Packed: types.NewPackedTupleWithPresence(schema, []any{"a"}, []bool{true}).WithExtra("agg_result", 3.5), Count: 1}},
	})
	if err != nil {
		t.Fatalf("execute tick: %v", err)
	}
	if len(out) != 1 {
		t.Fatalf("expected 1 output row, got %d", len(out))
	}
	if out[0].Packed != nil {
		t.Fatalf("expected materialized tuple output after non-packed alias op, got packed=%+v", out[0].Packed)
	}
	if got := types.ToFloat64(out[0].Tuple["agg_result"]); got != 3.5 {
		t.Fatalf("expected agg_result 3.5, got %v", out[0].Tuple)
	}
	if got := types.ToFloat64(out[0].Tuple["cumulative_energy"]); got != 3.5 {
		t.Fatalf("expected cumulative_energy 3.5, got %v", out[0].Tuple)
	}
}