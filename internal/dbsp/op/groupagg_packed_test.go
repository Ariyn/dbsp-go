package op

import (
	"testing"

	"github.com/ariyn/dbsp/internal/dbsp/types"
)

func TestGroupAggMultiEmitValueKeepsPackedGroupKeys(t *testing.T) {
	g := NewGroupAggMultiOp(
		func(tup types.Tuple) any { return tup["id"] },
		[]AggSlot{
			{Init: func() any { return float64(0) }, Fn: &SumAgg{ColName: "energy", DeltaCol: "energy"}},
			{Init: func() any { return int64(0) }, Fn: &CountAgg{DeltaCol: "count_delta"}},
		},
	)
	g.EmitValue = true
	g.SetGroupKeyColNames([]string{"id"})

	schema := types.NewPackedSchema([]string{"id", "energy"})
	out, err := g.Apply(types.Batch{{
		Packed: types.NewPackedTupleWithPresence(schema, []any{"panel-a", 3.5}, []bool{true, true}),
		Count:  1,
	}})
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if len(out) != 1 {
		t.Fatalf("expected 1 output row, got %d", len(out))
	}
	if out[0].Packed == nil {
		t.Fatalf("expected packed output, got %+v", out[0])
	}
	materialized := out[0].Packed.Materialize()
	if materialized["id"] != "panel-a" {
		t.Fatalf("expected id to be preserved, got %v", materialized)
	}
	if got := types.ToFloat64(materialized["energy"]); got != 3.5 {
		t.Fatalf("expected energy aggregate 3.5, got %v", materialized)
	}
	if got := types.ToInt64(materialized["count_delta"]); got != 1 {
		t.Fatalf("expected count 1, got %v", materialized)
	}
}
