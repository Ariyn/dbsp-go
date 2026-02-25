package op

import (
	"testing"

	"github.com/ariyn/dbsp/internal/dbsp/types"
)

func TestLagAgg_WithExpression(t *testing.T) {
	lag := &LagAgg{
		OrderByCol: "ts",
		LagCol:     "unused",
		LagExpr: func(tu types.Tuple) (any, error) {
			return tu["a"].(float64) + tu["b"].(float64), nil
		},
		Offset:    1,
		OutputCol: "prev_sum",
	}

	g := NewGroupAggOp(
		func(tu types.Tuple) any { return tu["k"] },
		func() any { return LagMonoid{Buffer: NewOrderedBuffer("ts")} },
		lag,
	)

	out, err := g.Apply(types.Batch{
		{Tuple: types.Tuple{"k": "A", "ts": int64(1), "a": 1.0, "b": 10.0}, Count: 1},
		{Tuple: types.Tuple{"k": "A", "ts": int64(2), "a": 2.0, "b": 20.0}, Count: 1},
	})
	if err != nil {
		t.Fatalf("Apply failed: %v", err)
	}
	if len(out) == 0 {
		t.Fatalf("expected output deltas")
	}

	found := false
	for _, td := range out {
		if td.Tuple["ts"] == int64(2) && td.Tuple["prev_sum"] == 11.0 {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected row ts=2 with prev_sum=11.0, got %v", out)
	}
}

func TestLagAgg_MultipleAffectedRows_ReturnsFirstDeltaRegression(t *testing.T) {
	lag := &LagAgg{
		OrderByCol: "ts",
		LagCol:     "v",
		Offset:     1,
		OutputCol:  "prev_v",
	}

	g := NewGroupAggOp(
		func(types.Tuple) any { return "A" },
		func() any { return LagMonoid{Buffer: NewOrderedBuffer("ts")} },
		lag,
	)

	_, err := g.Apply(types.Batch{
		{Tuple: types.Tuple{"ts": int64(1), "v": 10.0}, Count: 1},
		{Tuple: types.Tuple{"ts": int64(3), "v": 30.0}, Count: 1},
	})
	if err != nil {
		t.Fatalf("seed Apply failed: %v", err)
	}

	// Insert at ts=2 affects downstream row ts=3 as well; current implementation
	// returns only the first delta when multiple are produced.
	out, err := g.Apply(types.Batch{{Tuple: types.Tuple{"ts": int64(2), "v": 20.0}, Count: 1}})
	if err != nil {
		t.Fatalf("Apply failed: %v", err)
	}
	if len(out) != 1 {
		t.Fatalf("expected exactly 1 returned delta due to current behavior, got %d (%v)", len(out), out)
	}
}

func TestOrderedBuffer_Add_OutOfOrderMapPayload_NoPanic(t *testing.T) {
	buf := NewOrderedBuffer("ts")

	buf.Add(types.Tuple{
		"ts":      int64(2),
		"payload": map[string]any{"k": "v"},
	}, 1)

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("unexpected panic while inserting out-of-order map payload: %v", r)
		}
	}()

	pos, _ := buf.Add(types.Tuple{
		"ts":      int64(1),
		"payload": map[string]any{"k": "v"},
	}, 1)

	if pos != 0 {
		t.Fatalf("expected insert position 0, got %d", pos)
	}
}
