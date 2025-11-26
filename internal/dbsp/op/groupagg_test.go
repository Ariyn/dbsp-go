package op

import (
	"testing"

	"github.com/ariyn/dbsp/internal/dbsp/types"
)

func TestSumAggBasic(t *testing.T) {
	keyFn := func(tu types.Tuple) any { return tu["k"] }
	aggInit := func() any { return float64(0) }
	sumAgg := &SumAgg{}

	g := NewGroupAggOp(keyFn, aggInit, sumAgg)

	batch := types.Batch{
		{Tuple: types.Tuple{"k": "A", "v": 2}, Count: 1},
		{Tuple: types.Tuple{"k": "B", "v": 3}, Count: 1},
		{Tuple: types.Tuple{"k": "A", "v": 4}, Count: 1},
	}
	_, err := g.Apply(batch)
	if err != nil {
		t.Fatalf("Apply failed: %v", err)
	}
	st := g.State()
	if st["A"] != 6.0 {
		t.Fatalf("expected A=6 got %v", st["A"])
	}
	if st["B"] != 3.0 {
		t.Fatalf("expected B=3 got %v", st["B"])
	}

	// delete one of A's value
	del := types.Batch{{Tuple: types.Tuple{"k": "A", "v": 2}, Count: -1}}
	_, err = g.Apply(del)
	if err != nil {
		t.Fatalf("Apply(delete) failed: %v", err)
	}
	st2 := g.State()
	if st2["A"] != 4.0 {
		t.Fatalf("expected A=4 after delete got %v", st2["A"])
	}
}

func TestCountAggBasic(t *testing.T) {
	keyFn := func(tu types.Tuple) any { return tu["k"] }
	aggInit := func() any { return int64(0) }
	countAgg := &CountAgg{}

	g := NewGroupAggOp(keyFn, aggInit, countAgg)

	batch := types.Batch{
		{Tuple: types.Tuple{"k": "A"}, Count: 1},
		{Tuple: types.Tuple{"k": "B"}, Count: 1},
		{Tuple: types.Tuple{"k": "A"}, Count: 1},
	}
	_, err := g.Apply(batch)
	if err != nil {
		t.Fatalf("Apply failed: %v", err)
	}
	st := g.State()
	if st["A"] != int64(2) {
		t.Fatalf("expected A=2 got %v", st["A"])
	}
	if st["B"] != int64(1) {
		t.Fatalf("expected B=1 got %v", st["B"])
	}

	// delete one of A's value
	del := types.Batch{{Tuple: types.Tuple{"k": "A"}, Count: -1}}
	_, err = g.Apply(del)
	if err != nil {
		t.Fatalf("Apply(delete) failed: %v", err)
	}
	st2 := g.State()
	if st2["A"] != int64(1) {
		t.Fatalf("expected A=1 after delete got %v", st2["A"])
	}
}
