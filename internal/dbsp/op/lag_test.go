package op

import (
	"testing"

	"github.com/ariyn/dbsp/internal/dbsp/types"
)

func TestLagAggEmitsAllDeltas(t *testing.T) {
	lag := &LagAgg{OrderByCol: "ts", LagCol: "v", Offset: 1, OutputCol: "v_last"}
	g := NewGroupAggOp(func(t types.Tuple) any { return t["id"] }, func() any {
		return LagMonoid{Buffer: NewOrderedBuffer("ts")}
	}, lag)

	batch := types.Batch{
		{Tuple: types.Tuple{"id": "a", "ts": int64(1), "v": 10.0}, Count: 1},
		{Tuple: types.Tuple{"id": "a", "ts": int64(2), "v": 20.0}, Count: 1},
		{Tuple: types.Tuple{"id": "a", "ts": int64(3), "v": 30.0}, Count: 1},
	}

	out, err := g.Apply(batch)
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if len(out) != 3 {
		t.Fatalf("expected 3 output rows, got %d", len(out))
	}

	seen := make(map[int64]any)
	for _, td := range out {
		if td.Count != 1 {
			t.Fatalf("expected count 1, got %d", td.Count)
		}
		ts, ok := td.Tuple["ts"].(int64)
		if !ok {
			t.Fatalf("expected ts int64, got %T", td.Tuple["ts"])
		}
		seen[ts] = td.Tuple["v_last"]
	}

	if v, ok := seen[int64(1)]; !ok || v != nil {
		t.Fatalf("expected ts=1 lag nil, got %v", v)
	}
	if v, ok := seen[int64(2)]; !ok || types.ToFloat64(v) != 10.0 {
		t.Fatalf("expected ts=2 lag 10.0, got %v", v)
	}
	if v, ok := seen[int64(3)]; !ok || types.ToFloat64(v) != 20.0 {
		t.Fatalf("expected ts=3 lag 20.0, got %v", v)
	}
}
