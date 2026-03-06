package op

import (
	"testing"
	"time"

	"github.com/ariyn/dbsp/internal/dbsp/types"
)

func TestGroupAggStateTTL(t *testing.T) {
	g := NewGroupAggOp(
		func(t types.Tuple) any { return t["k"] },
		func() any { return float64(0) },
		&SumAgg{ColName: "v"},
	)
	g.SetStateTTL(2 * time.Millisecond)

	batch := types.Batch{{Tuple: types.Tuple{"k": "a", "v": 1.0}, Count: 1}}
	if _, err := g.Apply(batch); err != nil {
		t.Fatalf("apply: %v", err)
	}
	if got := len(g.State()); got != 1 {
		t.Fatalf("expected 1 state entry, got %d", got)
	}

	time.Sleep(5 * time.Millisecond)
	if _, err := g.Apply(nil); err != nil {
		t.Fatalf("apply for eviction: %v", err)
	}
	if got := len(g.State()); got != 0 {
		t.Fatalf("expected state to be evicted, got %d", got)
	}
}

func TestWindowAggStateTTL(t *testing.T) {
	agg := &SumAgg{ColName: "v"}
	w := NewWindowAggOp(
		WindowSpecLite{TimeCol: "ts", SizeMillis: 1000, WindowType: WindowTypeTumbling},
		func(t types.Tuple) any { return t["k"] },
		[]string{"k"},
		func() any { return float64(0) },
		agg,
	)
	w.SetStateTTL(2 * time.Millisecond)

	batch := types.Batch{{Tuple: types.Tuple{"k": "a", "ts": int64(1000), "v": 1.0}, Count: 1}}
	if _, err := w.Apply(batch); err != nil {
		t.Fatalf("apply: %v", err)
	}
	if got := len(w.State.Data); got != 1 {
		t.Fatalf("expected 1 window, got %d", got)
	}

	time.Sleep(5 * time.Millisecond)
	if _, err := w.Apply(nil); err != nil {
		t.Fatalf("apply for eviction: %v", err)
	}
	if got := len(w.State.Data); got != 0 {
		t.Fatalf("expected windows to be evicted, got %d", got)
	}
}

func TestJoinStateTTL(t *testing.T) {
	join := NewJoinOp(
		func(t types.Tuple) any { return t["k"] },
		func(t types.Tuple) any { return t["k"] },
		func(l, r types.Tuple) types.Tuple { return types.Tuple{"k": l["k"]} },
	)
	join.SetStateTTL(2 * time.Millisecond)

	left := types.Batch{{Tuple: types.Tuple{"k": "a"}, Count: 1}}
	right := types.Batch{{Tuple: types.Tuple{"k": "a"}, Count: 1}}

	if _, err := join.ApplyBinary(left, nil); err != nil {
		t.Fatalf("apply left: %v", err)
	}
	out, err := join.ApplyBinary(nil, right)
	if err != nil {
		t.Fatalf("apply right: %v", err)
	}
	if len(out) != 1 {
		t.Fatalf("expected join output, got %d", len(out))
	}

	time.Sleep(5 * time.Millisecond)
	out, err = join.ApplyBinary(nil, nil)
	if err != nil {
		t.Fatalf("apply for eviction: %v", err)
	}
	if len(out) != 1 {
		t.Fatalf("expected eviction retraction, got %d", len(out))
	}
	if out[0].Count != -1 {
		t.Fatalf("expected eviction count -1, got %d", out[0].Count)
	}

	out, err = join.ApplyBinary(nil, right)
	if err != nil {
		t.Fatalf("apply right after eviction: %v", err)
	}
	if len(out) != 0 {
		t.Fatalf("expected no output after eviction, got %d", len(out))
	}
}
