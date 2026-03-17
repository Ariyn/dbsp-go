package op

import (
	"testing"

	"github.com/ariyn/dbsp/internal/dbsp/types"
)

func TestResetGraphStateClearsWindowAggState(t *testing.T) {
	backend := NewMemoryStateBackend()
	w := NewWindowAggOp(
		WindowSpecLite{TimeCol: "ts", SizeMillis: 1000, WindowType: WindowTypeTumbling},
		func(t types.Tuple) any { return t["k"] },
		[]string{"k"},
		func() any { return float64(0) },
		&SumAgg{ColName: "v"},
	)
	w.SetStateBackend(backend, "window/test")
	root := &Node{Op: &ChainedOp{Ops: []Operator{w}}}

	if _, err := w.Apply(types.Batch{{Tuple: types.Tuple{"k": "a", "ts": int64(1500), "v": 1.0}, Count: 1}}); err != nil {
		t.Fatalf("apply: %v", err)
	}
	if got := len(w.State.Data); got != 1 {
		t.Fatalf("expected 1 retained window, got %d", got)
	}
	if w.observedMaxEventTime == 0 {
		t.Fatal("expected observed watermark progress to be tracked")
	}

	if err := backend.Reset(); err != nil {
		t.Fatalf("backend reset: %v", err)
	}
	if err := ResetGraphState(root); err != nil {
		t.Fatalf("graph reset: %v", err)
	}
	if got := len(w.State.Data); got != 0 {
		t.Fatalf("expected 0 retained windows after reset, got %d", got)
	}
	if w.observedMaxEventTime != 0 {
		t.Fatalf("expected observed watermark to reset, got %d", w.observedMaxEventTime)
	}
	if _, err := w.Apply(nil); err != nil {
		t.Fatalf("apply after reset: %v", err)
	}
	if got := len(w.State.Data); got != 0 {
		t.Fatalf("expected reset state to stay empty after reload, got %d", got)
	}
}

func TestResetGraphStateClearsOrderedWindowState(t *testing.T) {
	backend := NewMemoryStateBackend()
	w := NewOrderedWindowOp(func(t types.Tuple) any { return t["id"] }, "ts", "v", 1, "v_last")
	w.SetStateBackend(backend, "ordered/test")
	root := &Node{Op: &ChainedOp{Ops: []Operator{w}}}

	if _, err := w.Apply(types.Batch{{Tuple: types.Tuple{"id": "a", "ts": int64(1), "v": 10.0}, Count: 1}}); err != nil {
		t.Fatalf("apply: %v", err)
	}
	if got := len(w.Partitions); got != 1 {
		t.Fatalf("expected 1 retained partition, got %d", got)
	}

	if err := backend.Reset(); err != nil {
		t.Fatalf("backend reset: %v", err)
	}
	if err := ResetGraphState(root); err != nil {
		t.Fatalf("graph reset: %v", err)
	}
	if got := len(w.Partitions); got != 0 {
		t.Fatalf("expected 0 partitions after reset, got %d", got)
	}
	if _, err := w.Apply(nil); err != nil {
		t.Fatalf("apply after reset: %v", err)
	}
	if got := len(w.Partitions); got != 0 {
		t.Fatalf("expected reset partitions to stay empty after reload, got %d", got)
	}
}
