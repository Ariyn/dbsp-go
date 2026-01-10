package op

import (
	"testing"

	"github.com/ariyn/dbsp/internal/dbsp/types"
)

func batchToKeyCounts(b types.Batch) map[string]int64 {
	m := make(map[string]int64)
	for _, td := range b {
		m[stableTupleKey(td.Tuple)] += td.Count
	}
	return m
}

func TestDelayOp_OneTickShift(t *testing.T) {
	seed := types.Batch{{Tuple: types.Tuple{"id": 0}, Count: 1}}
	delay := NewDelayOp(seed)

	root := &Node{Op: delay, Inputs: []*Node{{Source: "t"}}}

	in1 := types.Batch{{Tuple: types.Tuple{"id": 1}, Count: 1}}
	out1, err := ExecuteTickCyclic(root, map[string]types.Batch{"t": in1})
	if err != nil {
		t.Fatalf("ExecuteTickCyclic tick1 failed: %v", err)
	}
	if got := batchToKeyCounts(out1); got[stableTupleKey(types.Tuple{"id": 0})] != 1 {
		t.Fatalf("expected seed on tick1, got %v", got)
	}

	in2 := types.Batch{{Tuple: types.Tuple{"id": 2}, Count: 1}}
	out2, err := ExecuteTickCyclic(root, map[string]types.Batch{"t": in2})
	if err != nil {
		t.Fatalf("ExecuteTickCyclic tick2 failed: %v", err)
	}
	if got := batchToKeyCounts(out2); got[stableTupleKey(types.Tuple{"id": 1})] != 1 {
		t.Fatalf("expected tick1 input on tick2, got %v", got)
	}
}

func TestExecuteTickCyclic_CycleWithDelayAllowed(t *testing.T) {
	// x[t] = in[t] + x[t-1]
	// Implemented as: x = Union(in, Delay(x))
	in := &Node{Source: "in"}
	delay := &Node{Op: NewDelayOp(nil)}
	union := &Node{Op: NewUnionOp(), Inputs: []*Node{in, delay}}
	delay.Inputs = []*Node{union}

	// tick1: in={a} => out={a}
	a := types.Batch{{Tuple: types.Tuple{"v": "a"}, Count: 1}}
	out1, err := ExecuteTickCyclic(union, map[string]types.Batch{"in": a})
	if err != nil {
		t.Fatalf("tick1 failed: %v", err)
	}
	m1 := batchToKeyCounts(out1)
	if m1[stableTupleKey(types.Tuple{"v": "a"})] != 1 {
		t.Fatalf("expected {a} on tick1, got %v", m1)
	}

	// tick2: in={b} => out={b}+{a}
	b := types.Batch{{Tuple: types.Tuple{"v": "b"}, Count: 1}}
	out2, err := ExecuteTickCyclic(union, map[string]types.Batch{"in": b})
	if err != nil {
		t.Fatalf("tick2 failed: %v", err)
	}
	m2 := batchToKeyCounts(out2)
	if m2[stableTupleKey(types.Tuple{"v": "a"})] != 1 || m2[stableTupleKey(types.Tuple{"v": "b"})] != 1 {
		t.Fatalf("expected {a,b} on tick2, got %v", m2)
	}
}

func TestExecuteTickCyclic_CombinationalCycleRejected(t *testing.T) {
	id := &MapOp{F: func(td types.TupleDelta) []types.TupleDelta { return []types.TupleDelta{td} }}
	a := &Node{Op: id}
	b := &Node{Op: id, Inputs: []*Node{a}}
	a.Inputs = []*Node{b} // cycle without DelayOp

	_, err := ExecuteTickCyclic(a, map[string]types.Batch{})
	if err == nil {
		t.Fatalf("expected combinational cycle error")
	}
}
