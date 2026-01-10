package op

import (
	"testing"

	"github.com/ariyn/dbsp/internal/dbsp/types"
)

func TestExecuteTick_JoinGraph(t *testing.T) {
	joinOp := NewJoinOp(
		func(tup types.Tuple) any { return tup["id"] },
		func(tup types.Tuple) any { return tup["id"] },
		func(l, r types.Tuple) types.Tuple {
			out := make(types.Tuple)
			for k, v := range l {
				out["l_"+k] = v
			}
			for k, v := range r {
				out["r_"+k] = v
			}
			return out
		},
	)

	root := &Node{
		Op: joinOp,
		Inputs: []*Node{
			{Source: "a"},
			{Source: "b"},
		},
	}

	aBatch := types.Batch{{Tuple: types.Tuple{"id": 1, "k": "A"}, Count: 1}}
	bBatch := types.Batch{{Tuple: types.Tuple{"id": 1, "v": 10}, Count: 1}}

	out1, err := ExecuteTick(root, map[string]types.Batch{"a": aBatch})
	if err != nil {
		t.Fatalf("ExecuteTick(a) failed: %v", err)
	}
	if len(out1) != 0 {
		t.Fatalf("expected no join output after only left input, got %d", len(out1))
	}

	out2, err := ExecuteTick(root, map[string]types.Batch{"b": bBatch})
	if err != nil {
		t.Fatalf("ExecuteTick(b) failed: %v", err)
	}
	if len(out2) != 1 {
		t.Fatalf("expected 1 join output, got %d", len(out2))
	}
	if out2[0].Tuple["l_id"] != 1 || out2[0].Tuple["r_id"] != 1 {
		t.Fatalf("unexpected joined tuple: %+v", out2[0].Tuple)
	}
}

func TestExecute_SingleSourceGraphAutoMapping(t *testing.T) {
	id := &MapOp{F: func(td types.TupleDelta) []types.TupleDelta { return []types.TupleDelta{td} }}
	root := &Node{Op: id, Inputs: []*Node{{Source: "t"}}}

	in := types.Batch{{Tuple: types.Tuple{"id": 1}, Count: 1}}
	out, err := Execute(root, in)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}
	if len(out) != 1 {
		t.Fatalf("expected 1 output, got %d", len(out))
	}
	if out[0].Tuple["id"] != 1 {
		t.Fatalf("unexpected output tuple: %+v", out[0].Tuple)
	}
}

func TestExecuteTick_CycleDetected(t *testing.T) {
	id := &MapOp{F: func(td types.TupleDelta) []types.TupleDelta { return []types.TupleDelta{td} }}
	n := &Node{Op: id}
	n.Inputs = []*Node{n}

	_, err := ExecuteTick(n, map[string]types.Batch{"t": {}})
	if err == nil {
		t.Fatalf("expected cycle detection error")
	}
}
