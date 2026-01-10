package op

import (
	"testing"

	"github.com/ariyn/dbsp/internal/dbsp/types"
)

func countsByTuple(b types.Batch) map[string]int64 {
	m := make(map[string]int64)
	for _, td := range b {
		m[stableTupleKey(td.Tuple)] += td.Count
	}
	return m
}

func TestIntegrateOp_AccumulateSnapshot(t *testing.T) {
	op := NewIntegrateOp()

	out1, err := op.Apply(types.Batch{{Tuple: types.Tuple{"id": 1}, Count: 1}})
	if err != nil {
		t.Fatalf("tick1 apply failed: %v", err)
	}
	m1 := countsByTuple(out1)
	if m1[stableTupleKey(types.Tuple{"id": 1})] != 1 {
		t.Fatalf("expected snapshot to contain id=1, got %v", m1)
	}

	out2, err := op.Apply(types.Batch{{Tuple: types.Tuple{"id": 2}, Count: 1}})
	if err != nil {
		t.Fatalf("tick2 apply failed: %v", err)
	}
	m2 := countsByTuple(out2)
	if m2[stableTupleKey(types.Tuple{"id": 1})] != 1 || m2[stableTupleKey(types.Tuple{"id": 2})] != 1 {
		t.Fatalf("expected snapshot {1,2}, got %v", m2)
	}
}

func TestIntegrateOp_DeleteRetractsFromSnapshot(t *testing.T) {
	op := NewIntegrateOp()

	_, err := op.Apply(types.Batch{{Tuple: types.Tuple{"id": 1}, Count: 1}})
	if err != nil {
		t.Fatalf("insert failed: %v", err)
	}

	out, err := op.Apply(types.Batch{{Tuple: types.Tuple{"id": 1}, Count: -1}})
	if err != nil {
		t.Fatalf("delete failed: %v", err)
	}
	m := countsByTuple(out)
	if len(m) != 0 {
		t.Fatalf("expected empty snapshot after delete, got %v", m)
	}
}

func TestIntegrateOp_UnderflowError(t *testing.T) {
	op := NewIntegrateOp()

	_, err := op.Apply(types.Batch{{Tuple: types.Tuple{"id": 1}, Count: -1}})
	if err == nil {
		t.Fatalf("expected underflow error")
	}
}
