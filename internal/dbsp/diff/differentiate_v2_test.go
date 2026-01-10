package diff

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/ariyn/dbsp/internal/dbsp/op"
	"github.com/ariyn/dbsp/internal/dbsp/types"
)

func batchCounts(b types.Batch) map[string]int64 {
	m := make(map[string]int64)
	for _, td := range b {
		m[stableKey(td.Tuple)] += td.Count
	}
	return m
}

func stableKey(t types.Tuple) string {
	// Match op.stableTupleKey behavior: json.Marshal of map keys is deterministic.
	b, _ := json.Marshal(t)
	return string(b)
}

func TestDifferentiateV2_JoinExplicitMatchesBinaryOp(t *testing.T) {
	joinOp := op.NewJoinOp(
		func(tu types.Tuple) any { return tu["k"] },
		func(tu types.Tuple) any { return tu["k"] },
		func(l, r types.Tuple) types.Tuple { return types.Tuple{"k": l["k"], "l": l["v"], "r": r["v"]} },
	)

	root := &op.Node{Op: joinOp, Inputs: []*op.Node{{Source: "l"}, {Source: "r"}}}

	dRoot, err := DifferentiateGraphV2(root)
	if err != nil {
		t.Fatalf("DifferentiateGraphV2 failed: %v", err)
	}

	// tick1: left insert
	l1 := types.Batch{{Tuple: types.Tuple{"k": "a", "v": 1}, Count: 1}}
	outA1, err := op.ExecuteTick(root, map[string]types.Batch{"l": l1})
	if err != nil {
		t.Fatalf("baseline tick1 failed: %v", err)
	}
	outB1, err := op.ExecuteTick(dRoot, map[string]types.Batch{"l": l1})
	if err != nil {
		t.Fatalf("v2 tick1 failed: %v", err)
	}
	if len(outA1) != 0 || len(outB1) != 0 {
		t.Fatalf("expected no output on tick1, baseline=%v v2=%v", outA1, outB1)
	}

	// tick2: right insert -> should join with left state
	r2 := types.Batch{{Tuple: types.Tuple{"k": "a", "v": 10}, Count: 1}}
	outA2, err := op.ExecuteTick(root, map[string]types.Batch{"r": r2})
	if err != nil {
		t.Fatalf("baseline tick2 failed: %v", err)
	}
	outB2, err := op.ExecuteTick(dRoot, map[string]types.Batch{"r": r2})
	if err != nil {
		t.Fatalf("v2 tick2 failed: %v", err)
	}
	if gotA, gotB := batchCounts(outA2), batchCounts(outB2); !reflect.DeepEqual(gotA, gotB) {
		t.Fatalf("tick2 mismatch baseline=%v v2=%v", gotA, gotB)
	}

	// tick3: both sides insert same tick
	l3 := types.Batch{{Tuple: types.Tuple{"k": "a", "v": 2}, Count: 1}}
	r3 := types.Batch{{Tuple: types.Tuple{"k": "a", "v": 20}, Count: 1}}
	outA3, err := op.ExecuteTick(root, map[string]types.Batch{"l": l3, "r": r3})
	if err != nil {
		t.Fatalf("baseline tick3 failed: %v", err)
	}
	outB3, err := op.ExecuteTick(dRoot, map[string]types.Batch{"l": l3, "r": r3})
	if err != nil {
		t.Fatalf("v2 tick3 failed: %v", err)
	}
	if gotA, gotB := batchCounts(outA3), batchCounts(outB3); !reflect.DeepEqual(gotA, gotB) {
		t.Fatalf("tick3 mismatch baseline=%v v2=%v", gotA, gotB)
	}

	// tick4: delete a left tuple -> should retract join results against current right state
	l4 := types.Batch{{Tuple: types.Tuple{"k": "a", "v": 1}, Count: -1}}
	outA4, err := op.ExecuteTick(root, map[string]types.Batch{"l": l4})
	if err != nil {
		t.Fatalf("baseline tick4 failed: %v", err)
	}
	outB4, err := op.ExecuteTick(dRoot, map[string]types.Batch{"l": l4})
	if err != nil {
		t.Fatalf("v2 tick4 failed: %v", err)
	}
	if gotA, gotB := batchCounts(outA4), batchCounts(outB4); !reflect.DeepEqual(gotA, gotB) {
		t.Fatalf("tick4 mismatch baseline=%v v2=%v", gotA, gotB)
	}
}
