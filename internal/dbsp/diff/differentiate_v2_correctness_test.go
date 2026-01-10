package diff

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/ariyn/dbsp/internal/dbsp/op"
	"github.com/ariyn/dbsp/internal/dbsp/types"
)

type joinStep struct {
	name  string
	left  types.Batch
	right types.Batch
}

func stableTupleKeyTest(t types.Tuple) string {
	b, _ := json.Marshal(t)
	return string(b)
}

func batchAsMultiset(b types.Batch) map[string]int64 {
	m := make(map[string]int64)
	for _, td := range b {
		m[stableTupleKeyTest(td.Tuple)] += td.Count
	}
	// Remove zeros (defensive)
	for k, v := range m {
		if v == 0 {
			delete(m, k)
		}
	}
	return m
}

func runJoinSequenceCompareBaselineVsV2(t *testing.T, steps []joinStep) {
	t.Helper()

	makeJoin := func() *op.BinaryOp {
		return op.NewJoinOp(
			func(tu types.Tuple) any { return tu["k"] },
			func(tu types.Tuple) any { return tu["k"] },
			func(l, r types.Tuple) types.Tuple {
				return types.Tuple{"k": l["k"], "l": l["v"], "r": r["v"]}
			},
		)
	}

	baselineRoot := &op.Node{Op: makeJoin(), Inputs: []*op.Node{{Source: "l"}, {Source: "r"}}}
	v2Root, err := DifferentiateGraphV2(&op.Node{Op: makeJoin(), Inputs: []*op.Node{{Source: "l"}, {Source: "r"}}})
	if err != nil {
		t.Fatalf("DifferentiateGraphV2 failed: %v", err)
	}

	for i, st := range steps {
		inputs := make(map[string]types.Batch)
		if st.left != nil {
			inputs["l"] = st.left
		}
		if st.right != nil {
			inputs["r"] = st.right
		}

		outA, err := op.ExecuteTick(baselineRoot, inputs)
		if err != nil {
			t.Fatalf("baseline step %d (%s) failed: %v", i+1, st.name, err)
		}
		outB, err := op.ExecuteTick(v2Root, inputs)
		if err != nil {
			t.Fatalf("v2 step %d (%s) failed: %v", i+1, st.name, err)
		}

		mA := batchAsMultiset(outA)
		mB := batchAsMultiset(outB)
		if !reflect.DeepEqual(mA, mB) {
			t.Fatalf("step %d (%s) mismatch\nbaseline=%v\nv2=%v\nrawA=%v\nrawB=%v", i+1, st.name, mA, mB, outA, outB)
		}
	}
}

func TestDifferentiateV2_Join_DeleteRetractAndMultiplicity(t *testing.T) {
	steps := []joinStep{
		{
			name:  "tick1 left x2",
			left:  types.Batch{{Tuple: types.Tuple{"k": "a", "v": 1}, Count: 2}},
			right: nil,
		},
		{
			name:  "tick2 right x3 (joins with left state)",
			left:  nil,
			right: types.Batch{{Tuple: types.Tuple{"k": "a", "v": 10}, Count: 3}},
		},
		{
			name:  "tick3 delete left x1 (retract joins)",
			left:  types.Batch{{Tuple: types.Tuple{"k": "a", "v": 1}, Count: -1}},
			right: nil,
		},
		{
			name:  "tick4 both sides delta same tick",
			left:  types.Batch{{Tuple: types.Tuple{"k": "a", "v": 2}, Count: 1}},
			right: types.Batch{{Tuple: types.Tuple{"k": "a", "v": 20}, Count: 1}},
		},
		{
			name:  "tick5 delete right x1",
			left:  nil,
			right: types.Batch{{Tuple: types.Tuple{"k": "a", "v": 10}, Count: -1}},
		},
	}

	runJoinSequenceCompareBaselineVsV2(t, steps)
}

func TestDifferentiateV2_Join_NullKeysIgnored(t *testing.T) {
	steps := []joinStep{
		{
			name:  "tick1 left NULL key ignored",
			left:  types.Batch{{Tuple: types.Tuple{"k": nil, "v": 1}, Count: 1}},
			right: nil,
		},
		{
			name:  "tick2 right NULL key ignored",
			left:  nil,
			right: types.Batch{{Tuple: types.Tuple{"k": nil, "v": 10}, Count: 1}},
		},
		{
			name:  "tick3 normal join works",
			left:  types.Batch{{Tuple: types.Tuple{"k": "a", "v": 1}, Count: 1}},
			right: types.Batch{{Tuple: types.Tuple{"k": "a", "v": 10}, Count: 1}},
		},
	}

	runJoinSequenceCompareBaselineVsV2(t, steps)
}

func TestDifferentiateV2_UnionAndDifference_Linear(t *testing.T) {
	// Union graph
	unionRoot := &op.Node{Op: op.NewUnionOp(), Inputs: []*op.Node{{Source: "a"}, {Source: "b"}}}
	dUnion, err := DifferentiateGraphV2(unionRoot)
	if err != nil {
		t.Fatalf("DifferentiateGraphV2(union) failed: %v", err)
	}

	a := types.Batch{{Tuple: types.Tuple{"id": 1}, Count: 1}}
	b := types.Batch{{Tuple: types.Tuple{"id": 2}, Count: 1}}
	outA, err := op.ExecuteTick(unionRoot, map[string]types.Batch{"a": a, "b": b})
	if err != nil {
		t.Fatalf("union baseline failed: %v", err)
	}
	outB, err := op.ExecuteTick(dUnion, map[string]types.Batch{"a": a, "b": b})
	if err != nil {
		t.Fatalf("union v2 failed: %v", err)
	}
	if !reflect.DeepEqual(batchAsMultiset(outA), batchAsMultiset(outB)) {
		t.Fatalf("union mismatch baseline=%v v2=%v", outA, outB)
	}

	// Difference graph
	diffRoot := &op.Node{Op: op.NewDifferenceOp(), Inputs: []*op.Node{{Source: "a"}, {Source: "b"}}}
	dDiff, err := DifferentiateGraphV2(diffRoot)
	if err != nil {
		t.Fatalf("DifferentiateGraphV2(diff) failed: %v", err)
	}

	outA2, err := op.ExecuteTick(diffRoot, map[string]types.Batch{"a": a, "b": b})
	if err != nil {
		t.Fatalf("diff baseline failed: %v", err)
	}
	outB2, err := op.ExecuteTick(dDiff, map[string]types.Batch{"a": a, "b": b})
	if err != nil {
		t.Fatalf("diff v2 failed: %v", err)
	}
	if !reflect.DeepEqual(batchAsMultiset(outA2), batchAsMultiset(outB2)) {
		t.Fatalf("diff mismatch baseline=%v v2=%v", outA2, outB2)
	}
}
