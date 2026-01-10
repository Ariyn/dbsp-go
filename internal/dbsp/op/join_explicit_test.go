package op

import (
	"testing"

	"github.com/ariyn/dbsp/internal/dbsp/types"
)

func TestJoinDeltaValue_ValueCountsMultiply(t *testing.T) {
	op := NewJoinDeltaValueOp(
		func(t types.Tuple) any { return t["k"] },
		func(t types.Tuple) any { return t["k"] },
		func(l, r types.Tuple) types.Tuple {
			return types.Tuple{"k": l["k"], "lv": l["v"], "rv": r["v"]}
		},
	)

	leftDelta := types.Batch{{Tuple: types.Tuple{"k": "a", "v": 1}, Count: 2}}
	rightValue := types.Batch{{Tuple: types.Tuple{"k": "a", "v": 10}, Count: 3}}

	out, err := op.Apply2(leftDelta, rightValue)
	if err != nil {
		t.Fatalf("Apply2 failed: %v", err)
	}
	if len(out) != 1 {
		t.Fatalf("expected 1 output, got %d", len(out))
	}
	if out[0].Count != 6 {
		t.Fatalf("expected count 6, got %d", out[0].Count)
	}
}
