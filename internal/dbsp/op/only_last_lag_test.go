package op

import (
	"testing"

	"github.com/ariyn/dbsp/internal/dbsp/types"
)

func TestApplyOnlyLastLagPropagatesIntoChainedOp(t *testing.T) {
	ordered := NewOrderedWindowOp(func(t types.Tuple) any { return t["k"] }, "ts", "v", 1, "v_last")
	root := &Node{Op: &ChainedOp{Ops: []Operator{&MapOp{F: func(td types.TupleDelta) []types.TupleDelta { return []types.TupleDelta{td} }}, ordered}}}

	ApplyOnlyLastLag(root, true)
	if !ordered.OnlyLastLag {
		t.Fatalf("expected chained OrderedWindowOp to receive only_last_lag")
	}
}
