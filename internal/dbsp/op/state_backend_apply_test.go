package op

import (
	"testing"

	"github.com/ariyn/dbsp/internal/dbsp/types"
)

func TestApplyStateBackendAttachesOrderedWindowOp(t *testing.T) {
	backend := NewMemoryStateBackend()
	ordered := NewOrderedWindowOp(nil, "ts", "v", 1, "v_last")
	root := &Node{Op: ordered}

	ApplyStateBackend(root, backend, "testgraph")

	if ordered.stateBackend != backend {
		t.Fatal("expected OrderedWindowOp to receive state backend")
	}
	if ordered.statePrefix != "testgraph/node-000" {
		t.Fatalf("unexpected OrderedWindowOp prefix: %q", ordered.statePrefix)
	}
}

func TestApplyStateBackendAttachesChainedInnerOperators(t *testing.T) {
	backend := NewMemoryStateBackend()
	ordered := NewOrderedWindowOp(nil, "ts", "v", 1, "v_last")
	chain := &ChainedOp{Ops: []Operator{&MapOp{F: func(td types.TupleDelta) []types.TupleDelta { return []types.TupleDelta{td} }}, ordered}}
	root := &Node{Op: chain}

	ApplyStateBackend(root, backend, "testgraph")

	if ordered.stateBackend != backend {
		t.Fatal("expected chained OrderedWindowOp to receive state backend")
	}
	if ordered.statePrefix != "testgraph/node-000/chain-01" {
		t.Fatalf("unexpected chained OrderedWindowOp prefix: %q", ordered.statePrefix)
	}
}