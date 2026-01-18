package op

import (
	"bytes"
	"encoding/gob"
	"fmt"
)

func init() {
	// SnapshotGraph encodes operator state behind an interface field (any).
	// gob requires concrete types used in interface values to be registered.
	gob.Register(groupAggSnapshotV1{})
	gob.Register(integrateSnapshotV1{})
	gob.Register(binarySnapshotV1{})
	gob.Register(delaySnapshotV1{})
	gob.Register(windowAggSnapshotV1{})
	gob.Register(watermarkAwareSnapshotV1{})
}

// StatefulOperator can persist and restore its internal mutable state.
//
// IMPORTANT: Snapshot/Restore must not attempt to encode function fields
// (closures, callbacks). Only encode deterministic, gob-friendly state.
//
// The operator graph (Node topology) is reconstructed from SQL/IR; snapshot
// restores the in-memory state to avoid replaying the entire WAL.
type StatefulOperator interface {
	Snapshot() (any, error)
	Restore(state any) error
}

type graphSnapshotV1 struct {
	// Nodes are stored in a deterministic post-order (inputs first).
	Nodes []nodeSnapshotV1
}

type nodeSnapshotV1 struct {
	OpType string
	Source string
	Arity  int
	// State is nil when operator is not stateful.
	State any
}

// SnapshotGraph serializes the mutable state of a DBSP operator graph.
//
// The snapshot is intended to be restored into a graph with the same topology
// and operator types (re-built from the same query/config).
func SnapshotGraph(root *Node) ([]byte, error) {
	gs := graphSnapshotV1{}
	order := postOrderNodes(root)
	gs.Nodes = make([]nodeSnapshotV1, 0, len(order))
	for _, n := range order {
		ns := nodeSnapshotV1{OpType: fmt.Sprintf("%T", n.Op), Source: n.Source, Arity: len(n.Inputs)}
		if n.Op == nil {
			ns.OpType = "<nil>"
		}
		if st, ok := n.Op.(StatefulOperator); ok {
			state, err := st.Snapshot()
			if err != nil {
				return nil, fmt.Errorf("snapshot %T: %w", n.Op, err)
			}
			ns.State = state
		}
		gs.Nodes = append(gs.Nodes, ns)
	}

	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(gs); err != nil {
		return nil, fmt.Errorf("encode graph snapshot: %w", err)
	}
	return buf.Bytes(), nil
}

// RestoreGraph restores a previously captured SnapshotGraph into the given graph.
func RestoreGraph(root *Node, snapshot []byte) error {
	if root == nil {
		return fmt.Errorf("root is nil")
	}
	var gs graphSnapshotV1
	dec := gob.NewDecoder(bytes.NewReader(snapshot))
	if err := dec.Decode(&gs); err != nil {
		return fmt.Errorf("decode graph snapshot: %w", err)
	}

	order := postOrderNodes(root)
	if len(order) != len(gs.Nodes) {
		return fmt.Errorf("snapshot node count mismatch: graph=%d snapshot=%d", len(order), len(gs.Nodes))
	}

	for i, n := range order {
		ns := gs.Nodes[i]
		opType := fmt.Sprintf("%T", n.Op)
		if n.Op == nil {
			opType = "<nil>"
		}
		if opType != ns.OpType {
			return fmt.Errorf("snapshot operator type mismatch at idx=%d: graph=%s snapshot=%s", i, opType, ns.OpType)
		}
		if n.Source != ns.Source {
			return fmt.Errorf("snapshot source mismatch at idx=%d: graph=%q snapshot=%q", i, n.Source, ns.Source)
		}
		if len(n.Inputs) != ns.Arity {
			return fmt.Errorf("snapshot arity mismatch at idx=%d: graph=%d snapshot=%d", i, len(n.Inputs), ns.Arity)
		}
		if ns.State == nil {
			continue
		}
		st, ok := n.Op.(StatefulOperator)
		if !ok {
			return fmt.Errorf("snapshot includes state for non-stateful op %T at idx=%d", n.Op, i)
		}
		if err := st.Restore(ns.State); err != nil {
			return fmt.Errorf("restore %T at idx=%d: %w", n.Op, i, err)
		}
	}
	return nil
}

func postOrderNodes(root *Node) []*Node {
	seen := make(map[*Node]bool)
	out := make([]*Node, 0)
	var walk func(n *Node)
	walk = func(n *Node) {
		if n == nil || seen[n] {
			return
		}
		seen[n] = true
		for _, in := range n.Inputs {
			walk(in)
		}
		out = append(out, n)
	}
	walk(root)
	return out
}
