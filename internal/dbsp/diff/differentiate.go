package diff

import (
	"fmt"

	"github.com/ariyn/dbsp/internal/dbsp/op"
)

// Differentiate transforms a DBSP operator graph Q into its derivative dQ.
//
// Differentiation rules (from differential-rule.instructions.md):
// - Map: d(map(f, S)) = map(f, dS)
// - Binary: d(S ⊙ T) = (dS ⊙ T) + (S ⊙ dT) + (dS ⊙ dT)
// - Delay: d(delay(S)) = dS
// - Integrate: d(integrate(S)) = S
//
// For Phase1, we focus on:
// - Map operators (including GroupAgg which acts like a stateful map)
// - The derivative of integrate is the identity (already handled by GroupAgg state)
func Differentiate(node *op.Node) (*op.Node, error) {
	if node == nil || node.Op == nil {
		return nil, fmt.Errorf("cannot differentiate nil node")
	}

	switch opType := node.Op.(type) {
	case *op.MapOp:
		// d(map(f)) = map(f) - Map is linear, derivative is the same function
		return &op.Node{
			Op:     opType,
			Inputs: node.Inputs,
		}, nil

	case *op.GroupAggOp:
		// GroupAgg with integrate semantics: maintains state
		// The differentiate of an aggregate with integrate is the input delta itself
		// (the operator already handles incremental updates internally)
		return &op.Node{
			Op:     opType,
			Inputs: node.Inputs,
		}, nil

	case *op.ChainedOp:
		// ChainedOp is a composition of operators applied sequentially
		// d(f ∘ g) = d(f) ∘ d(g) - differentiate each operator in the chain
		return &op.Node{
			Op:     opType,
			Inputs: node.Inputs,
		}, nil

	case *op.WindowAggOp:
		// WindowAggOp already handles incremental updates internally (window-local deltas)
		// The derivative is the operator itself - it processes input deltas and produces output deltas
		return &op.Node{
			Op:     opType,
			Inputs: node.Inputs,
		}, nil

	// TODO: Add Binary operator support for JOIN
	// case *op.BinaryOp:
	//     return differentiateBinary(node)

	default:
		return nil, fmt.Errorf("differentiation not implemented for operator type %T", opType)
	}
}

// DifferentiateGraph recursively differentiates an entire operator graph.
// For now, this is a simple wrapper since we have single-operator nodes.
func DifferentiateGraph(root *op.Node) (*op.Node, error) {
	return Differentiate(root)
}
