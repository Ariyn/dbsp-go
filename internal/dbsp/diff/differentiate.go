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
	return DifferentiateGraph(node)
}

// DifferentiateGraph recursively differentiates an entire operator graph.
// For now, this is a simple wrapper since we have single-operator nodes.
func DifferentiateGraph(root *op.Node) (*op.Node, error) {
	if root == nil {
		return nil, fmt.Errorf("cannot differentiate nil node")
	}

	memo := make(map[*op.Node]*op.Node)
	visiting := make(map[*op.Node]bool)

	var diffNode func(n *op.Node) (*op.Node, error)
	diffNode = func(n *op.Node) (*op.Node, error) {
		if n == nil {
			return nil, nil
		}
		if out, ok := memo[n]; ok {
			return out, nil
		}
		if visiting[n] {
			return nil, fmt.Errorf("cycle detected while differentiating graph")
		}
		visiting[n] = true
		defer delete(visiting, n)

		// Source leaf: d(source) = source (delta stream)
		if n.Source != "" {
			out := &op.Node{Source: n.Source}
			memo[n] = out
			return out, nil
		}
		if n.Op == nil {
			return nil, fmt.Errorf("cannot differentiate node with nil operator")
		}

		inputs := make([]*op.Node, 0, len(n.Inputs))
		for _, in := range n.Inputs {
			dIn, err := diffNode(in)
			if err != nil {
				return nil, err
			}
			inputs = append(inputs, dIn)
		}

		// Most operators in this codebase already operate on deltas.
		// Preserve operator identity (and its internal state) and only rebuild topology.
		out := &op.Node{Op: n.Op, Inputs: inputs}
		memo[n] = out
		return out, nil
	}

	return diffNode(root)
}

// differentiateBinary applies the product rule for binary operators.
// For binary operators: d(S ⊙ T) = (dS ⊙ T) + (S ⊙ dT) + (dS ⊙ dT)
//
// The BinaryOp implementation already handles this through its ApplyBinary method
// which maintains left and right state and computes all three terms.
// The differentiated operator is the operator itself, as it processes deltas.
func differentiateBinary(node *op.Node) (*op.Node, error) {
	binOp, ok := node.Op.(*op.BinaryOp)
	if !ok {
		return nil, fmt.Errorf("expected BinaryOp, got %T", node.Op)
	}

	// For binary operators in DBSP, the derivative processes input deltas
	// and the operator itself implements the product rule internally.
	// We return a new node with the same operator (which maintains state).
	return &op.Node{
		Op:     binOp,
		Inputs: node.Inputs,
	}, nil
}
