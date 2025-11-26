package op

import "github.com/ariyn/dbsp/internal/dbsp/types"

// Operator processes a Batch and returns an output Batch (delta semantics).
type Operator interface {
	Apply(batch types.Batch) (types.Batch, error)
}

// Node is a node in the operator graph.
type Node struct {
	Op     Operator
	Inputs []*Node
}

// Execute runs the operator at root with the given delta batch and returns its output.
func Execute(root *Node, delta types.Batch) (types.Batch, error) {
	if root == nil || root.Op == nil {
		return nil, nil
	}
	return root.Op.Apply(delta)
}

// ChainedOp applies multiple operators in sequence.
type ChainedOp struct {
	Ops []Operator
}

func (c *ChainedOp) Apply(batch types.Batch) (types.Batch, error) {
	current := batch
	var err error
	for _, op := range c.Ops {
		current, err = op.Apply(current)
		if err != nil {
			return nil, err
		}
	}
	return current, nil
}
