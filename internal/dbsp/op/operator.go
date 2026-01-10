package op

import (
	"fmt"
	"sort"

	"github.com/ariyn/dbsp/internal/dbsp/types"
)

// Operator processes a Batch and returns an output Batch (delta semantics).
type Operator interface {
	Apply(batch types.Batch) (types.Batch, error)
}

// BinaryOperator processes two input batches and returns an output batch.
// This is used for true 2-input DAG nodes (e.g., Join/Union/Diff).
type BinaryOperator interface {
	Apply2(left, right types.Batch) (types.Batch, error)
}

// Node is a node in the operator graph.
type Node struct {
	Op     Operator
	Inputs []*Node

	// Source identifies a leaf input in the operator graph.
	// When non-empty, this node reads its batch from the per-tick input map.
	Source string
}

// Execute runs the operator at root with the given delta batch and returns its output.
func Execute(root *Node, delta types.Batch) (types.Batch, error) {
	if root == nil {
		return nil, nil
	}

	sources := SourceNames(root)
	if len(sources) == 0 {
		if root.Op == nil {
			return nil, nil
		}
		return root.Op.Apply(delta)
	}
	if len(sources) == 1 {
		return ExecuteTick(root, map[string]types.Batch{sources[0]: delta})
	}

	return nil, fmt.Errorf("graph has %d sources (%v); use ExecuteTick", len(sources), sources)
}

// ExecuteTick evaluates the operator DAG rooted at root for a single logical time tick.
// Input batches are provided by source name.
func ExecuteTick(root *Node, sources map[string]types.Batch) (types.Batch, error) {
	if root == nil {
		return nil, nil
	}

	// If the graph contains DelayOp, we must evaluate with register semantics.
	// ExecuteTickCyclic supports both acyclic graphs and Delay-separated cycles.
	if graphHasDelay(root) {
		return ExecuteTickCyclic(root, sources)
	}

	memo := make(map[*Node]types.Batch)
	visiting := make(map[*Node]bool)

	var eval func(n *Node) (types.Batch, error)
	eval = func(n *Node) (types.Batch, error) {
		if n == nil {
			return nil, nil
		}
		if out, ok := memo[n]; ok {
			return out, nil
		}
		if visiting[n] {
			return nil, fmt.Errorf("cycle detected in operator graph")
		}
		visiting[n] = true
		defer delete(visiting, n)

		// Source leaf
		if n.Source != "" {
			out := sources[n.Source]
			memo[n] = out
			return out, nil
		}

		if n.Op == nil {
			return nil, fmt.Errorf("non-source node has nil operator")
		}

		switch len(n.Inputs) {
		case 1:
			in, err := eval(n.Inputs[0])
			if err != nil {
				return nil, err
			}
			out, err := n.Op.Apply(in)
			if err != nil {
				return nil, err
			}
			memo[n] = out
			return out, nil

		case 2:
			left, err := eval(n.Inputs[0])
			if err != nil {
				return nil, err
			}
			right, err := eval(n.Inputs[1])
			if err != nil {
				return nil, err
			}
			bin, ok := n.Op.(BinaryOperator)
			if !ok {
				return nil, fmt.Errorf("operator %T does not support 2-input evaluation", n.Op)
			}
			out, err := bin.Apply2(left, right)
			if err != nil {
				return nil, err
			}
			memo[n] = out
			return out, nil

		default:
			return nil, fmt.Errorf("unsupported input arity %d", len(n.Inputs))
		}
	}

	return eval(root)
}

func graphHasDelay(root *Node) bool {
	if root == nil {
		return false
	}
	seen := make(map[*Node]bool)
	stack := []*Node{root}
	for len(stack) > 0 {
		n := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if n == nil || seen[n] {
			continue
		}
		seen[n] = true
		if _, ok := n.Op.(*DelayOp); ok {
			return true
		}
		for _, in := range n.Inputs {
			stack = append(stack, in)
		}
	}
	return false
}

// SourceNames returns the sorted unique source names reachable from root.
func SourceNames(root *Node) []string {
	if root == nil {
		return nil
	}
	seenNodes := make(map[*Node]bool)
	seenSources := make(map[string]bool)

	var walk func(n *Node)
	walk = func(n *Node) {
		if n == nil {
			return
		}
		if seenNodes[n] {
			return
		}
		seenNodes[n] = true
		if n.Source != "" {
			seenSources[n.Source] = true
			return
		}
		for _, in := range n.Inputs {
			walk(in)
		}
	}

	walk(root)

	out := make([]string, 0, len(seenSources))
	for s := range seenSources {
		out = append(out, s)
	}
	sort.Strings(out)
	return out
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
