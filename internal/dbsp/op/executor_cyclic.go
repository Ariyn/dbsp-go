package op

import (
	"fmt"

	"github.com/ariyn/dbsp/internal/dbsp/types"
)

// ExecuteTickCyclic evaluates an operator graph for a single logical tick.
//
// Unlike ExecuteTick, this evaluator allows cycles *only* when every cycle
// passes through at least one DelayOp. DelayOps act as 1-tick registers.
//
// Tick semantics (2-phase):
//  1) Read: Delay nodes output their prev (seed for the first tick)
//  2) Compute: evaluate the combinational DAG using prev values
//  3) Write: compute and store each Delay's next value from its input
//  4) Commit: prev = next
func ExecuteTickCyclic(root *Node, sources map[string]types.Batch) (types.Batch, error) {
	if root == nil {
		return nil, nil
	}
	if err := ValidateNoCombinationalCycles(root); err != nil {
		return nil, err
	}

	// Collect delay nodes reachable from root.
	delayNodes := make([]*Node, 0)
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
			delayNodes = append(delayNodes, n)
		}
		for _, in := range n.Inputs {
			stack = append(stack, in)
		}
	}

	memo := make(map[*Node]types.Batch)
	visiting := make(map[*Node]bool)

	// Phase 1: Read delay outputs (prev) into memo.
	for _, dn := range delayNodes {
		dop, ok := dn.Op.(*DelayOp)
		if !ok {
			continue
		}
		memo[dn] = dop.Prev()
	}

	var eval func(n *Node) (types.Batch, error)
	eval = func(n *Node) (types.Batch, error) {
		if n == nil {
			return nil, nil
		}
		if out, ok := memo[n]; ok {
			return out, nil
		}
		if visiting[n] {
			// Any remaining cycle here is a bug, because combinational cycles were validated out
			// and Delay nodes are memoized.
			return nil, fmt.Errorf("cycle detected during cyclic evaluation")
		}
		visiting[n] = true
		defer delete(visiting, n)

		if n.Source != "" {
			out := sources[n.Source]
			memo[n] = out
			return out, nil
		}
		if n.Op == nil {
			return nil, fmt.Errorf("non-source node has nil operator")
		}

		switch len(n.Inputs) {
		case 0:
			// For non-source leaf operators (rare), treat as Apply(nil).
			out, err := n.Op.Apply(nil)
			if err != nil {
				return nil, err
			}
			memo[n] = out
			return out, nil
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

	// Phase 2: Compute root output.
	out, err := eval(root)
	if err != nil {
		return nil, err
	}

	// Phase 3: Write next values into delay registers.
	for _, dn := range delayNodes {
		dop, ok := dn.Op.(*DelayOp)
		if !ok {
			continue
		}
		if len(dn.Inputs) != 1 {
			return nil, fmt.Errorf("DelayOp node must have exactly 1 input, got %d", len(dn.Inputs))
		}
		next, err := eval(dn.Inputs[0])
		if err != nil {
			return nil, err
		}
		dop.SetNext(next)
	}

	// Phase 4: Commit delay registers.
	for _, dn := range delayNodes {
		dop, ok := dn.Op.(*DelayOp)
		if !ok {
			continue
		}
		dop.Commit()
	}

	return out, nil
}
