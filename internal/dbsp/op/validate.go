package op

import "fmt"

// ValidateNoCombinationalCycles enforces the rule:
// any cycle in the operator graph must pass through at least one DelayOp.
//
// Implementation strategy:
// - We traverse edges from a node to its Inputs (dependency direction).
// - A DelayOp is treated as a cut: we do NOT traverse into its inputs.
// - Any remaining cycle is therefore a combinational (delay-free) cycle.
func ValidateNoCombinationalCycles(root *Node) error {
	if root == nil {
		return nil
	}

	visited := make(map[*Node]bool)
	visiting := make(map[*Node]bool)

	var dfs func(n *Node) error
	dfs = func(n *Node) error {
		if n == nil {
			return nil
		}
		if visited[n] {
			return nil
		}
		if visiting[n] {
			return fmt.Errorf("combinational cycle detected (cycle without DelayOp)")
		}
		visiting[n] = true
		defer delete(visiting, n)

		// DelayOp cuts combinational dependencies.
		if _, ok := n.Op.(*DelayOp); ok {
			visited[n] = true
			return nil
		}

		for _, in := range n.Inputs {
			if err := dfs(in); err != nil {
				return err
			}
		}

		visited[n] = true
		return nil
	}

	return dfs(root)
}
