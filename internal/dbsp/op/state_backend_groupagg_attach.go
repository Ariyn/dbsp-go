package op

import "fmt"

// AttachGroupAggStateBackend wires an optional state backend to all GroupAgg operators in graph.
func AttachGroupAggStateBackend(root *Node, backend StateBackend) int {
	if root == nil || backend == nil {
		return 0
	}
	seen := make(map[*Node]bool)
	attached := 0

	var walk func(n *Node)
	walk = func(n *Node) {
		if n == nil || seen[n] {
			return
		}
		seen[n] = true
		if g, ok := n.Op.(*GroupAggOp); ok {
			attached++
			g.SetStateBackend(backend, fmt.Sprintf("groupagg/op-%d", attached))
		}
		for _, in := range n.Inputs {
			walk(in)
		}
	}

	walk(root)
	return attached
}
