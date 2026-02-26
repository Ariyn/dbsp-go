package op

import "fmt"

// AttachWindowAggStateBackend wires an optional state backend to all WindowAgg operators in graph.
func AttachWindowAggStateBackend(root *Node, backend StateBackend) int {
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
		if w, ok := n.Op.(*WindowAggOp); ok {
			attached++
			w.SetStateBackend(backend, fmt.Sprintf("windowagg/op-%d", attached))
		}
		for _, in := range n.Inputs {
			walk(in)
		}
	}

	walk(root)
	return attached
}
