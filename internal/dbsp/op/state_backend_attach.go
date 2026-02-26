package op

import "fmt"

// AttachJoinStateBackend wires an optional state backend to all join operators in graph.
//
// The same backend instance can be shared across operators; each operator receives
// a unique prefix to avoid key collisions.
func AttachJoinStateBackend(root *Node, backend StateBackend) int {
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
		if bin, ok := n.Op.(*BinaryOp); ok && bin.Type == BinaryJoin {
			attached++
			bin.SetJoinStateBackend(backend, fmt.Sprintf("join/op-%d", attached))
		}
		if j, ok := n.Op.(*JoinOp); ok && j.BinaryOp != nil {
			attached++
			j.BinaryOp.SetJoinStateBackend(backend, fmt.Sprintf("join/op-%d", attached))
		}
		for _, in := range n.Inputs {
			walk(in)
		}
	}

	walk(root)
	return attached
}
