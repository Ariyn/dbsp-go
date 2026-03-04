package op

import "fmt"

// walkGraph visits every node in the operator graph exactly once, calling fn on each.
func walkGraph(root *Node, fn func(n *Node)) {
	if root == nil {
		return
	}
	seen := make(map[*Node]bool)
	var walk func(n *Node)
	walk = func(n *Node) {
		if n == nil || seen[n] {
			return
		}
		seen[n] = true
		fn(n)
		for _, in := range n.Inputs {
			walk(in)
		}
	}
	walk(root)
}

// AttachJoinStateBackend wires an optional state backend to all join operators in graph.
func AttachJoinStateBackend(root *Node, backend StateBackend) int {
	if backend == nil {
		return 0
	}
	attached := 0
	walkGraph(root, func(n *Node) {
		if bin, ok := n.Op.(*BinaryOp); ok && bin.Type == BinaryJoin {
			attached++
			bin.SetJoinStateBackend(backend, fmt.Sprintf("join/op-%d", attached))
		}
		if j, ok := n.Op.(*JoinOp); ok && j.BinaryOp != nil {
			attached++
			j.BinaryOp.SetJoinStateBackend(backend, fmt.Sprintf("join/op-%d", attached))
		}
	})
	return attached
}

// AttachGroupAggStateBackend wires an optional state backend to all GroupAgg operators in graph.
func AttachGroupAggStateBackend(root *Node, backend StateBackend) int {
	if backend == nil {
		return 0
	}
	attached := 0
	walkGraph(root, func(n *Node) {
		if g, ok := n.Op.(*GroupAggOp); ok {
			attached++
			g.SetStateBackend(backend, fmt.Sprintf("groupagg/op-%d", attached))
		}
	})
	return attached
}

// AttachWindowAggStateBackend wires an optional state backend to all WindowAgg operators in graph.
func AttachWindowAggStateBackend(root *Node, backend StateBackend) int {
	if backend == nil {
		return 0
	}
	attached := 0
	walkGraph(root, func(n *Node) {
		if w, ok := n.Op.(*WindowAggOp); ok {
			attached++
			w.SetStateBackend(backend, fmt.Sprintf("windowagg/op-%d", attached))
		}
	})
	return attached
}
