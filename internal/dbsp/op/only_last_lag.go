package op

// OnlyLastLagSetter allows operators to receive the only-last-lag policy.
type OnlyLastLagSetter interface {
	SetOnlyLastLag(enabled bool)
}

// ApplyOnlyLastLag walks the operator graph and enables the only-last-lag
// optimization for operators that support it.
func ApplyOnlyLastLag(root *Node, enabled bool) {
	if root == nil || !enabled {
		return
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
		if setter, ok := n.Op.(OnlyLastLagSetter); ok {
			setter.SetOnlyLastLag(true)
		}
		if chained, ok := n.Op.(*ChainedOp); ok {
			for _, inner := range chained.Ops {
				if setter, ok := inner.(OnlyLastLagSetter); ok {
					setter.SetOnlyLastLag(true)
				}
			}
		}
		for _, in := range n.Inputs {
			stack = append(stack, in)
		}
	}
}
