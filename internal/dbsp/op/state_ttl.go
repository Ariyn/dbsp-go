package op

import "time"

// StateTTLSetter allows operators to receive a global state TTL policy.
type StateTTLSetter interface {
	SetStateTTL(ttl time.Duration)
}

// ApplyStateTTL walks the operator graph and applies a global state TTL.
func ApplyStateTTL(root *Node, ttl time.Duration) {
	if root == nil || ttl <= 0 {
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
		if setter, ok := n.Op.(StateTTLSetter); ok {
			setter.SetStateTTL(ttl)
		}
		for _, in := range n.Inputs {
			stack = append(stack, in)
		}
	}
}
