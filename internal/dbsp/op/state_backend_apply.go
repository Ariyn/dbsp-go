package op

import "fmt"

// StateBackendSetter allows operators to receive a state backend.
type StateBackendSetter interface {
	SetStateBackend(backend StateBackend, prefix string)
}

// JoinStateBackendSetter allows join operators to receive a backend for join state.
type JoinStateBackendSetter interface {
	SetJoinStateBackend(backend StateBackend, prefix string)
}

// ApplyStateBackend walks the operator graph and attaches a shared state backend
// to stateful operators using deterministic per-operator prefixes.
func ApplyStateBackend(root *Node, backend StateBackend, prefix string) {
	if root == nil || backend == nil {
		return
	}
	basePrefix := prefix
	if basePrefix == "" {
		basePrefix = "graph"
	}

	nodes := postOrderNodes(root)
	for idx, node := range nodes {
		nodePrefix := fmt.Sprintf("%s/node-%03d", basePrefix, idx)
		applyBackendToOperator(node.Op, backend, nodePrefix)
	}
}

func applyBackendToOperator(operator Operator, backend StateBackend, prefix string) {
	if operator == nil || backend == nil {
		return
	}
	if setter, ok := operator.(StateBackendSetter); ok {
		setter.SetStateBackend(backend, prefix)
	}
	if setter, ok := operator.(JoinStateBackendSetter); ok {
		setter.SetJoinStateBackend(backend, prefix+"/join")
	}
	if chained, ok := operator.(*ChainedOp); ok {
		for idx, inner := range chained.Ops {
			applyBackendToOperator(inner, backend, fmt.Sprintf("%s/chain-%02d", prefix, idx))
		}
	}
}