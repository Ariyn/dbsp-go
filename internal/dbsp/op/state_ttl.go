package op

import "time"

// StateTTLSetter allows operators to receive a global state TTL policy.
type StateTTLSetter interface {
	SetStateTTL(ttl time.Duration)
}

// WatermarkGCSetter allows operators to receive a watermark-based GC toggle.
type WatermarkGCSetter interface {
	SetWatermarkGC(enabled bool)
}

type EventTimeWatermarkConfig struct {
	MaxOutOfOrderness time.Duration
	AllowedLateness   time.Duration
	Policy            string
}

// WatermarkConfigSetter allows operators to receive event-time watermark config.
type WatermarkConfigSetter interface {
	SetEventTimeWatermark(cfg EventTimeWatermarkConfig)
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
		if chained, ok := n.Op.(*ChainedOp); ok {
			for _, inner := range chained.Ops {
				if setter, ok := inner.(StateTTLSetter); ok {
					setter.SetStateTTL(ttl)
				}
			}
		}
		for _, in := range n.Inputs {
			stack = append(stack, in)
		}
	}
}

// ApplyEventTimeWatermark walks the operator graph and applies event-time watermark
// configuration to all operators that support it.
func ApplyEventTimeWatermark(root *Node, cfg EventTimeWatermarkConfig) {
	if root == nil {
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
		if setter, ok := n.Op.(WatermarkConfigSetter); ok {
			setter.SetEventTimeWatermark(cfg)
		}
		if chained, ok := n.Op.(*ChainedOp); ok {
			for _, inner := range chained.Ops {
				if setter, ok := inner.(WatermarkConfigSetter); ok {
					setter.SetEventTimeWatermark(cfg)
				}
			}
		}
		for _, in := range n.Inputs {
			stack = append(stack, in)
		}
	}
}

// ApplyWatermarkGC walks the operator graph and enables watermark-based GC
// on all operators that support it.
func ApplyWatermarkGC(root *Node, enabled bool) {
	if root == nil {
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
		if setter, ok := n.Op.(WatermarkGCSetter); ok {
			setter.SetWatermarkGC(enabled)
		}
		if chained, ok := n.Op.(*ChainedOp); ok {
			for _, inner := range chained.Ops {
				if setter, ok := inner.(WatermarkGCSetter); ok {
					setter.SetWatermarkGC(enabled)
				}
			}
		}
		for _, in := range n.Inputs {
			stack = append(stack, in)
		}
	}
}
