package main

import (
	"fmt"
	"strings"
	"time"

	"github.com/ariyn/dbsp/internal/dbsp/op"
	"github.com/ariyn/dbsp/internal/dbsp/types"
)

func parseWatermarkPolicy(s string) (op.LateEventPolicy, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "", "drop":
		return op.DropLateEvents, nil
	case "buffer":
		return op.BufferLateEvents, nil
	case "emit":
		return op.EmitLateEvents, nil
	default:
		return 0, fmt.Errorf("unknown watermark policy: %s", s)
	}
}

func buildWatermarkConfig(cfg WatermarkYAMLConfig) (op.WatermarkConfig, error) {
	wm := op.DefaultWatermarkConfig()
	wm.Enabled = cfg.Enabled
	if !cfg.Enabled {
		return wm, nil
	}

	if cfg.MaxOutOfOrderness != "" {
		if d, err := time.ParseDuration(cfg.MaxOutOfOrderness); err == nil {
			wm.MaxOutOfOrderness = int64(d / time.Millisecond)
		} else {
			iv, err := types.ParseInterval(cfg.MaxOutOfOrderness)
			if err != nil {
				return op.WatermarkConfig{}, err
			}
			wm.MaxOutOfOrderness = iv.Millis
		}
	}
	if cfg.AllowedLateness != "" {
		if d, err := time.ParseDuration(cfg.AllowedLateness); err == nil {
			wm.AllowedLateness = int64(d / time.Millisecond)
		} else {
			iv, err := types.ParseInterval(cfg.AllowedLateness)
			if err != nil {
				return op.WatermarkConfig{}, err
			}
			wm.AllowedLateness = iv.Millis
		}
	}

	policy, err := parseWatermarkPolicy(cfg.Policy)
	if err != nil {
		return op.WatermarkConfig{}, err
	}
	wm.Policy = policy
	if cfg.MaxBufferSize > 0 {
		wm.MaxBufferSize = cfg.MaxBufferSize
	}
	return wm, nil
}

// applyWatermarkConfig wraps time-window WindowAggOp nodes with WatermarkAwareWindowOp.
// It is best-effort: only wraps time-based windows (FrameSpec==nil).
func applyWatermarkConfig(root *op.Node, cfg op.WatermarkConfig) {
	if root == nil || root.Op == nil {
		return
	}
	seen := make(map[*op.Node]struct{})
	var walk func(n *op.Node)
	walk = func(n *op.Node) {
		if n == nil {
			return
		}
		if _, ok := seen[n]; ok {
			return
		}
		seen[n] = struct{}{}
		for _, in := range n.Inputs {
			walk(in)
		}

		wa, ok := n.Op.(*op.WindowAggOp)
		if ok {
			if wa == nil {
				return
			}
			// Only time-based windowing, not frame-based windows.
			if wa.FrameSpec != nil {
				return
			}
			if strings.TrimSpace(wa.Spec.TimeCol) == "" {
				return
			}
			n.Op = op.NewWatermarkAwareWindowOp(wa.Spec, wa.KeyFn, wa.GroupKeys, wa.AggInit, wa.AggFn, cfg)
			return
		}

		// Best-effort: if WindowAggOp is inside a ChainedOp, wrap it in-place.
		if chained, ok := n.Op.(*op.ChainedOp); ok && chained != nil {
			for i, inner := range chained.Ops {
				innerWA, ok := inner.(*op.WindowAggOp)
				if !ok || innerWA == nil {
					continue
				}
				if innerWA.FrameSpec != nil {
					continue
				}
				if strings.TrimSpace(innerWA.Spec.TimeCol) == "" {
					continue
				}
				chained.Ops[i] = op.NewWatermarkAwareWindowOp(innerWA.Spec, innerWA.KeyFn, innerWA.GroupKeys, innerWA.AggInit, innerWA.AggFn, cfg)
			}
		}
	}
	walk(root)
}
