package main

import (
	"fmt"
	"time"

	"github.com/ariyn/dbsp/internal/dbsp/op"
	"github.com/ariyn/dbsp/internal/dbsp/types"
)

func parseJoinTTL(s string) (time.Duration, error) {
	if s == "" {
		return 0, nil
	}
	if d, err := time.ParseDuration(s); err == nil {
		return d, nil
	}
	iv, err := types.ParseInterval(s)
	if err != nil {
		return 0, fmt.Errorf("invalid join_ttl %q: %w", s, err)
	}
	return time.Duration(iv.Millis) * time.Millisecond, nil
}

func applyJoinTTL(root *op.Node, ttl time.Duration) {
	if root == nil || ttl <= 0 {
		return
	}

	seen := make(map[*op.Node]bool)
	var walk func(n *op.Node)
	walk = func(n *op.Node) {
		if n == nil || seen[n] {
			return
		}
		seen[n] = true

		if bin, ok := n.Op.(*op.BinaryOp); ok {
			if bin.Type == op.BinaryJoin {
				bin.JoinTTL = ttl
			}
		}

		for _, in := range n.Inputs {
			walk(in)
		}
	}

	walk(root)
}
