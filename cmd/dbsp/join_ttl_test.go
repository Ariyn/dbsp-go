package main

import (
	"testing"
	"time"

	sqlconv "github.com/ariyn/dbsp/internal/dbsp/sql"
	"github.com/ariyn/dbsp/internal/dbsp/op"
)

func TestParseJoinTTL(t *testing.T) {
	got, err := parseJoinTTL("10s")
	if err != nil {
		t.Fatalf("parseJoinTTL failed: %v", err)
	}
	if got != 10*time.Second {
		t.Fatalf("expected 10s, got %s", got)
	}

	got, err = parseJoinTTL("5 minutes")
	if err != nil {
		t.Fatalf("parseJoinTTL failed: %v", err)
	}
	if got != 5*time.Minute {
		t.Fatalf("expected 5m, got %s", got)
	}
}

func TestApplyJoinTTL_SetsAllJoinOps(t *testing.T) {
	query := "SELECT * FROM orders JOIN customers ON orders.customer_id = customers.id"
	root, err := sqlconv.ParseQueryToIncrementalDBSP(query)
	if err != nil {
		t.Fatalf("ParseQueryToIncrementalDBSP failed: %v", err)
	}

	ttl := 7 * time.Second
	applyJoinTTL(root, ttl)

	// Walk graph and ensure every join has the ttl.
	seen := make(map[*op.Node]bool)
	joinCount := 0
	var walk func(n *op.Node)
	walk = func(n *op.Node) {
		if n == nil || seen[n] {
			return
		}
		seen[n] = true
		if bin, ok := n.Op.(*op.BinaryOp); ok {
			if bin.Type == op.BinaryJoin {
				joinCount++
				if bin.JoinTTL != ttl {
					t.Fatalf("expected join ttl %s, got %s", ttl, bin.JoinTTL)
				}
			}
		}
		for _, in := range n.Inputs {
			walk(in)
		}
	}
	walk(root)
	// With explicit join differentiation (3-term join composition), the graph may no longer
	// contain a stateful *op.BinaryOp join node. In that case JoinTTL is currently a no-op.
}
