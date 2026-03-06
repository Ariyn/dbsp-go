package graph
package graph

import (
	"strings"
	"testing"

	"github.com/ariyn/dbsp/internal/dbsp/ir"



































}	}		t.Fatalf("expected source label in DOT, got: %s", dot)	if !strings.Contains(dot, "Source: events") {	}		t.Fatalf("expected MapOp in DOT, got: %s", dot)	if !strings.Contains(dot, "MapOp") {	dot := OperatorGraphDOT(root, Options{Verbose: true})	root := &op.Node{Op: mapOp, Inputs: []*op.Node{src}}	mapOp := &op.MapOp{F: func(types.TupleDelta) []types.TupleDelta { return nil }}	src := &op.Node{Source: "events"}func TestOperatorGraphDOTIncludesOperatorTypes(t *testing.T) {}	}		t.Fatalf("expected table name in DOT, got: %s", dot)	if !strings.Contains(dot, "events") {	}		t.Fatalf("expected LogicalScan in DOT, got: %s", dot)	if !strings.Contains(dot, "LogicalScan") {	}		t.Fatalf("expected LogicalFilter in DOT, got: %s", dot)	if !strings.Contains(dot, "LogicalFilter") {	dot := LogicalPlanDOT(plan, Options{Verbose: true})	}		Input: &ir.LogicalScan{Table: "events"},		PredicateSQL: "x = 1",	plan := &ir.LogicalFilter{func TestLogicalPlanDOTIncludesNodeNames(t *testing.T) {)	"github.com/ariyn/dbsp/internal/dbsp/types"	"github.com/ariyn/dbsp/internal/dbsp/op"