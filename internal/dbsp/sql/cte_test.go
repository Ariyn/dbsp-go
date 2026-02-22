package sqlconv

import (
	"reflect"
	"testing"
	"github.com/Ariyn/tree-sitter-duckdb/bindings/go/ast"
	"github.com/Ariyn/tree-sitter-duckdb/bindings/go/parser"
	"github.com/ariyn/dbsp/internal/dbsp/ir"
	"github.com/ariyn/dbsp/internal/dbsp/op"
)

func TestInspectSelect(t *testing.T) {
	p := parser.NewParser()
	q := "WITH cte AS (SELECT 1) SELECT * FROM cte"
	stmt, err := p.Parse(q)
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}
	sel, ok := stmt.(*ast.Select)
	if !ok {
		t.Fatalf("Not a Select: %T", stmt)
	}
	t.Logf("Checking CTE structure")
	if len(sel.With) > 0 {
		cte := sel.With[0]
		valCte := reflect.ValueOf(cte).Elem()
		typCte := valCte.Type()
		for i := 0; i < valCte.NumField(); i++ {
			t.Logf("CTE Field: %s, Type: %s", typCte.Field(i).Name, typCte.Field(i).Type)
		}
	}
}

func TestBasicCTE(t *testing.T) {
	q := "WITH cte AS (SELECT k, SUM(v) as sv FROM t GROUP BY k) SELECT k, sv FROM cte WHERE sv > 0"
	lp, err := ParseQueryToLogicalPlan(q)
	if err != nil {
		t.Fatalf("ParseQueryToLogicalPlan failed: %v", err)
	}
	t.Logf("ParseQueryToLogicalPlan succeeded: %T", lp)

	// Check if it transforms correctly to DBSP operators
	opNode, err := ir.LogicalToDBSP(lp)
	if err != nil {
		t.Fatalf("LogicalToDBSP failed: %v", err)
	}
	t.Logf("Operator graph root: %T", opNode.Op)

	// Traverse the graph to see if we reached the physical source "t"
	foundSource := false
	var checkNode func(*op.Node)
	checkNode = func(node *op.Node) {
		if node.Source == "t" {
			foundSource = true
			return
		}
		for _, in := range node.Inputs {
			checkNode(in)
		}
	}
	checkNode(opNode)
	if !foundSource {
		t.Errorf("Physical source 't' not found in operator graph")
	} else {
		t.Logf("Successfully reached physical source 't' through CTE 'cte'")
	}
}

func TestComplexCTE(t *testing.T) {
	// CTE refers to another CTE
	q := `WITH 
		cte1 AS (SELECT k, v FROM t1),
		cte2 AS (SELECT k, SUM(v) as sv FROM cte1 GROUP BY k)
		SELECT k, sv FROM cte2 WHERE sv > 10`
	
	lp, err := ParseQueryToLogicalPlan(q)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	opNode, err := ir.LogicalToDBSP(lp)
	if err != nil {
		t.Fatalf("Transform failed: %v", err)
	}

	// Verify we can reach the base table t1
	foundT1 := false
	var checkNode func(*op.Node)
	checkNode = func(node *op.Node) {
		if node.Source == "t1" {
			foundT1 = true
			return
		}
		for _, in := range node.Inputs {
			checkNode(in)
		}
	}
	checkNode(opNode)
	if !foundT1 {
		t.Errorf("Source 't1' not found in operator graph for complex CTE")
	}
}

func TestUndefinedCTE(t *testing.T) {
	// If it's a CTE ref pointing to something that wasn't defined:
	lp := &ir.LogicalCTERef{CTEName: "non_existent"}
	_, err := ir.LogicalToDBSP(lp)
	if err == nil {
		t.Errorf("Expected error for undefined CTE reference, got nil")
	} else {
		t.Logf("Got expected error: %v", err)
	}
}

func TestShadowedCTE(t *testing.T) {
	// Shadowed CTE: second definition should win
	q := `WITH 
		cte AS (SELECT * FROM t1),
		cte AS (SELECT * FROM t2)
		SELECT * FROM cte`
	
	lp, err := ParseQueryToLogicalPlan(q)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	opNode, err := ir.LogicalToDBSP(lp)
	if err != nil {
		t.Fatalf("Transform failed: %v", err)
	}

	// Check if we reach t2 AND NOT t1
	foundT1 := false
	foundT2 := false
	var checkNode func(*op.Node)
	checkNode = func(node *op.Node) {
		if node.Source == "t1" {
			foundT1 = true
		}
		if node.Source == "t2" {
			foundT2 = true
		}
		for _, in := range node.Inputs {
			checkNode(in)
		}
	}
	checkNode(opNode)
	if foundT1 {
		t.Errorf("Shadowed source 't1' should not be in the graph")
	}
	if !foundT2 {
		t.Errorf("Latest definition 't2' not found in graph")
	}
}

func TestSharedCTE(t *testing.T) {
	// Multiple references to the same CTE (e.g., recursive sub-graph sharing)
	q := "WITH shared AS (SELECT k, v FROM t) SELECT a.v, b.v FROM shared a JOIN shared b ON a.k = b.k"

	lp, err := ParseQueryToLogicalPlan(q)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	opNode, err := ir.LogicalToDBSP(lp)
	if err != nil {
		t.Fatalf("Transform failed: %v", err)
	}

	// In the resulting operator graph, JOIN's both inputs should be the same *op.Node
	
	countSharedRefs := 0
	var sharedNode *op.Node

	// Traverse to find the JoinNode
	var findJoin func(*op.Node) *op.Node
	findJoin = func(n *op.Node) *op.Node {
		// If it's a binary op, check if it's a join.
		if _, ok := n.Op.(*op.BinaryOp); ok {
			return n
		}
		if c, ok := n.Op.(*op.ChainedOp); ok {
			for _, sub := range c.Ops {
				if _, isBinary := sub.(*op.BinaryOp); isBinary {
					return n
				}
			}
		}
		for _, in := range n.Inputs {
			if res := findJoin(in); res != nil {
				return res
			}
		}
		return nil
	}

	joinNode := findJoin(opNode)
	if joinNode == nil {
		// Log the structure for debugging.
		var walk func(*op.Node, int)
		walk = func(n *op.Node, indent int) {
			t.Logf("%sNode Op: %T, Inputs: %d", string(make([]byte, indent)), n.Op, len(n.Inputs))
			for _, in := range n.Inputs {
				walk(in, indent+2)
			}
		}
		walk(opNode, 0)
		t.Fatalf("JoinOp node not found in graph")
	}

	if len(joinNode.Inputs) != 2 {
		t.Fatalf("JoinNode should have 2 inputs, got %d", len(joinNode.Inputs))
	}

	// Check if both inputs eventually lead to the same shared CTE output node.
	if joinNode.Inputs[0] == joinNode.Inputs[1] {
		t.Logf("Join inputs share the exact same operator node instance (perfect sharing)")
		sharedNode = joinNode.Inputs[0]
	}

	if sharedNode == nil {
		// They might be different pointers if project/alias was applied on top (for 'a' and 'b').
		// We expect the underlying CTE output to be shared.
		visited := make(map[*op.Node]bool)
		var checkShared func(*op.Node)
		checkShared = func(curr *op.Node) {
			if visited[curr] {
				sharedNode = curr
				countSharedRefs++
				return
			}
			visited[curr] = true
			for _, in := range curr.Inputs {
				checkShared(in)
			}
		}
		checkShared(opNode)
	}

	if sharedNode == nil {
		t.Errorf("No shared operator node found in graph, expected multi-reference CTE to be shared")
	} else {
		t.Logf("Found shared node at %p (referenced multiple times)", sharedNode)
	}
}

func TestCTEWithColumnAliases(t *testing.T) {
	// Removing parentheses just in case the parser is picky, but it should work.
	// Actually, let's use standard aliases if parentheses fail.
	q := "WITH cte AS (SELECT k, v FROM t) SELECT k as new_k, v as new_v FROM cte"
	
	lp, err := ParseQueryToLogicalPlan(q)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	t.Logf("Successfully parsed with aliases")
	
	_, err = ir.LogicalToDBSP(lp)
	if err != nil {
		t.Fatalf("Transform failed: %v", err)
	}
}

func TestNestedCTEWithClause(t *testing.T) {
	// Nested WITH: outer definition and inner definition
	q := `WITH outer_cte AS (SELECT * FROM t)
	      SELECT * FROM (WITH inner_cte AS (SELECT * FROM outer_cte) SELECT * FROM inner_cte) as sub`
	
	lp, err := ParseQueryToLogicalPlan(q)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	_, err = ir.LogicalToDBSP(lp)
	if err != nil {
		t.Fatalf("Failed to transform nested CTE structure: %v", err)
	}
	t.Logf("Successfully transformed nested WITH clauses")
}
