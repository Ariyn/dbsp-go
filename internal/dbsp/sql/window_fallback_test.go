package sqlconv

import "testing"

import "github.com/ariyn/dbsp/internal/dbsp/ir"

func TestFindAllWindowFunctionsFromQuery_LAG(t *testing.T) {
	q := "SELECT LAG(a) OVER (PARTITION BY id ORDER BY ts) AS prev_a FROM t"
	funcs, err := findAllWindowFunctionsFromQuery(q)
	if err != nil {
		t.Fatalf("findAllWindowFunctionsFromQuery failed: %v", err)
	}
	if len(funcs) != 1 {
		t.Fatalf("expected 1 window func, got %d", len(funcs))
	}
	wf := funcs[0]
	if wf.Spec.FuncName != "LAG" {
		t.Fatalf("expected LAG, got %s", wf.Spec.FuncName)
	}
	if wf.Spec.OrderBy != "ts" {
		t.Fatalf("expected orderBy=ts, got %s", wf.Spec.OrderBy)
	}
	if wf.OutputCol != "prev_a" {
		t.Fatalf("expected output prev_a, got %s", wf.OutputCol)
	}
}

func TestFindAllWindowAggregatesFromQuery_SUMOver(t *testing.T) {
	q := "SELECT SUM(energy) OVER (PARTITION BY id ORDER BY binned_date) AS cumulative_energy FROM x"
	aggs, err := findAllWindowAggregatesFromQuery(q)
	if err != nil {
		t.Fatalf("findAllWindowAggregatesFromQuery failed: %v", err)
	}
	if len(aggs) != 1 {
		t.Fatalf("expected 1 window aggregate, got %d", len(aggs))
	}
	wa := aggs[0]
	if wa.AggName != "SUM" {
		t.Fatalf("expected SUM, got %s", wa.AggName)
	}
	if wa.AggCol != "energy" {
		t.Fatalf("expected agg col energy, got %s", wa.AggCol)
	}
	if wa.OutputCol != "cumulative_energy" {
		t.Fatalf("expected output cumulative_energy, got %s", wa.OutputCol)
	}
}

func TestParseQueryToLogicalPlan_LAGIncludesWindowNode(t *testing.T) {
	q := "SELECT LAG(a) OVER (PARTITION BY id ORDER BY ts) AS prev_a FROM t"
	lp, err := ParseQueryToLogicalPlan(q)
	if err != nil {
		t.Fatalf("ParseQueryToLogicalPlan failed: %v", err)
	}
	if !containsWindowFuncNode(lp) {
		t.Fatalf("expected logical plan to include LogicalWindowFunc, got %T", lp)
	}
	wf := findWindowFuncNode(lp)
	if wf == nil {
		t.Fatalf("expected window func node in plan")
	}
	if wf.OutputCol != "prev_a" {
		t.Fatalf("expected LAG output alias prev_a, got %q", wf.OutputCol)
	}
	if wf.Spec.FuncName != "LAG" {
		t.Fatalf("expected func LAG, got %q", wf.Spec.FuncName)
	}
	if len(wf.Spec.Args) != 1 || wf.Spec.Args[0] != "a" {
		t.Fatalf("expected args [a], got %+v", wf.Spec.Args)
	}
	if wf.Spec.OrderBy != "ts" {
		t.Fatalf("expected orderBy ts, got %q", wf.Spec.OrderBy)
	}
	if len(wf.Spec.PartitionBy) != 1 || wf.Spec.PartitionBy[0] != "id" {
		t.Fatalf("expected partitionBy [id], got %+v", wf.Spec.PartitionBy)
	}
}

func containsWindowFuncNode(n ir.LogicalNode) bool {
	switch t := n.(type) {
	case *ir.LogicalWindowFunc:
		return true
	case *ir.LogicalProject:
		return containsWindowFuncNode(t.Input)
	case *ir.LogicalFilter:
		return containsWindowFuncNode(t.Input)
	case *ir.LogicalGroupAgg:
		return containsWindowFuncNode(t.Input)
	case *ir.LogicalWindowAgg:
		return containsWindowFuncNode(t.Input)
	case *ir.LogicalSort:
		return containsWindowFuncNode(t.Input)
	case *ir.LogicalWith:
		if containsWindowFuncNode(t.Body) {
			return true
		}
		for _, name := range t.CTENames {
			if containsWindowFuncNode(t.CTEs[name]) {
				return true
			}
		}
	}
	return false
}

func findWindowFuncNode(n ir.LogicalNode) *ir.LogicalWindowFunc {
	switch t := n.(type) {
	case *ir.LogicalWindowFunc:
		return t
	case *ir.LogicalProject:
		return findWindowFuncNode(t.Input)
	case *ir.LogicalFilter:
		return findWindowFuncNode(t.Input)
	case *ir.LogicalGroupAgg:
		return findWindowFuncNode(t.Input)
	case *ir.LogicalWindowAgg:
		return findWindowFuncNode(t.Input)
	case *ir.LogicalSort:
		return findWindowFuncNode(t.Input)
	case *ir.LogicalWith:
		if got := findWindowFuncNode(t.Body); got != nil {
			return got
		}
		for _, name := range t.CTENames {
			if got := findWindowFuncNode(t.CTEs[name]); got != nil {
				return got
			}
		}
	}
	return nil
}
