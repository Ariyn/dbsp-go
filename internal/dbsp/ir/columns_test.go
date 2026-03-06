package ir

import "testing"

func TestCollectRequiredColumnsProjectExpr(t *testing.T) {
	root := &LogicalProject{
		Columns: []string{"a"},
		Exprs:   []ProjectExpr{{ExprSQL: "b + c", As: "x"}},
		Input:   &LogicalScan{Table: "t"},
	}

	cols := CollectRequiredInputColumns(root)
	if cols == nil {
		t.Fatalf("expected column set, got nil")
	}
	assertHasColumn(t, cols, "a")
	assertHasColumn(t, cols, "b")
	assertHasColumn(t, cols, "c")
}

func TestCollectRequiredColumnsKeepInput(t *testing.T) {
	root := &LogicalProject{
		KeepInput: true,
		Input:     &LogicalScan{Table: "t"},
	}

	cols := CollectRequiredInputColumns(root)
	if cols != nil {
		t.Fatalf("expected nil to keep all columns")
	}
}

func TestCollectRequiredColumnsPredicate(t *testing.T) {
	root := &LogicalFilter{
		PredicateSQL: "a > 1 AND b = 'x'",
		Input:        &LogicalScan{Table: "t"},
	}

	cols := CollectRequiredInputColumns(root)
	if cols == nil {
		t.Fatalf("expected column set, got nil")
	}
	assertHasColumn(t, cols, "a")
	assertHasColumn(t, cols, "b")
}

func TestCollectRequiredColumnsEmptyPlan(t *testing.T) {
	root := &LogicalScan{Table: "t"}
	cols := CollectRequiredInputColumns(root)
	if cols != nil {
		t.Fatalf("expected nil to keep all columns")
	}
}

func TestCollectRequiredColumnsWithCTE(t *testing.T) {
	cteBody := &LogicalProject{
		Columns: []string{"a"},
		Exprs:   []ProjectExpr{{ExprSQL: "b + c", As: "bc"}},
		Input:   &LogicalScan{Table: "t"},
	}
	root := &LogicalWith{
		CTENames: []string{"x"},
		CTEs:     map[string]LogicalNode{"x": cteBody},
		Body: &LogicalProject{
			Columns: []string{"a", "bc"},
			Input:   &LogicalCTERef{CTEName: "x"},
		},
	}

	cols := CollectRequiredInputColumns(root)
	if cols == nil {
		t.Fatalf("expected column set, got nil")
	}
	assertHasColumn(t, cols, "a")
	assertHasColumn(t, cols, "b")
	assertHasColumn(t, cols, "c")
}

func assertHasColumn(t *testing.T, cols map[string]struct{}, key string) {
	t.Helper()
	if _, ok := cols[key]; !ok {
		t.Fatalf("expected column %s", key)
	}
}
