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

func TestCollectRequiredColumnsSelectStarOverCTE(t *testing.T) {
	cteBody := &LogicalProject{
		Columns: []string{"a"},
		Exprs:   []ProjectExpr{{ExprSQL: "b + c", As: "bc"}},
		Input:   &LogicalScan{Table: "t"},
	}
	root := &LogicalWith{
		CTENames: []string{"x"},
		CTEs:     map[string]LogicalNode{"x": cteBody},
		Body: &LogicalProject{
			Columns: []string{"*"},
			Input:   &LogicalCTERef{CTEName: "x"},
		},
	}

	cols := CollectRequiredInputColumns(root)
	if cols == nil {
		t.Fatalf("expected concrete source columns, got nil")
	}
	assertHasColumn(t, cols, "a")
	assertHasColumn(t, cols, "b")
	assertHasColumn(t, cols, "c")
}

func TestCollectRequiredColumnsKeepInputOverDerivedInput(t *testing.T) {
	root := &LogicalProject{
		KeepInput: true,
		Exprs:     []ProjectExpr{{ExprSQL: "a + b", As: "ab"}},
		Input: &LogicalProject{
			Columns: []string{"a", "b"},
			Input:   &LogicalScan{Table: "t"},
		},
	}

	cols := CollectRequiredInputColumns(root)
	if cols == nil {
		t.Fatalf("expected concrete source columns, got nil")
	}
	assertHasColumn(t, cols, "a")
	assertHasColumn(t, cols, "b")
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

func TestCollectRequiredColumnsIgnoresStringLiteralFragments(t *testing.T) {
	root := &LogicalFilter{
		PredicateSQL: "id = '0e02e183-c1b2-4492-9eda-26b08892e427.0.0' AND plant_id = 'plant-a'",
		Input:        &LogicalScan{Table: "t"},
	}

	cols := CollectRequiredInputColumns(root)
	if cols == nil {
		t.Fatalf("expected column set, got nil")
	}
	assertHasColumn(t, cols, "id")
	assertHasColumn(t, cols, "plant_id")
	assertMissingColumn(t, cols, "0")
	assertMissingColumn(t, cols, "0e02e183")
	assertMissingColumn(t, cols, "c1b2")
	assertMissingColumn(t, cols, "b08892e427.0.0")
	assertMissingColumn(t, cols, "plant")
}

func TestCollectRequiredColumnsResolvesProjectedAliasToInputs(t *testing.T) {
	root := &LogicalProject{
		Columns: []string{"bc"},
		Input: &LogicalProject{
			Exprs: []ProjectExpr{{ExprSQL: "b + c", As: "bc"}},
			Input: &LogicalScan{Table: "t"},
		},
	}

	cols := CollectRequiredInputColumns(root)
	if cols == nil {
		t.Fatalf("expected column set, got nil")
	}
	assertHasColumn(t, cols, "b")
	assertHasColumn(t, cols, "c")
	assertMissingColumn(t, cols, "bc")
}

func TestCollectRequiredColumnsResolvesFilterAliasChainToInputs(t *testing.T) {
	root := &LogicalFilter{
		PredicateSQL: "bc > 10",
		Input: &LogicalProject{
			Exprs: []ProjectExpr{{ExprSQL: "b + c", As: "bc"}},
			Input: &LogicalScan{Table: "t"},
		},
	}

	cols := CollectRequiredInputColumns(root)
	if cols == nil {
		t.Fatalf("expected column set, got nil")
	}
	assertHasColumn(t, cols, "b")
	assertHasColumn(t, cols, "c")
	assertMissingColumn(t, cols, "bc")
}

func TestCollectRequiredColumnsResolvesSortAliasChainToInputs(t *testing.T) {
	root := &LogicalSort{
		OrderColumns: []string{"bucket"},
		Input: &LogicalProject{
			Exprs: []ProjectExpr{{ExprSQL: "TIME_BUCKET(INTERVAL '5 min', timestamp::TIMESTAMP)", As: "bucket"}},
			Input: &LogicalScan{Table: "t"},
		},
	}

	cols := CollectRequiredInputColumns(root)
	if cols == nil {
		t.Fatalf("expected column set, got nil")
	}
	assertHasColumn(t, cols, "timestamp")
	assertMissingColumn(t, cols, "bucket")
}

func TestCollectRequiredColumnsResolvesAggregateAliasChainToInputs(t *testing.T) {
	root := &LogicalProject{
		Columns: []string{"energy"},
		Input: &LogicalGroupAgg{
			Keys: []string{"panel_position"},
			Aggs: []AggSpec{{Name: "SUM", Col: "p_out", As: "energy"}},
			Input: &LogicalProject{
				Columns: []string{"panel_position"},
				Exprs:   []ProjectExpr{{ExprSQL: "v_out * i_out", As: "p_out"}},
				Input:   &LogicalScan{Table: "t"},
			},
		},
	}

	cols := CollectRequiredInputColumns(root)
	if cols == nil {
		t.Fatalf("expected column set, got nil")
	}
	assertHasColumn(t, cols, "panel_position")
	assertHasColumn(t, cols, "v_out")
	assertHasColumn(t, cols, "i_out")
	assertMissingColumn(t, cols, "energy")
	assertMissingColumn(t, cols, "p_out")
}

func assertHasColumn(t *testing.T, cols map[string]struct{}, key string) {
	t.Helper()
	if _, ok := cols[key]; !ok {
		t.Fatalf("expected column %s", key)
	}
}

func assertMissingColumn(t *testing.T, cols map[string]struct{}, key string) {
	t.Helper()
	if _, ok := cols[key]; ok {
		t.Fatalf("did not expect column %s in %v", key, cols)
	}
}
