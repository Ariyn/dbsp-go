package ir

import (
	"regexp"
	"strings"
)

var identRegex = regexp.MustCompile(`[A-Za-z_][A-Za-z0-9_.]*`)

var keywordSet = map[string]struct{}{
	"AND":         {},
	"OR":          {},
	"NOT":         {},
	"IS":          {},
	"IN":          {},
	"LIKE":        {},
	"NULL":        {},
	"TRUE":        {},
	"FALSE":       {},
	"INTERVAL":    {},
	"CAST":        {},
	"AS":          {},
	"OVER":        {},
	"PARTITION":   {},
	"BY":          {},
	"ORDER":       {},
	"CASE":        {},
	"WHEN":        {},
	"THEN":        {},
	"ELSE":        {},
	"END":         {},
	"DISTINCT":    {},
	"SUM":         {},
	"AVG":         {},
	"COUNT":       {},
	"MIN":         {},
	"MAX":         {},
	"ROUND":       {},
	"STRFTIME":    {},
	"TIME_BUCKET": {},
	"EPOCH":       {},
}

// CollectRequiredInputColumns returns the set of input columns required by the logical plan.
// If it returns nil, the caller should keep all input fields.
func CollectRequiredInputColumns(root LogicalNode) map[string]struct{} {
	if root == nil {
		return nil
	}
	cols := make(map[string]struct{})
	if collectColumns(root, cols) {
		return nil
	}
	if len(cols) == 0 {
		return nil
	}
	return cols
}

func collectColumns(node LogicalNode, cols map[string]struct{}) bool {
	switch n := node.(type) {
	case *LogicalScan:
		return false
	case *LogicalFilter:
		addPredicateColumns(n.PredicateSQL, cols)
		return collectColumns(n.Input, cols)
	case *LogicalProject:
		if n.KeepInput || hasStar(n.Columns) {
			return true
		}
		for _, col := range n.Columns {
			addColumnRef(col, cols)
		}
		for _, expr := range n.Exprs {
			addExprColumns(expr.ExprSQL, cols)
		}
		return collectColumns(n.Input, cols)
	case *LogicalGroupAgg:
		for _, key := range n.Keys {
			addColumnRef(key, cols)
		}
		if n.AggCol != "" {
			addExprColumns(n.AggCol, cols)
		}
		for _, agg := range n.Aggs {
			if agg.Col != "" {
				addExprColumns(agg.Col, cols)
			}
		}
		if n.WindowSpec != nil {
			addColumnRef(n.WindowSpec.TimeCol, cols)
		}
		if n.TimeWindowSpec != nil {
			addColumnRef(n.TimeWindowSpec.TimeCol, cols)
		}
		return collectColumns(n.Input, cols)
	case *LogicalWindowFunc:
		for _, key := range n.Spec.PartitionBy {
			addColumnRef(key, cols)
		}
		addExprColumns(n.Spec.OrderBy, cols)
		for _, arg := range n.Spec.Args {
			addExprColumns(arg, cols)
		}
		return collectColumns(n.Input, cols)
	case *LogicalWindowAgg:
		for _, key := range n.PartitionBy {
			addColumnRef(key, cols)
		}
		addExprColumns(n.OrderBy, cols)
		if n.AggCol != "" {
			addExprColumns(n.AggCol, cols)
		}
		if n.TimeWindowSpec != nil {
			addColumnRef(n.TimeWindowSpec.TimeCol, cols)
		}
		return collectColumns(n.Input, cols)
	case *LogicalSort:
		for _, col := range n.OrderColumns {
			addExprColumns(col, cols)
		}
		return collectColumns(n.Input, cols)
	case *LogicalLimit:
		return collectColumns(n.Input, cols)
	case *LogicalJoin:
		for _, cond := range n.Conditions {
			addColumnRef(cond.LeftCol, cols)
			addColumnRef(cond.RightCol, cols)
		}
		if collectColumns(n.Left, cols) {
			return true
		}
		return collectColumns(n.Right, cols)
	case *LogicalView:
		for _, key := range n.PartitionBy {
			addColumnRef(key, cols)
		}
		return collectColumns(n.Input, cols)
	case *LogicalWith:
		if collectColumns(n.Body, cols) {
			return true
		}
		for _, cte := range n.CTEs {
			if collectColumns(cte, cols) {
				return true
			}
		}
		return false
	case *LogicalCTERef:
		return false
	default:
		return false
	}
}

func hasStar(cols []string) bool {
	for _, c := range cols {
		if strings.TrimSpace(c) == "*" {
			return true
		}
	}
	return false
}

func addColumnRef(col string, cols map[string]struct{}) {
	col = strings.TrimSpace(strings.Trim(col, "`\"'"))
	if col == "" || col == "*" {
		return
	}
	addColumn(col, cols)
	if idx := strings.LastIndex(col, "."); idx > 0 && idx < len(col)-1 {
		addColumn(col[idx+1:], cols)
	}
}

func addColumn(col string, cols map[string]struct{}) {
	if col == "" {
		return
	}
	cols[col] = struct{}{}
}

func addPredicateColumns(predicateSQL string, cols map[string]struct{}) {
	for _, id := range identRegex.FindAllString(predicateSQL, -1) {
		if isKeyword(id) {
			continue
		}
		addColumnRef(id, cols)
	}
}

func addExprColumns(exprSQL string, cols map[string]struct{}) {
	exprSQL = strings.TrimSpace(exprSQL)
	if exprSQL == "" {
		return
	}
	if isSimpleColumnRef(exprSQL) {
		addColumnRef(exprSQL, cols)
		return
	}
	if strings.HasPrefix(strings.ToUpper(exprSQL), "CASE") {
		addPredicateColumns(exprSQL, cols)
		return
	}
	parser := newExprParser(exprSQL)
	node, err := parser.parse()
	if err != nil {
		addPredicateColumns(exprSQL, cols)
		return
	}
	collectExprNodeColumns(node, cols)
}

func collectExprNodeColumns(node exprNode, cols map[string]struct{}) {
	switch n := node.(type) {
	case *identNode:
		addColumnRef(n.name, cols)
	case *unaryNode:
		collectExprNodeColumns(n.inner, cols)
	case *binOpNode:
		collectExprNodeColumns(n.left, cols)
		collectExprNodeColumns(n.right, cols)
	case *jsonAccessNode:
		collectExprNodeColumns(n.inner, cols)
	case *castNode:
		collectExprNodeColumns(n.inner, cols)
	case *funcCallNode:
		for _, arg := range n.args {
			collectExprNodeColumns(arg, cols)
		}
	case *intervalNode:
		return
	case *literalNode:
		return
	}
}

func isKeyword(ident string) bool {
	_, ok := keywordSet[strings.ToUpper(ident)]
	return ok
}
