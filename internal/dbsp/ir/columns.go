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
	if collectColumns(root, cols, nil) {
		return nil
	}
	if len(cols) == 0 {
		return nil
	}
	return cols
}

// CollectRequiredInputTypeHints returns conservative source type hints inferred
// from the logical plan. Hints are only emitted when the plan implies a stable
// source-level type, such as timestamp/time columns used by time operators.
func CollectRequiredInputTypeHints(root LogicalNode) map[string]string {
	if root == nil {
		return nil
	}
	hints := make(map[string]string)
	collectTypeHints(root, hints)
	if len(hints) == 0 {
		return nil
	}
	return hints
}

func collectColumns(node LogicalNode, cols map[string]struct{}, ctes map[string]LogicalNode) bool {
	switch n := node.(type) {
	case *LogicalScan:
		return false
	case *LogicalFilter:
		addResolvedExprColumns(n.Input, n.PredicateSQL, cols, ctes)
		return collectColumns(n.Input, cols, ctes)
	case *LogicalProject:
		keepAllInput := n.KeepInput || hasStar(n.Columns)
		if !keepAllInput {
			for _, col := range n.Columns {
				if strings.TrimSpace(col) == "*" {
					continue
				}
				_ = resolveInputColumnRefs(n.Input, col, cols, ctes)
			}
		}
		for _, expr := range n.Exprs {
			addResolvedExprColumns(n.Input, expr.ExprSQL, cols, ctes)
		}
		if keepAllInput {
			return collectColumns(n.Input, cols, ctes)
		}
		return false
	case *LogicalGroupAgg:
		for _, key := range n.Keys {
			if !resolveInputColumnRefs(n.Input, key, cols, ctes) && !isSimpleColumnRef(key) {
				addExprColumns(key, cols)
			}
		}
		if n.AggCol != "" {
			addResolvedExprColumns(n.Input, n.AggCol, cols, ctes)
		}
		for _, agg := range n.Aggs {
			if agg.Col != "" {
				addResolvedExprColumns(n.Input, agg.Col, cols, ctes)
			}
		}
		if n.WindowSpec != nil {
			addResolvedColumnRef(n.Input, n.WindowSpec.TimeCol, cols, ctes)
		}
		if n.TimeWindowSpec != nil {
			addResolvedColumnRef(n.Input, n.TimeWindowSpec.TimeCol, cols, ctes)
		}
		return collectColumns(n.Input, cols, ctes)
	case *LogicalWindowFunc:
		for _, key := range n.Spec.PartitionBy {
			addResolvedColumnRef(n.Input, key, cols, ctes)
		}
		addResolvedExprColumns(n.Input, n.Spec.OrderBy, cols, ctes)
		for _, arg := range n.Spec.Args {
			addResolvedExprColumns(n.Input, arg, cols, ctes)
		}
		return collectColumns(n.Input, cols, ctes)
	case *LogicalWindowAgg:
		for _, key := range n.PartitionBy {
			addResolvedColumnRef(n.Input, key, cols, ctes)
		}
		addResolvedExprColumns(n.Input, n.OrderBy, cols, ctes)
		if n.AggCol != "" {
			addResolvedExprColumns(n.Input, n.AggCol, cols, ctes)
		}
		if n.TimeWindowSpec != nil {
			addResolvedColumnRef(n.Input, n.TimeWindowSpec.TimeCol, cols, ctes)
		}
		return collectColumns(n.Input, cols, ctes)
	case *LogicalSort:
		for _, col := range n.OrderColumns {
			addResolvedExprColumns(n.Input, col, cols, ctes)
		}
		return collectColumns(n.Input, cols, ctes)
	case *LogicalLimit:
		return collectColumns(n.Input, cols, ctes)
	case *LogicalJoin:
		for _, cond := range n.Conditions {
			addColumnRef(cond.LeftCol, cols)
			addColumnRef(cond.RightCol, cols)
		}
		if collectColumns(n.Left, cols, ctes) {
			return true
		}
		return collectColumns(n.Right, cols, ctes)
	case *LogicalView:
		for _, key := range n.PartitionBy {
			addResolvedColumnRef(n.Input, key, cols, ctes)
		}
		return collectColumns(n.Input, cols, ctes)
	case *LogicalWith:
		return collectColumns(n.Body, cols, mergeCTERefs(ctes, n.CTEs))
	case *LogicalCTERef:
		if ctes == nil {
			return false
		}
		cte, ok := ctes[n.CTEName]
		if !ok {
			return false
		}
		return collectColumns(cte, cols, ctes)
	default:
		return false
	}
}

func resolveInputColumnRefs(node LogicalNode, col string, cols map[string]struct{}, ctes map[string]LogicalNode) bool {
	target := normalizeColumnRef(col)
	if target == "" || target == "*" {
		return false
	}
	switch n := node.(type) {
	case nil:
		return false
	case *LogicalScan:
		addColumnRef(col, cols)
		return true
	case *LogicalFilter:
		return resolveInputColumnRefs(n.Input, col, cols, ctes)
	case *LogicalSort:
		return resolveInputColumnRefs(n.Input, col, cols, ctes)
	case *LogicalLimit:
		return resolveInputColumnRefs(n.Input, col, cols, ctes)
	case *LogicalView:
		return resolveInputColumnRefs(n.Input, col, cols, ctes)
	case *LogicalWith:
		return resolveInputColumnRefs(n.Body, col, cols, mergeCTERefs(ctes, n.CTEs))
	case *LogicalCTERef:
		if ctes == nil {
			return false
		}
		cte, ok := ctes[n.CTEName]
		if !ok {
			return false
		}
		return resolveInputColumnRefs(cte, col, cols, ctes)
	case *LogicalProject:
		resolved := false
		if n.KeepInput || hasStar(n.Columns) {
			resolved = resolveInputColumnRefs(n.Input, col, cols, ctes) || resolved
		}
		for _, projected := range n.Columns {
			if normalizeColumnRef(projected) != target {
				continue
			}
			resolved = resolveInputColumnRefs(n.Input, projected, cols, ctes) || resolved
		}
		for _, expr := range n.Exprs {
			if normalizeColumnRef(expr.As) != target {
				continue
			}
			addResolvedExprColumns(n.Input, expr.ExprSQL, cols, ctes)
			resolved = true
		}
		return resolved
	case *LogicalGroupAgg:
		resolved := false
		addGroupKeys := func() {
			for _, key := range n.Keys {
				if !resolveInputColumnRefs(n.Input, key, cols, ctes) {
					if isSimpleColumnRef(key) {
						addColumnRef(key, cols)
					} else {
						addResolvedExprColumns(n.Input, key, cols, ctes)
					}
				}
			}
			if n.WindowSpec != nil {
				addResolvedColumnRef(n.Input, n.WindowSpec.TimeCol, cols, ctes)
			}
			if n.TimeWindowSpec != nil {
				addResolvedColumnRef(n.Input, n.TimeWindowSpec.TimeCol, cols, ctes)
			}
		}
		for _, key := range n.Keys {
			if normalizeColumnRef(key) != target {
				continue
			}
			if resolveInputColumnRefs(n.Input, key, cols, ctes) || !isSimpleColumnRef(key) {
				if !isSimpleColumnRef(key) {
					addExprColumns(key, cols)
				}
				resolved = true
			}
		}
		for _, agg := range n.Aggs {
			if normalizeColumnRef(agg.As) != target {
				continue
			}
			addGroupKeys()
			if agg.Col != "" && agg.Col != "*" {
				addResolvedExprColumns(n.Input, agg.Col, cols, ctes)
			}
			resolved = true
		}
		if !resolved && normalizeColumnRef(n.AggCol) == target {
			addGroupKeys()
			addResolvedExprColumns(n.Input, n.AggCol, cols, ctes)
			resolved = true
		}
		return resolved
	case *LogicalWindowFunc:
		if normalizeColumnRef(n.OutputCol) == target {
			for _, key := range n.Spec.PartitionBy {
				addResolvedColumnRef(n.Input, key, cols, ctes)
			}
			addResolvedExprColumns(n.Input, n.Spec.OrderBy, cols, ctes)
			for _, arg := range n.Spec.Args {
				addResolvedExprColumns(n.Input, arg, cols, ctes)
			}
			return true
		}
		return resolveInputColumnRefs(n.Input, col, cols, ctes)
	case *LogicalWindowAgg:
		return resolveInputColumnRefs(n.Input, col, cols, ctes)
	case *LogicalJoin:
		return resolveInputColumnRefs(n.Left, col, cols, ctes) || resolveInputColumnRefs(n.Right, col, cols, ctes)
	default:
		return false
	}
}

func addResolvedColumnRef(input LogicalNode, col string, cols map[string]struct{}, ctes map[string]LogicalNode) {
	if !resolveInputColumnRefs(input, col, cols, ctes) {
		addColumnRef(col, cols)
	}
}

func addResolvedExprColumns(input LogicalNode, exprSQL string, cols map[string]struct{}, ctes map[string]LogicalNode) {
	exprSQL = strings.TrimSpace(exprSQL)
	if exprSQL == "" {
		return
	}
	if isSimpleColumnRef(exprSQL) {
		addResolvedColumnRef(input, exprSQL, cols, ctes)
		return
	}
	if strings.HasPrefix(strings.ToUpper(exprSQL), "CASE") {
		addResolvedPredicateColumns(input, exprSQL, cols, ctes)
		return
	}
	parser := newExprParser(exprSQL)
	node, err := parser.parse()
	if err != nil {
		addResolvedPredicateColumns(input, exprSQL, cols, ctes)
		return
	}
	collectResolvedExprNodeColumns(input, node, cols, ctes)
}

func addResolvedPredicateColumns(input LogicalNode, predicateSQL string, cols map[string]struct{}, ctes map[string]LogicalNode) {
	predicateSQL = stripSingleQuotedLiterals(predicateSQL)
	for _, id := range identRegex.FindAllString(predicateSQL, -1) {
		if isKeyword(id) {
			continue
		}
		addResolvedColumnRef(input, id, cols, ctes)
	}
}

func collectResolvedExprNodeColumns(input LogicalNode, node exprNode, cols map[string]struct{}, ctes map[string]LogicalNode) {
	switch n := node.(type) {
	case *identNode:
		addResolvedColumnRef(input, n.name, cols, ctes)
	case *unaryNode:
		collectResolvedExprNodeColumns(input, n.inner, cols, ctes)
	case *binOpNode:
		collectResolvedExprNodeColumns(input, n.left, cols, ctes)
		collectResolvedExprNodeColumns(input, n.right, cols, ctes)
	case *jsonAccessNode:
		collectResolvedExprNodeColumns(input, n.inner, cols, ctes)
	case *castNode:
		collectResolvedExprNodeColumns(input, n.inner, cols, ctes)
	case *funcCallNode:
		for _, arg := range n.args {
			collectResolvedExprNodeColumns(input, arg, cols, ctes)
		}
	case *intervalNode:
		return
	case *literalNode:
		return
	}
}

func mergeCTERefs(parent map[string]LogicalNode, local map[string]LogicalNode) map[string]LogicalNode {
	if len(parent) == 0 && len(local) == 0 {
		return nil
	}
	merged := make(map[string]LogicalNode, len(parent)+len(local))
	for name, node := range parent {
		merged[name] = node
	}
	for name, node := range local {
		merged[name] = node
	}
	return merged
}

func collectTypeHints(node LogicalNode, hints map[string]string) {
	switch n := node.(type) {
	case *LogicalScan:
		return
	case *LogicalFilter:
		addExprTypeHints(n.PredicateSQL, hints)
		collectTypeHints(n.Input, hints)
	case *LogicalProject:
		for _, expr := range n.Exprs {
			addExprTypeHints(expr.ExprSQL, hints)
		}
		collectTypeHints(n.Input, hints)
	case *LogicalGroupAgg:
		if n.AggCol != "" {
			addExprTypeHints(n.AggCol, hints)
		}
		for _, agg := range n.Aggs {
			if agg.Col != "" {
				addExprTypeHints(agg.Col, hints)
			}
		}
		if n.WindowSpec != nil {
			addTypeHint(n.WindowSpec.TimeCol, "timestamp", hints)
		}
		if n.TimeWindowSpec != nil {
			addTypeHint(n.TimeWindowSpec.TimeCol, "timestamp", hints)
		}
		collectTypeHints(n.Input, hints)
	case *LogicalWindowFunc:
		addExprTypeHints(n.Spec.OrderBy, hints)
		for _, arg := range n.Spec.Args {
			addExprTypeHints(arg, hints)
		}
		collectTypeHints(n.Input, hints)
	case *LogicalWindowAgg:
		addExprTypeHints(n.OrderBy, hints)
		if n.AggCol != "" {
			addExprTypeHints(n.AggCol, hints)
		}
		if n.TimeWindowSpec != nil {
			addTypeHint(n.TimeWindowSpec.TimeCol, "timestamp", hints)
		}
		collectTypeHints(n.Input, hints)
	case *LogicalSort:
		for _, col := range n.OrderColumns {
			addExprTypeHints(col, hints)
		}
		collectTypeHints(n.Input, hints)
	case *LogicalLimit:
		collectTypeHints(n.Input, hints)
	case *LogicalJoin:
		collectTypeHints(n.Left, hints)
		collectTypeHints(n.Right, hints)
	case *LogicalView:
		collectTypeHints(n.Input, hints)
	case *LogicalWith:
		collectTypeHints(n.Body, hints)
		for _, cte := range n.CTEs {
			collectTypeHints(cte, hints)
		}
	case *LogicalCTERef:
		return
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
	predicateSQL = stripSingleQuotedLiterals(predicateSQL)
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

func normalizeColumnRef(col string) string {
	col = strings.TrimSpace(strings.Trim(col, "`\"'"))
	return strings.ToLower(col)
}

func stripSingleQuotedLiterals(sql string) string {
	if sql == "" {
		return sql
	}
	var b strings.Builder
	b.Grow(len(sql))
	inString := false
	for idx := 0; idx < len(sql); idx++ {
		ch := sql[idx]
		if ch == '\'' {
			b.WriteByte(' ')
			if inString && idx+1 < len(sql) && sql[idx+1] == '\'' {
				b.WriteByte(' ')
				idx++
				continue
			}
			inString = !inString
			continue
		}
		if inString {
			b.WriteByte(' ')
			continue
		}
		b.WriteByte(ch)
	}
	return b.String()
}

func addExprTypeHints(exprSQL string, hints map[string]string) {
	exprSQL = strings.TrimSpace(exprSQL)
	if exprSQL == "" {
		return
	}
	parser := newExprParser(exprSQL)
	node, err := parser.parse()
	if err != nil {
		return
	}
	collectExprNodeTypeHints(node, hints)
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

func collectExprNodeTypeHints(node exprNode, hints map[string]string) {
	switch n := node.(type) {
	case *identNode:
		return
	case *unaryNode:
		collectExprNodeTypeHints(n.inner, hints)
	case *binOpNode:
		collectExprNodeTypeHints(n.left, hints)
		collectExprNodeTypeHints(n.right, hints)
	case *jsonAccessNode:
		collectExprNodeTypeHints(n.inner, hints)
	case *castNode:
		if isTimestampTargetType(n.targetType) {
			if ident, ok := n.inner.(*identNode); ok {
				addTypeHint(ident.name, "timestamp", hints)
			}
		}
		collectExprNodeTypeHints(n.inner, hints)
	case *funcCallNode:
		switch strings.ToUpper(n.name) {
		case "TIME_BUCKET":
			if len(n.args) >= 2 {
				addTimestampHintFromExpr(n.args[1], hints)
			}
		case "STRFTIME":
			if len(n.args) >= 2 {
				addTimestampHintFromExpr(n.args[1], hints)
			}
		case "EPOCH":
			if len(n.args) >= 1 {
				addTimestampHintFromExpr(n.args[0], hints)
			}
		}
		for _, arg := range n.args {
			collectExprNodeTypeHints(arg, hints)
		}
	case *intervalNode:
		return
	case *literalNode:
		return
	}
}

func addTimestampHintFromExpr(node exprNode, hints map[string]string) {
	switch n := node.(type) {
	case *identNode:
		addTypeHint(n.name, "timestamp", hints)
	case *castNode:
		if ident, ok := n.inner.(*identNode); ok && isTimestampTargetType(n.targetType) {
			addTypeHint(ident.name, "timestamp", hints)
		}
	}
}

func addTypeHint(col string, hint string, hints map[string]string) {
	col = strings.TrimSpace(strings.Trim(col, "`\"'"))
	if col == "" || col == "*" || hint == "" {
		return
	}
	hints[col] = hint
	if idx := strings.LastIndex(col, "."); idx > 0 && idx < len(col)-1 {
		hints[col[idx+1:]] = hint
	}
}

func isTimestampTargetType(target string) bool {
	switch strings.ToUpper(strings.TrimSpace(target)) {
	case "TIMESTAMP", "TIME", "DATE":
		return true
	default:
		return false
	}
}

func isKeyword(ident string) bool {
	_, ok := keywordSet[strings.ToUpper(ident)]
	return ok
}
