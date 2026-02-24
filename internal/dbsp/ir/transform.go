package ir

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/ariyn/dbsp/internal/dbsp/op"
	"github.com/ariyn/dbsp/internal/dbsp/types"
)

func buildGroupKeyFn(keys []string) func(types.Tuple) any {
	if len(keys) == 0 {
		return func(types.Tuple) any { return nil }
	}

	exprFns := make([]func(types.Tuple) (any, error), len(keys))
	for i, key := range keys {
		if strings.ContainsAny(key, "()->:+-*/ ") {
			exprFns[i] = BuildExprFunc(key)
		} else {
			k := key
			exprFns[i] = func(t types.Tuple) (any, error) { return t[k], nil }
		}
	}

	if len(keys) == 1 {
		fn := exprFns[0]
		return func(t types.Tuple) any {
			v, _ := fn(t)
			return v
		}
	}

	keyCols := append([]string(nil), keys...)
	return func(t types.Tuple) any {
		kt := make(types.Tuple, len(keyCols))
		for i, col := range keyCols {
			v, _ := exprFns[i](t)
			kt[col] = v
		}
		b, err := json.Marshal(kt)
		if err == nil {
			return string(b)
		}
		return fmt.Sprintf("%#v", kt)
	}
}

// buildWindowKeyFn creates a key function for a simple tumbling window
// over a single time column. For the first version we assume that the
// time column is represented as an int64 containing milliseconds since
// epoch. The key is the window start time in milliseconds.
func buildWindowKeyFn(timeCol string, sizeMillis int64) func(types.Tuple) any {
	return func(t types.Tuple) any {
		v, ok := t[timeCol]
		if !ok || v == nil {
			return nil
		}
		// We keep the first implementation very strict and only accept int64.
		// Other representations (string, time.Time, etc.) can be added later
		// with explicit conversion.
		ms, ok := v.(int64)
		if !ok {
			return nil
		}
		if sizeMillis <= 0 {
			return ms
		}
		windowStart := (ms / sizeMillis) * sizeMillis
		return windowStart
	}
}

// BuildPredicateFunc converts a simple SQL WHERE condition into a predicate function.
// Supports: =, !=, <, <=, >, >=, AND, OR, parentheses
func BuildPredicateFunc(predicateSQL string) func(types.Tuple) bool {
	predicateSQL = strings.TrimSpace(predicateSQL)
	// Clean up backticks from SQL identifiers (e.g., `status` -> status)
	predicateSQL = strings.ReplaceAll(predicateSQL, "`", "")

	// Handle parentheses first (highest precedence)
	if strings.Contains(predicateSQL, "(") {
		return buildPredicateWithParens(predicateSQL)
	}

	// Handle OR (lowest precedence)
	if containsKeywordOutsideParens(predicateSQL, "OR") {
		orParts := splitByKeywordOutsideParens(predicateSQL, "OR")
		if len(orParts) > 1 {
			predicates := make([]func(types.Tuple) bool, len(orParts))
			for i, part := range orParts {
				predicates[i] = BuildPredicateFunc(part)
			}
			return func(t types.Tuple) bool {
				for _, pred := range predicates {
					if pred(t) {
						return true
					}
				}
				return false
			}
		}
	}

	// Handle AND (higher precedence than OR)
	if containsKeywordOutsideParens(predicateSQL, "AND") {
		andParts := splitByKeywordOutsideParens(predicateSQL, "AND")
		if len(andParts) > 1 {
			predicates := make([]func(types.Tuple) bool, len(andParts))
			for i, part := range andParts {
				predicates[i] = BuildPredicateFunc(part)
			}
			return func(t types.Tuple) bool {
				for _, pred := range predicates {
					if !pred(t) {
						return false
					}
				}
				return true
			}
		}
	}

	// Handle single comparison operators
	return buildComparisonFunc(predicateSQL)
}

// buildPredicateWithParens handles expressions with parentheses
func buildPredicateWithParens(predicateSQL string) func(types.Tuple) bool {
	predicateSQL = strings.TrimSpace(predicateSQL)

	// If the entire expression is wrapped in balanced outer parens, unwrap it
	if strings.HasPrefix(predicateSQL, "(") && strings.HasSuffix(predicateSQL, ")") {
		if isBalancedAndOuter(predicateSQL) {
			inner := predicateSQL[1 : len(predicateSQL)-1]
			return BuildPredicateFunc(inner)
		}
	}

	// Process OR outside parentheses (lowest precedence)
	if containsKeywordOutsideParens(predicateSQL, "OR") {
		orParts := splitByKeywordOutsideParens(predicateSQL, "OR")
		if len(orParts) > 1 {
			predicates := make([]func(types.Tuple) bool, len(orParts))
			for i, part := range orParts {
				predicates[i] = BuildPredicateFunc(part)
			}
			return func(t types.Tuple) bool {
				for _, pred := range predicates {
					if pred(t) {
						return true
					}
				}
				return false
			}
		}
	}

	// Process AND outside parentheses (higher precedence than OR)
	if containsKeywordOutsideParens(predicateSQL, "AND") {
		andParts := splitByKeywordOutsideParens(predicateSQL, "AND")
		if len(andParts) > 1 {
			predicates := make([]func(types.Tuple) bool, len(andParts))
			for i, part := range andParts {
				predicates[i] = BuildPredicateFunc(part)
			}
			return func(t types.Tuple) bool {
				for _, pred := range predicates {
					if !pred(t) {
						return false
					}
				}
				return true
			}
		}
	}

	// No operators found outside parens, must be a single comparison
	return buildComparisonFunc(predicateSQL)
}

// isBalancedAndOuter checks if the opening and closing parens are the outermost pair
func isBalancedAndOuter(s string) bool {
	if !strings.HasPrefix(s, "(") || !strings.HasSuffix(s, ")") {
		return false
	}

	depth := 0
	for i, ch := range s {
		if ch == '(' {
			depth++
		} else if ch == ')' {
			depth--
			// If we hit zero before the end, the parens aren't outer
			if depth == 0 && i < len(s)-1 {
				return false
			}
		}
	}
	return depth == 0
}

// containsKeywordOutsideParens checks if a keyword exists outside of parentheses
func containsKeywordOutsideParens(s, keyword string) bool {
	upper := strings.ToUpper(s)
	keywordUpper := " " + strings.ToUpper(keyword) + " "

	depth := 0
	for i := 0; i < len(s); i++ {
		if s[i] == '(' {
			depth++
		} else if s[i] == ')' {
			depth--
		} else if depth == 0 {
			// Check if keyword starts here
			if i+len(keywordUpper) <= len(upper) && upper[i:i+len(keywordUpper)] == keywordUpper {
				return true
			}
		}
	}
	return false
}

// splitByKeywordOutsideParens splits by keyword only when outside parentheses
func splitByKeywordOutsideParens(s, keyword string) []string {
	upper := strings.ToUpper(s)
	keywordUpper := " " + strings.ToUpper(keyword) + " "

	var parts []string
	depth := 0
	lastIdx := 0

	for i := 0; i < len(s); i++ {
		if s[i] == '(' {
			depth++
		} else if s[i] == ')' {
			depth--
		} else if depth == 0 {
			// Check if keyword starts here
			if i+len(keywordUpper) <= len(upper) && upper[i:i+len(keywordUpper)] == keywordUpper {
				parts = append(parts, strings.TrimSpace(s[lastIdx:i]))
				lastIdx = i + len(keywordUpper)
				i = lastIdx - 1 // Will be incremented by loop
			}
		}
	}

	if lastIdx < len(s) {
		parts = append(parts, strings.TrimSpace(s[lastIdx:]))
	}

	return parts
}

// buildComparisonFunc builds a predicate for a single comparison
func buildComparisonFunc(predicateSQL string) func(types.Tuple) bool {
	predicateSQL = strings.TrimSpace(predicateSQL)

	// Check for IS NULL / IS NOT NULL first
	if strings.Contains(strings.ToUpper(predicateSQL), " IS NULL") {
		return buildIsNullFunc(predicateSQL)
	}

	if strings.Contains(strings.ToUpper(predicateSQL), " IS NOT NULL") {
		return buildIsNotNullFunc(predicateSQL)
	}

	// Try operators in order: !=, <=, >=, =, <, >
	// Check two-char operators first to avoid false matches

	if strings.Contains(predicateSQL, "!=") {
		return buildNotEqualFunc(predicateSQL)
	}

	if strings.Contains(predicateSQL, ">=") {
		return buildGreaterEqualFunc(predicateSQL)
	}

	if strings.Contains(predicateSQL, "<=") {
		return buildLessEqualFunc(predicateSQL)
	}

	if strings.Contains(predicateSQL, "=") {
		return buildEqualFunc(predicateSQL)
	}

	if strings.Contains(predicateSQL, ">") {
		return buildGreaterFunc(predicateSQL)
	}

	if strings.Contains(predicateSQL, "<") {
		return buildLessFunc(predicateSQL)
	}

	// Default: always true
	return func(t types.Tuple) bool { return true }
}

// buildIsNullFunc handles "column IS NULL"
func buildIsNullFunc(predicateSQL string) func(types.Tuple) bool {
	idx := strings.Index(strings.ToUpper(predicateSQL), " IS NULL")
	if idx == -1 {
		return func(t types.Tuple) bool { return false }
	}
	colExpr := strings.TrimSpace(predicateSQL[:idx])
	exprFn := BuildExprFunc(colExpr)

	return func(t types.Tuple) bool {
		val, err := exprFn(t)
		return err != nil || val == nil
	}
}

// buildIsNotNullFunc handles "column IS NOT NULL"
func buildIsNotNullFunc(predicateSQL string) func(types.Tuple) bool {
	idx := strings.Index(strings.ToUpper(predicateSQL), " IS NOT NULL")
	if idx == -1 {
		return func(t types.Tuple) bool { return false }
	}
	colExpr := strings.TrimSpace(predicateSQL[:idx])
	exprFn := BuildExprFunc(colExpr)

	return func(t types.Tuple) bool {
		val, err := exprFn(t)
		return err == nil && val != nil
	}
}

// buildEqualFunc handles "column = value"
func buildEqualFunc(predicateSQL string) func(types.Tuple) bool {
	parts := strings.Split(predicateSQL, "=")
	if len(parts) != 2 {
		return func(t types.Tuple) bool { return true }
	}

	leftExpr := strings.TrimSpace(parts[0])
	val := strings.TrimSpace(parts[1])
	val = strings.Trim(val, "'\"")
	exprFn := BuildExprFunc(leftExpr)

	return func(t types.Tuple) bool {
		tupleVal, err := exprFn(t)
		// NULL values should not match in equality comparisons
		if err != nil || tupleVal == nil {
			return false
		}
		return compareEqual(tupleVal, val)
	}
}

// buildNotEqualFunc handles "column != value"
func buildNotEqualFunc(predicateSQL string) func(types.Tuple) bool {
	parts := strings.Split(predicateSQL, "!=")
	if len(parts) != 2 {
		return func(t types.Tuple) bool { return true }
	}

	leftExpr := strings.TrimSpace(parts[0])
	val := strings.TrimSpace(parts[1])
	val = strings.Trim(val, "'\"")
	exprFn := BuildExprFunc(leftExpr)

	return func(t types.Tuple) bool {
		tupleVal, err := exprFn(t)
		// NULL values should not match in inequality comparisons
		if err != nil || tupleVal == nil {
			return false
		}
		return !compareEqual(tupleVal, val)
	}
}

// buildGreaterFunc handles "column > value"
func buildGreaterFunc(predicateSQL string) func(types.Tuple) bool {
	parts := strings.Split(predicateSQL, ">")
	if len(parts) != 2 {
		return func(t types.Tuple) bool { return true }
	}

	leftExpr := strings.TrimSpace(parts[0])
	valStr := strings.TrimSpace(parts[1])
	threshold, err := strconv.ParseFloat(valStr, 64)
	if err != nil {
		return func(t types.Tuple) bool { return false }
	}
	exprFn := BuildExprFunc(leftExpr)

	return func(t types.Tuple) bool {
		tupleVal, err := exprFn(t)
		// NULL values should not match in comparisons
		if err != nil || tupleVal == nil {
			return false
		}
		return compareGreater(tupleVal, threshold)
	}
}

// buildGreaterEqualFunc handles "column >= value"
func buildGreaterEqualFunc(predicateSQL string) func(types.Tuple) bool {
	parts := strings.Split(predicateSQL, ">=")
	if len(parts) != 2 {
		return func(t types.Tuple) bool { return true }
	}

	leftExpr := strings.TrimSpace(parts[0])
	valStr := strings.TrimSpace(parts[1])
	threshold, err := strconv.ParseFloat(valStr, 64)
	if err != nil {
		return func(t types.Tuple) bool { return false }
	}
	exprFn := BuildExprFunc(leftExpr)

	return func(t types.Tuple) bool {
		tupleVal, err := exprFn(t)
		// NULL values should not match in comparisons
		if err != nil || tupleVal == nil {
			return false
		}
		return compareGreaterOrEqual(tupleVal, threshold)
	}
}

// buildLessFunc handles "column < value"
func buildLessFunc(predicateSQL string) func(types.Tuple) bool {
	parts := strings.Split(predicateSQL, "<")
	if len(parts) != 2 {
		return func(t types.Tuple) bool { return true }
	}

	leftExpr := strings.TrimSpace(parts[0])
	valStr := strings.TrimSpace(parts[1])
	threshold, err := strconv.ParseFloat(valStr, 64)
	if err != nil {
		return func(t types.Tuple) bool { return false }
	}
	exprFn := BuildExprFunc(leftExpr)

	return func(t types.Tuple) bool {
		tupleVal, err := exprFn(t)
		// NULL values should not match in comparisons
		if err != nil || tupleVal == nil {
			return false
		}
		return compareLess(tupleVal, threshold)
	}
}

// buildLessEqualFunc handles "column <= value"
func buildLessEqualFunc(predicateSQL string) func(types.Tuple) bool {
	parts := strings.Split(predicateSQL, "<=")
	if len(parts) != 2 {
		return func(t types.Tuple) bool { return true }
	}

	leftExpr := strings.TrimSpace(parts[0])
	valStr := strings.TrimSpace(parts[1])
	threshold, err := strconv.ParseFloat(valStr, 64)
	if err != nil {
		return func(t types.Tuple) bool { return false }
	}
	exprFn := BuildExprFunc(leftExpr)

	return func(t types.Tuple) bool {
		tupleVal, err := exprFn(t)
		// NULL values should not match in comparisons
		if err != nil || tupleVal == nil {
			return false
		}
		return compareLessOrEqual(tupleVal, threshold)
	}
}

// Helper comparison functions
func compareEqual(tupleVal any, val string) bool {
	// If the RHS looks numeric, prefer numeric comparison (tolerant).
	if rhs, err := strconv.ParseFloat(val, 64); err == nil {
		if lhs, ok := toFloat64Loose(tupleVal); ok {
			return lhs == rhs
		}
	}
	// Fallback to string-ish comparison.
	if strVal, ok := tupleVal.(string); ok {
		return strVal == val
	}
	return fmt.Sprintf("%v", tupleVal) == val
}

func toFloat64Loose(v any) (float64, bool) {
	switch x := v.(type) {
	case float64:
		return x, true
	case float32:
		return float64(x), true
	case int:
		return float64(x), true
	case int64:
		return float64(x), true
	case int32:
		return float64(x), true
	case uint:
		return float64(x), true
	case uint64:
		return float64(x), true
	case uint32:
		return float64(x), true
	case string:
		f, err := strconv.ParseFloat(strings.TrimSpace(x), 64)
		if err != nil {
			return 0, false
		}
		return f, true
	case json.Number:
		f, err := x.Float64()
		if err != nil {
			return 0, false
		}
		return f, true
	default:
		return 0, false
	}
}

func compareGreater(tupleVal any, threshold float64) bool {
	if v, ok := toFloat64Loose(tupleVal); ok {
		return v > threshold
	}
	return false
}

func compareGreaterOrEqual(tupleVal any, threshold float64) bool {
	if v, ok := toFloat64Loose(tupleVal); ok {
		return v >= threshold
	}
	return false
}

func compareLess(tupleVal any, threshold float64) bool {
	if v, ok := toFloat64Loose(tupleVal); ok {
		return v < threshold
	}
	return false
}

func compareLessOrEqual(tupleVal any, threshold float64) bool {
	if v, ok := toFloat64Loose(tupleVal); ok {
		return v <= threshold
	}
	return false
}

// LogicalToDBSP transforms a LogicalNode into a runtime DBSP operator Node.
// For Phase1 it supports LogicalScan -> LogicalFilter -> LogicalProject -> LogicalGroupAgg pattern.
func LogicalToDBSP(l LogicalNode) (*op.Node, error) {
	return logicalToDBSPWithContext(l, make(map[string]*op.Node))
}

// attachLogicalGroupAggInputWithContext handles input attachment for GroupAgg, supporting chaining and recursive transformation.
func attachLogicalGroupAggInputWithContext(n *LogicalGroupAgg, aggOp op.Operator, ctes map[string]*op.Node) (*op.Node, error) {
	if n == nil || n.Input == nil {
		return &op.Node{Op: aggOp}, nil
	}

	// Filter before GroupAgg: chain filter MapOp then the group agg op.
	if f, ok := n.Input.(*LogicalFilter); ok {
		predicateFn := BuildPredicateFunc(f.PredicateSQL)
		filterOp := &op.MapOp{
			F: func(td types.TupleDelta) []types.TupleDelta {
				if predicateFn(td.Tuple) {
					return []types.TupleDelta{td}
				}
				return nil
			},
		}

		inNode, err := logicalToDBSPWithContext(f.Input, ctes)
		if err != nil {
			return nil, err
		}
		chained := &op.ChainedOp{Ops: []op.Operator{filterOp, aggOp}}
		return &op.Node{Op: chained, Inputs: []*op.Node{inNode}}, nil
	}

	// Normal recursive transform for input.
	inNode, err := logicalToDBSPWithContext(n.Input, ctes)
	if err != nil {
		return nil, err
	}

	return &op.Node{Op: aggOp, Inputs: []*op.Node{inNode}}, nil
}

func logicalToDBSPWithContext(l LogicalNode, ctes map[string]*op.Node) (*op.Node, error) {
	switch n := l.(type) {
	case *LogicalView:
		// View is the root for a sink. It carries PartitionBy metadata.
		child, err := logicalToDBSPWithContext(n.Input, ctes)
		if err != nil {
			return nil, err
		}
		child.PartitionBy = append([]string(nil), n.PartitionBy...)
		return child, nil

	case *LogicalWith:
		// Transform CTEs in order.
		newCTEs := make(map[string]*op.Node)
		for k, v := range ctes {
			newCTEs[k] = v
		}

		for _, name := range n.CTENames {
			subLp := n.CTEs[name]
			node, err := logicalToDBSPWithContext(subLp, newCTEs)
			if err != nil {
				return nil, err
			}
			newCTEs[name] = node
		}

		return logicalToDBSPWithContext(n.Body, newCTEs)

	case *LogicalCTERef:
		node, ok := ctes[n.CTEName]
		if !ok {
			return nil, fmt.Errorf("undefined CTE: %s", n.CTEName)
		}
		return node, nil

	case *LogicalScan:
		return &op.Node{Source: n.Table}, nil

	case *LogicalProject:
		columns := append([]string(nil), n.Columns...)
		var projectOp op.Operator
		if len(n.Exprs) == 0 {
			// Backward-compatible: keep simple projections as MapOp.
			projectOp = &op.MapOp{
				F: func(td types.TupleDelta) []types.TupleDelta {
					projected := make(types.Tuple)
					for _, col := range columns {
						if val, ok := td.Tuple[col]; ok {
							projected[col] = val
						}
					}
					return []types.TupleDelta{{Tuple: projected, Count: td.Count}}
				},
			}
		} else {
			exprs := make([]op.ProjectExprFn, 0, len(n.Exprs))
			for _, e := range n.Exprs {
				fn := BuildExprFunc(e.ExprSQL)
				exprs = append(exprs, op.ProjectExprFn{OutCol: e.As, Eval: fn})
			}
			projectOp = &op.ProjectOp{Columns: columns, Exprs: exprs}
		}

		// Check if input needs processing
		if n.Input != nil {
			inNode, err := logicalToDBSPWithContext(n.Input, ctes)
			if err != nil {
				return nil, err
			}
			return &op.Node{Op: projectOp, Inputs: []*op.Node{inNode}}, nil
		}

		return &op.Node{Op: projectOp}, nil

	case *LogicalFilter:
		// Transform filter to MapOp
		predicateFn := BuildPredicateFunc(n.PredicateSQL)
		mapOp := &op.MapOp{
			F: func(td types.TupleDelta) []types.TupleDelta {
				if predicateFn(td.Tuple) {
					return []types.TupleDelta{td}
				}
				return nil
			},
		}
		if n.Input != nil {
			inNode, err := logicalToDBSPWithContext(n.Input, ctes)
			if err != nil {
				return nil, err
			}
			return &op.Node{Op: mapOp, Inputs: []*op.Node{inNode}}, nil
		}
		return &op.Node{Op: mapOp}, nil

	case *LogicalGroupAgg:
		// 1. Prepare key function
		keyFn := buildGroupKeyFn(n.Keys)

		// 2. Determine aggregate operator type and initialize it
		var aggOp op.Operator
		if len(n.Aggs) > 0 {
			// Multi-aggregate configuration
			aggSlots := make([]op.AggSlot, 0, len(n.Aggs))
			for _, a := range n.Aggs {
				name := strings.ToUpper(a.Name)
				switch name {
				case "SUM":
					s := &op.SumAgg{ColName: a.Col, DeltaCol: "agg_delta"}
					if strings.ContainsAny(a.Col, "()->:") {
						s.Expr = BuildExprFunc(a.Col)
					}
					aggSlots = append(aggSlots, op.AggSlot{
						Init: func() any { return float64(0) },
						Fn:   s,
					})
				case "AVG":
					avg := &op.AvgAgg{ColName: a.Col}
					if strings.ContainsAny(a.Col, "()->:") {
						avg.Expr = BuildExprFunc(a.Col)
					}
					aggSlots = append(aggSlots, op.AggSlot{
						Init: func() any { return op.AvgMonoid{} },
						Fn:   avg,
					})
				case "COUNT":
					c := &op.CountAgg{ColName: a.Col, DeltaCol: "count_delta"}
					if a.Col != "" && strings.ContainsAny(a.Col, "()->:") {
						c.Expr = BuildExprFunc(a.Col)
					}
					aggSlots = append(aggSlots, op.AggSlot{
						Init: func() any { return int64(0) },
						Fn:   c,
					})
				default:
					return nil, fmt.Errorf("unsupported agg %s in multi-aggregate", a.Name)
				}
			}
			g := op.NewGroupAggMultiOp(keyFn, aggSlots)
			g.SetGroupKeyColNames(n.Keys)
			aggOp = g
		} else {
			// Single aggregate configuration
			var agg op.AggFunc
			var aggInit func() any
			switch strings.ToUpper(n.AggName) {
			case "SUM":
				s := &op.SumAgg{ColName: n.AggCol}
				if strings.ContainsAny(n.AggCol, "()->:") {
					s.Expr = BuildExprFunc(n.AggCol)
				}
				agg = s
				aggInit = func() any { return float64(0) }
			case "COUNT":
				c := &op.CountAgg{ColName: n.AggCol}
				if n.AggCol != "" && strings.ContainsAny(n.AggCol, "()->:") {
					c.Expr = BuildExprFunc(n.AggCol)
				}
				agg = c
				aggInit = func() any { return int64(0) }
			case "AVG":
				av := &op.AvgAgg{ColName: n.AggCol}
				if strings.ContainsAny(n.AggCol, "()->:") {
					av.Expr = BuildExprFunc(n.AggCol)
				}
				agg = av
				aggInit = func() any { return nil }
			case "MIN":
				agg = &op.MinAgg{ColName: n.AggCol}
				aggInit = func() any { return op.NewSortedMultiset() }
			case "MAX":
				agg = &op.MaxAgg{ColName: n.AggCol}
				aggInit = func() any { return op.NewSortedMultiset() }
			default:
				return nil, fmt.Errorf("unsupported agg %s", n.AggName)
			}

			if n.WindowSpec != nil {
				ws := n.WindowSpec
				waSpec := op.WindowSpecLite{
					TimeCol:    ws.TimeCol,
					SizeMillis: ws.SizeMillis,
				}
				aggOp = op.NewWindowAggOp(waSpec, keyFn, n.Keys, aggInit, agg)
			} else {
				g := op.NewGroupAggOp(keyFn, aggInit, agg)
				g.SetGroupKeyColNames(n.Keys)
				aggOp = g
			}
		}

		// 3. Transform and attach input recursively
		return attachLogicalGroupAggInputWithContext(n, aggOp, ctes)

	case *LogicalWindowFunc:
		// Transform window function to appropriate operator
		return logicalWindowFuncToDBSPWithContext(n, ctes)

	case *LogicalWindowAgg:
		// Transform window aggregate function to appropriate operator
		return logicalWindowAggToDBSPWithContext(n, ctes)

	case *LogicalJoin:
		// Transform JOIN to BinaryOp
		return logicalJoinToDBSPWithContext(n, ctes)

	case *LogicalSort:
		// Transform ORDER BY to SortOp
		return logicalSortToDBSPWithContext(n, ctes)

	case *LogicalLimit:
		// Transform LIMIT to LimitOp
		return logicalLimitToDBSPWithContext(n, ctes)

	default:
		return nil, fmt.Errorf("unsupported logical node: %T", n)
	}
}

// logicalWindowFuncToDBSP transforms LogicalWindowFunc to DBSP operators
func logicalWindowFuncToDBSPWithContext(wf *LogicalWindowFunc, ctes map[string]*op.Node) (*op.Node, error) {
	if wf.Spec.FuncName != "LAG" {
		return nil, fmt.Errorf("only LAG window function is currently supported, got %s", wf.Spec.FuncName)
	}

	if len(wf.Spec.Args) == 0 {
		return nil, fmt.Errorf("LAG requires at least one argument")
	}

	lagCol := wf.Spec.Args[0]

	// Determine partition key function
	var keyFn func(types.Tuple) any
	if len(wf.Spec.PartitionBy) == 0 {
		// No partition - single global partition
		keyFn = func(t types.Tuple) any { return nil }
	} else if len(wf.Spec.PartitionBy) == 1 {
		// Single partition column
		keyCol := wf.Spec.PartitionBy[0]
		keyFn = func(t types.Tuple) any { return t[keyCol] }
	} else {
		// Multiple partition columns - composite key
		partCols := wf.Spec.PartitionBy
		keyFn = func(t types.Tuple) any {
			key := make([]any, len(partCols))
			for i, col := range partCols {
				key[i] = t[col]
			}
			return fmt.Sprintf("%v", key)
		}
	}

	// Create LagAgg operator
	lagAgg := &op.LagAgg{
		OrderByCol: wf.Spec.OrderBy,
		LagCol:     lagCol,
		Offset:     wf.Spec.Offset,
		OutputCol:  wf.OutputCol,
	}

	// If lagCol is an expression (contains -> or :: or operators or space), build an expression function
	if strings.ContainsAny(lagCol, "()->:+-*/ ") {
		lagAgg.LagExpr = BuildExprFunc(lagCol)
	}

	// Initialize function for LagMonoid
	aggInit := func() any {
		return op.LagMonoid{
			Buffer: op.NewOrderedBuffer(wf.Spec.OrderBy),
		}
	}

	// Create GroupAggOp to handle partitioning
	g := op.NewGroupAggOp(keyFn, aggInit, lagAgg)

	// Check if there's an input node
	if wf.Input != nil {
		inNode, err := logicalToDBSPWithContext(wf.Input, ctes)
		if err != nil {
			return nil, err
		}
		return &op.Node{Op: g, Inputs: []*op.Node{inNode}}, nil
	}

	return &op.Node{Op: g}, nil
}

// logicalWindowAggToDBSP transforms LogicalWindowAgg (DuckDB standard window aggregate) to DBSP operators
func logicalWindowAggToDBSPWithContext(wa *LogicalWindowAgg, ctes map[string]*op.Node) (*op.Node, error) {
	// Determine partition key function
	var keyFn func(types.Tuple) any
	if len(wa.PartitionBy) == 0 {
		// No partition - single global partition
		keyFn = func(t types.Tuple) any { return nil }
	} else if len(wa.PartitionBy) == 1 {
		// Single partition column
		keyCol := wa.PartitionBy[0]
		keyFn = func(t types.Tuple) any { return t[keyCol] }
	} else {
		// Multiple partition columns - composite key
		partCols := wa.PartitionBy
		keyFn = func(t types.Tuple) any {
			key := make([]any, len(partCols))
			for i, col := range partCols {
				key[i] = t[col]
			}
			return fmt.Sprintf("%v", key)
		}
	}

	// Create appropriate aggregate function
	var agg op.AggFunc
	var aggInit func() any

	switch wa.AggName {
	case "SUM":
		s := &op.SumAgg{ColName: wa.AggCol}
		if strings.ContainsAny(wa.AggCol, "()->:") {
			s.Expr = BuildExprFunc(wa.AggCol)
		}
		agg = s
		aggInit = func() any { return float64(0) }
	case "AVG":
		av := &op.AvgAgg{ColName: wa.AggCol}
		if strings.ContainsAny(wa.AggCol, "()->:") {
			av.Expr = BuildExprFunc(wa.AggCol)
		}
		agg = av
		aggInit = func() any { return op.AvgMonoid{} }
	case "COUNT":
		c := &op.CountAgg{ColName: wa.AggCol}
		if wa.AggCol != "" && strings.ContainsAny(wa.AggCol, "()->:") {
			c.Expr = BuildExprFunc(wa.AggCol)
		}
		agg = c
		aggInit = func() any { return int64(0) }
	case "MIN":
		agg = &op.MinAgg{ColName: wa.AggCol}
		aggInit = func() any { return op.NewSortedMultiset() }
	case "MAX":
		agg = &op.MaxAgg{ColName: wa.AggCol}
		aggInit = func() any { return op.NewSortedMultiset() }
	default:
		return nil, fmt.Errorf("unsupported window aggregate function: %s", wa.AggName)
	}

	// Check for time-based windowing
	if wa.TimeWindowSpec != nil {
		spec := wa.TimeWindowSpec

		// Convert to op.WindowType
		var windowType op.WindowType
		switch strings.ToUpper(spec.WindowType) {
		case "TUMBLING":
			windowType = op.WindowTypeTumbling
		case "SLIDING":
			windowType = op.WindowTypeSliding
		case "SESSION":
			windowType = op.WindowTypeSession
		default:
			windowType = op.WindowTypeTumbling
		}

		// Create WindowSpecLite for time-based windows
		windowSpec := op.WindowSpecLite{
			TimeCol:     spec.TimeCol,
			SizeMillis:  spec.SizeMillis,
			WindowType:  windowType,
			SlideMillis: spec.SlideMillis,
			GapMillis:   spec.GapMillis,
		}

		windowOp := op.NewWindowAggOp(windowSpec, keyFn, wa.PartitionBy, aggInit, agg)
		return attachLogicalWindowAggInputWithContext(wa, windowOp, ctes)
	}

	// For DuckDB window aggregates with ORDER BY and frame specification,
	// use WindowAggOp for proper frame-based aggregation
	if wa.OrderBy != "" && wa.FrameSpec != nil {
		// Convert FrameSpec to op.FrameSpecLite
		frameSpec := &op.FrameSpecLite{
			Type:       wa.FrameSpec.Type,
			StartType:  wa.FrameSpec.StartType,
			StartValue: wa.FrameSpec.StartValue,
			EndType:    wa.FrameSpec.EndType,
			EndValue:   wa.FrameSpec.EndValue,
		}

		windowOp := op.NewWindowAggOp(op.WindowSpecLite{}, keyFn, wa.PartitionBy, aggInit, agg)
		windowOp.OrderByCol = wa.OrderBy
		windowOp.FrameSpec = frameSpec

		return attachLogicalWindowAggInputWithContext(wa, windowOp, ctes)
	}

	// Fallback to GroupAggOp for simple aggregations without frame
	g := op.NewGroupAggOp(keyFn, aggInit, agg)
	if len(wa.PartitionBy) == 1 {
		g.SetKeyColName(wa.PartitionBy[0])
	}

	return attachLogicalWindowAggInputWithContext(wa, g, ctes)
}

func attachLogicalWindowAggInputWithContext(wa *LogicalWindowAgg, aggOp op.Operator, ctes map[string]*op.Node) (*op.Node, error) {
	if wa == nil || wa.Input == nil {
		return &op.Node{Op: aggOp}, nil
	}

	// Filter before window agg: chain filter MapOp then the window agg op.
	if f, ok := wa.Input.(*LogicalFilter); ok {
		predicateFn := BuildPredicateFunc(f.PredicateSQL)
		filterOp := &op.MapOp{F: func(td types.TupleDelta) []types.TupleDelta {
			if predicateFn(td.Tuple) {
				return []types.TupleDelta{td}
			}
			return nil
		}}

		inNode, err := logicalToDBSPWithContext(f.Input, ctes)
		if err != nil {
			return nil, err
		}
		chained := &op.ChainedOp{Ops: []op.Operator{filterOp, aggOp}}
		return &op.Node{Op: chained, Inputs: []*op.Node{inNode}}, nil
	}

	// Normal recursive transform for input.
	inNode, err := logicalToDBSPWithContext(wa.Input, ctes)
	if err != nil {
		return nil, err
	}

	return &op.Node{Op: aggOp, Inputs: []*op.Node{inNode}}, nil
}

// logicalJoinToDBSP transforms LogicalJoin to BinaryOp (JoinOp)
func logicalJoinToDBSPWithContext(join *LogicalJoin, ctes map[string]*op.Node) (*op.Node, error) {
	if len(join.Conditions) == 0 {
		return nil, fmt.Errorf("JOIN requires at least one join condition")
	}

	leftCols := make([]string, 0, len(join.Conditions))
	rightCols := make([]string, 0, len(join.Conditions))
	for _, c := range join.Conditions {
		leftCols = append(leftCols, c.LeftCol)
		rightCols = append(rightCols, c.RightCol)
	}

	encodeKey := func(values []any) any {
		b, err := json.Marshal(values)
		if err == nil {
			return string(b)
		}
		return fmt.Sprintf("%#v", values)
	}

	// Create key extraction functions for left and right.
	// NULL keys should not match (if any component is NULL, skip match).
	leftKeyFn := func(t types.Tuple) any {
		vals := make([]any, 0, len(leftCols))
		for _, col := range leftCols {
			v := t[col]
			if v == nil {
				return nil
			}
			vals = append(vals, v)
		}
		if len(vals) == 1 {
			return vals[0]
		}
		return encodeKey(vals)
	}

	rightKeyFn := func(t types.Tuple) any {
		vals := make([]any, 0, len(rightCols))
		for _, col := range rightCols {
			v := t[col]
			if v == nil {
				return nil
			}
			vals = append(vals, v)
		}
		if len(vals) == 1 {
			return vals[0]
		}
		return encodeKey(vals)
	}

	// Combine function merges left and right tuples
	combineFn := func(l, r types.Tuple) types.Tuple {
		result := make(types.Tuple)
		// Copy all columns from left
		for k, v := range l {
			result[k] = v
		}
		// Copy all columns from right
		for k, v := range r {
			result[k] = v
		}
		return result
	}

	// Create JoinOp
	joinOp := op.NewJoinOp(leftKeyFn, rightKeyFn, combineFn)

	// Transform left and right inputs
	leftNode, err := logicalToDBSPWithContext(join.Left, ctes)
	if err != nil {
		return nil, err
	}
	rightNode, err := logicalToDBSPWithContext(join.Right, ctes)
	if err != nil {
		return nil, err
	}

	return &op.Node{Op: joinOp, Inputs: []*op.Node{leftNode, rightNode}}, nil
}

// logicalSortToDBSP transforms LogicalSort to SortOp
func logicalSortToDBSPWithContext(sort *LogicalSort, ctes map[string]*op.Node) (*op.Node, error) {
	sortOp := op.NewSortOp(sort.OrderColumns, sort.Descending)
	if sort.Input == nil {
		return &op.Node{Op: sortOp}, nil
	}
	inNode, err := logicalToDBSPWithContext(sort.Input, ctes)
	if err != nil {
		return nil, err
	}
	return &op.Node{Op: sortOp, Inputs: []*op.Node{inNode}}, nil
}

// logicalLimitToDBSP transforms LogicalLimit to LimitOp
func logicalLimitToDBSPWithContext(limit *LogicalLimit, ctes map[string]*op.Node) (*op.Node, error) {
	limitOp := op.NewLimitOp(limit.Limit, limit.Offset)
	if limit.Input == nil {
		return &op.Node{Op: limitOp}, nil
	}
	inNode, err := logicalToDBSPWithContext(limit.Input, ctes)
	if err != nil {
		return nil, err
	}
	return &op.Node{Op: limitOp, Inputs: []*op.Node{inNode}}, nil
}
