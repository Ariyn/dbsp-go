package ir

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/ariyn/dbsp/internal/dbsp/op"
	"github.com/ariyn/dbsp/internal/dbsp/parse"
	"github.com/ariyn/dbsp/internal/dbsp/types"
)

var simpleColumnRefPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_.]*$`)

func isSimpleColumnRef(expr string) bool {
	expr = strings.TrimSpace(expr)
	if expr == "" || expr == "*" {
		return true
	}
	return simpleColumnRefPattern.MatchString(expr)
}

func buildGroupKeyFn(keys []string) func(types.Tuple) any {
	return buildGroupKeyFnWithWindow(keys, nil)
}

func buildGroupKeyFnWithWindow(keys []string, window *TimeWindowSpec) func(types.Tuple) any {
	if len(keys) == 0 && window == nil {
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

	// Prepare window bucket function if needed
	var timeCol string
	var size int64
	var timeExpr func(types.Tuple) (any, error)
	if window != nil {
		timeCol = window.TimeCol
		size = window.SizeMillis
		if strings.ContainsAny(timeCol, "()->:+-*/ ") {
			timeExpr = BuildExprFunc(timeCol)
		} else {
			timeExpr = func(t types.Tuple) (any, error) { return t[timeCol], nil }
		}
	}

	return func(t types.Tuple) any {
		values := make([]any, 0, len(keys)+1)
		for i := range keys {
			v, _ := exprFns[i](t)
			values = append(values, v)
		}

		var windowStart any
		if window != nil {
			v, _ := timeExpr(t)
			if v != nil {
				ts, err := toTime(v)
				if err == nil {
					millis := ts.UnixNano() / 1e6
					if size > 0 {
						windowStart = (millis / size) * size
					} else {
						windowStart = millis
					}
				} else {
					fmt.Printf("DEBUG: toTime failed for value %v (type %T): %v\n", v, v, err)
				}
			} else {
				fmt.Printf("DEBUG: timeExpr returned nil for column %s in tuple %v\n", window.TimeCol, t)
			}
		}
		if window != nil {
			values = append(values, windowStart)
		}
		return buildCompositeGroupKey(keys, values, window != nil)
	}
}

func buildCompositeGroupKey(keys []string, values []any, includeWindow bool) string {
	var b strings.Builder
	for idx, key := range keys {
		if idx > 0 {
			b.WriteByte('|')
		}
		b.WriteString(key)
		b.WriteByte('=')
		writeGroupKeyValue(&b, values[idx])
	}
	if includeWindow {
		if len(keys) > 0 {
			b.WriteByte('|')
		}
		b.WriteString("window_start=")
		writeGroupKeyValue(&b, values[len(values)-1])
	}
	return b.String()
}

func writeGroupKeyValue(b *strings.Builder, value any) {
	switch v := value.(type) {
	case nil:
		b.WriteString("null")
	case string:
		b.WriteString(v)
	case bool:
		b.WriteString(strconv.FormatBool(v))
	case int:
		b.WriteString(strconv.FormatInt(int64(v), 10))
	case int8:
		b.WriteString(strconv.FormatInt(int64(v), 10))
	case int16:
		b.WriteString(strconv.FormatInt(int64(v), 10))
	case int32:
		b.WriteString(strconv.FormatInt(int64(v), 10))
	case int64:
		b.WriteString(strconv.FormatInt(v, 10))
	case uint:
		b.WriteString(strconv.FormatUint(uint64(v), 10))
	case uint8:
		b.WriteString(strconv.FormatUint(uint64(v), 10))
	case uint16:
		b.WriteString(strconv.FormatUint(uint64(v), 10))
	case uint32:
		b.WriteString(strconv.FormatUint(uint64(v), 10))
	case uint64:
		b.WriteString(strconv.FormatUint(v, 10))
	case float32:
		b.WriteString(strconv.FormatFloat(float64(v), 'g', -1, 32))
	case float64:
		b.WriteString(strconv.FormatFloat(v, 'g', -1, 64))
	default:
		b.WriteString(fmt.Sprintf("%v", value))
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

func isBalancedAndOuter(s string) bool {
	return parse.IsBalancedAndOuter(s)
}

func containsKeywordOutsideParens(s, keyword string) bool {
	return parse.ContainsKeywordOutsideParens(s, keyword)
}

func splitByKeywordOutsideParens(s, keyword string) []string {
	return parse.SplitByKeyword(s, keyword)
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

// buildStringComparisonFunc builds a predicate for = and != operators.
// The cmp function receives (tupleVal, rhsString) and returns whether they match.
func buildStringComparisonFunc(predicateSQL, op string, cmp func(any, string) bool) func(types.Tuple) bool {
	parts := strings.SplitN(predicateSQL, op, 2)
	if len(parts) != 2 {
		return func(types.Tuple) bool { return true }
	}
	leftExpr := strings.TrimSpace(parts[0])
	val := strings.Trim(strings.TrimSpace(parts[1]), "'\"")
	exprFn := BuildExprFunc(leftExpr)
	return func(t types.Tuple) bool {
		v, err := exprFn(t)
		if err != nil || v == nil {
			return false
		}
		return cmp(v, val)
	}
}

// buildNumericComparisonFunc builds a predicate for >, >=, <, <= operators.
// The cmp function receives (tupleVal, threshold) and returns whether the comparison holds.
func buildNumericComparisonFunc(predicateSQL, op string, cmp func(float64, float64) bool) func(types.Tuple) bool {
	parts := strings.SplitN(predicateSQL, op, 2)
	if len(parts) != 2 {
		return func(types.Tuple) bool { return true }
	}
	leftExpr := strings.TrimSpace(parts[0])
	threshold, err := strconv.ParseFloat(strings.TrimSpace(parts[1]), 64)
	if err != nil {
		return func(types.Tuple) bool { return false }
	}
	exprFn := BuildExprFunc(leftExpr)
	return func(t types.Tuple) bool {
		v, err := exprFn(t)
		if err != nil || v == nil {
			return false
		}
		if f, ok := toFloat64Loose(v); ok {
			return cmp(f, threshold)
		}
		return false
	}
}

func buildEqualFunc(predicateSQL string) func(types.Tuple) bool {
	return buildStringComparisonFunc(predicateSQL, "=", func(v any, s string) bool { return compareEqual(v, s) })
}

func buildNotEqualFunc(predicateSQL string) func(types.Tuple) bool {
	return buildStringComparisonFunc(predicateSQL, "!=", func(v any, s string) bool { return !compareEqual(v, s) })
}

func buildGreaterFunc(predicateSQL string) func(types.Tuple) bool {
	return buildNumericComparisonFunc(predicateSQL, ">", func(v, t float64) bool { return v > t })
}

func buildGreaterEqualFunc(predicateSQL string) func(types.Tuple) bool {
	return buildNumericComparisonFunc(predicateSQL, ">=", func(v, t float64) bool { return v >= t })
}

func buildLessFunc(predicateSQL string) func(types.Tuple) bool {
	return buildNumericComparisonFunc(predicateSQL, "<", func(v, t float64) bool { return v < t })
}

func buildLessEqualFunc(predicateSQL string) func(types.Tuple) bool {
	return buildNumericComparisonFunc(predicateSQL, "<=", func(v, t float64) bool { return v <= t })
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
	return types.ToFloat64Safe(v)
}

// LogicalToDBSP transforms a LogicalNode into a runtime DBSP operator Node.
// For Phase1 it supports LogicalScan -> LogicalFilter -> LogicalProject -> LogicalGroupAgg pattern.
func LogicalToDBSP(l LogicalNode) (*op.Node, error) {
	return logicalToDBSPWithContext(l, make(map[string]*op.Node))
}

// logicalGroupAggToDBSPWithContext transforms LogicalGroupAgg to a GroupAgg or WindowAgg operator.
func logicalGroupAggToDBSPWithContext(n *LogicalGroupAgg, ctes map[string]*op.Node) (*op.Node, error) {
	keyFn := buildGroupKeyFnWithWindow(n.Keys, n.TimeWindowSpec)

	var aggOp op.Operator
	if len(n.Aggs) > 0 {
		aggSlots := make([]op.AggSlot, 0, len(n.Aggs))
		for _, a := range n.Aggs {
			name := strings.ToUpper(a.Name)
			deltaCol := strings.TrimSpace(a.As)
			slot, err := buildAggSlot(name, a.Col, deltaCol)
			if err != nil {
				return nil, fmt.Errorf("unsupported agg %s in multi-aggregate", a.Name)
			}
			aggSlots = append(aggSlots, slot)
		}
		g := op.NewGroupAggMultiOp(keyFn, aggSlots)
		g.SetGroupKeyColNames(n.Keys)
		g.EmitValue = true
		if n.TimeWindowSpec != nil {
			g.TimeWindowSpec = op.WindowSpecLite{
				TimeCol:     n.TimeWindowSpec.TimeCol,
				SizeMillis:  n.TimeWindowSpec.SizeMillis,
				WindowType:  op.WindowTypeTumbling, // Default for now
				SlideMillis: n.TimeWindowSpec.SlideMillis,
				GapMillis:   n.TimeWindowSpec.GapMillis,
			}
		}
		aggOp = g
	} else {
		agg, aggInit, err := buildSingleAggFunc(strings.ToUpper(n.AggName), n.AggCol)
		if err != nil {
			return nil, err
		}
		if n.TimeWindowSpec != nil {
			ws := n.TimeWindowSpec
			aggOp = op.NewWindowAggOp(op.WindowSpecLite{
				TimeCol:    ws.TimeCol,
				SizeMillis: ws.SizeMillis,
				WindowType: op.WindowTypeTumbling,
			}, keyFn, n.Keys, aggInit, agg)
		} else if n.WindowSpec != nil {
			ws := n.WindowSpec
			aggOp = op.NewWindowAggOp(op.WindowSpecLite{TimeCol: ws.TimeCol, SizeMillis: ws.SizeMillis}, keyFn, n.Keys, aggInit, agg)
		} else {
			g := op.NewGroupAggOp(keyFn, aggInit, agg)
			g.SetGroupKeyColNames(n.Keys)
			g.EmitValue = true
			aggOp = g
		}
	}

	return attachLogicalGroupAggInputWithContext(n, aggOp, ctes)
}

// buildSingleAggFunc constructs an AggFunc and its init function for a single aggregate.
func buildSingleAggFunc(name, col string) (op.AggFunc, func() any, error) {
	switch name {
	case "SUM":
		s := &op.SumAgg{ColName: col}
		if !isSimpleColumnRef(col) {
			s.Expr = BuildExprFunc(col)
		}
		return s, func() any { return float64(0) }, nil
	case "COUNT":
		c := &op.CountAgg{ColName: col}
		if col != "" && !isSimpleColumnRef(col) {
			c.Expr = BuildExprFunc(col)
		}
		return c, func() any { return int64(0) }, nil
	case "AVG":
		av := &op.AvgAgg{ColName: col}
		if !isSimpleColumnRef(col) {
			av.Expr = BuildExprFunc(col)
		}
		return av, func() any { return nil }, nil
	case "MIN":
		return &op.MinAgg{ColName: col}, func() any { return op.NewSortedMultiset() }, nil
	case "MAX":
		return &op.MaxAgg{ColName: col}, func() any { return op.NewSortedMultiset() }, nil
	default:
		return nil, nil, fmt.Errorf("unsupported agg %s", name)
	}
}

// buildAggSlot constructs an AggSlot for multi-aggregate operators.
func buildAggSlot(name, col, deltaCol string) (op.AggSlot, error) {
	switch name {
	case "SUM":
		s := &op.SumAgg{ColName: col, DeltaCol: deltaCol}
		if !isSimpleColumnRef(col) {
			s.Expr = BuildExprFunc(col)
		}
		return op.AggSlot{Init: func() any { return float64(0) }, Fn: s}, nil
	case "AVG":
		avg := &op.AvgAgg{ColName: col, DeltaCol: deltaCol}
		if !isSimpleColumnRef(col) {
			avg.Expr = BuildExprFunc(col)
		}
		return op.AggSlot{Init: func() any { return op.AvgMonoid{} }, Fn: avg}, nil
	case "COUNT":
		c := &op.CountAgg{ColName: col, DeltaCol: deltaCol}
		if col != "" && !isSimpleColumnRef(col) {
			c.Expr = BuildExprFunc(col)
		}
		return op.AggSlot{Init: func() any { return int64(0) }, Fn: c}, nil
	default:
		return op.AggSlot{}, fmt.Errorf("unsupported agg %s", name)
	}
}

// attachInputToAgg attaches an input LogicalNode to an aggregate operator.
// If the input is a LogicalFilter, the filter is chained before the aggregate.
func attachInputToAgg(input LogicalNode, aggOp op.Operator, ctes map[string]*op.Node) (*op.Node, error) {
	if input == nil {
		return &op.Node{Op: aggOp}, nil
	}
	if f, ok := input.(*LogicalFilter); ok {
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
		return &op.Node{Op: &op.ChainedOp{Ops: []op.Operator{filterOp, aggOp}}, Inputs: []*op.Node{inNode}}, nil
	}
	inNode, err := logicalToDBSPWithContext(input, ctes)
	if err != nil {
		return nil, err
	}
	return &op.Node{Op: aggOp, Inputs: []*op.Node{inNode}}, nil
}

func attachLogicalGroupAggInputWithContext(n *LogicalGroupAgg, aggOp op.Operator, ctes map[string]*op.Node) (*op.Node, error) {
	if n == nil {
		return &op.Node{Op: aggOp}, nil
	}
	return attachInputToAgg(n.Input, aggOp, ctes)
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
		if len(n.Exprs) == 0 && !n.KeepInput {
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
			projectOp = &op.ProjectOp{Columns: columns, Exprs: exprs, KeepInput: n.KeepInput}
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
		return logicalGroupAggToDBSPWithContext(n, ctes)

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

// buildPartitionKeyFn builds a key function over cols for partitioned window operators.
// Zero cols → global partition (nil key). One col → direct value. Multiple → composite string.
func buildPartitionKeyFn(cols []string) func(types.Tuple) any {
	switch len(cols) {
	case 0:
		return func(types.Tuple) any { return nil }
	case 1:
		col := cols[0]
		return func(t types.Tuple) any { return t[col] }
	default:
		partCols := append([]string(nil), cols...)
		return func(t types.Tuple) any {
			key := make([]any, len(partCols))
			for i, c := range partCols {
				key[i] = t[c]
			}
			return fmt.Sprintf("%v", key)
		}
	}
}

// logicalWindowFuncToDBSP transforms LogicalWindowFunc to DBSP operators
func logicalWindowFuncToDBSPWithContext(wf *LogicalWindowFunc, ctes map[string]*op.Node) (*op.Node, error) {
	grouped, input := collectSharedWindowFuncs(wf)
	for _, current := range grouped {
		if current.Spec.FuncName != "LAG" {
			return nil, fmt.Errorf("only LAG window function is currently supported, got %s", current.Spec.FuncName)
		}
		if len(current.Spec.Args) == 0 {
			return nil, fmt.Errorf("LAG requires at least one argument")
		}
	}

	primary := grouped[0]
	lagCol := primary.Spec.Args[0]
	keyFn := buildPartitionKeyFn(primary.Spec.PartitionBy)
	ordered := op.NewOrderedWindowOp(keyFn, primary.Spec.OrderBy, lagCol, primary.Spec.Offset, primary.OutputCol)
	ordered.PartitionCols = append([]string(nil), primary.Spec.PartitionBy...)
	if strings.ContainsAny(lagCol, "()->:+-*/ ") {
		ordered.LagExpr = BuildExprFunc(lagCol)
		ordered.LagOutputs[0].LagExpr = ordered.LagExpr
	}
	for _, current := range grouped[1:] {
		currentLagCol := current.Spec.Args[0]
		var lagExpr func(types.Tuple) (any, error)
		if strings.ContainsAny(currentLagCol, "()->:+-*/ ") {
			lagExpr = BuildExprFunc(currentLagCol)
		}
		ordered.AddLagOutput(currentLagCol, lagExpr, current.OutputCol)
	}

	// Check if there's an input node
	if input != nil {
		inNode, err := logicalToDBSPWithContext(input, ctes)
		if err != nil {
			return nil, err
		}
		return &op.Node{Op: ordered, Inputs: []*op.Node{inNode}}, nil
	}

	return &op.Node{Op: ordered}, nil
}

func collectSharedWindowFuncs(root *LogicalWindowFunc) ([]*LogicalWindowFunc, LogicalNode) {
	if root == nil {
		return nil, nil
	}
	grouped := make([]*LogicalWindowFunc, 0, 4)
	produced := make(map[string]struct{})
	current := root
	for current != nil {
		if len(grouped) > 0 && !windowFuncExecutionSpecEqual(grouped[0].Spec, current.Spec) {
			break
		}
		if currentDependsOnPriorWindowOutput(current, produced) {
			break
		}
		grouped = append(grouped, current)
		if strings.TrimSpace(current.OutputCol) != "" {
			produced[strings.TrimSpace(current.OutputCol)] = struct{}{}
		}
		next, ok := current.Input.(*LogicalWindowFunc)
		if !ok {
			break
		}
		current = next
	}
	input := grouped[len(grouped)-1].Input
	for left, right := 0, len(grouped)-1; left < right; left, right = left+1, right-1 {
		grouped[left], grouped[right] = grouped[right], grouped[left]
	}
	return grouped, input
}

func windowFuncExecutionSpecEqual(a, b WindowFuncSpec) bool {
	if strings.ToUpper(strings.TrimSpace(a.FuncName)) != strings.ToUpper(strings.TrimSpace(b.FuncName)) {
		return false
	}
	if strings.TrimSpace(a.OrderBy) != strings.TrimSpace(b.OrderBy) {
		return false
	}
	if a.Offset != b.Offset {
		return false
	}
	if len(a.PartitionBy) != len(b.PartitionBy) {
		return false
	}
	for idx := range a.PartitionBy {
		if strings.TrimSpace(a.PartitionBy[idx]) != strings.TrimSpace(b.PartitionBy[idx]) {
			return false
		}
	}
	return true
}

func currentDependsOnPriorWindowOutput(wf *LogicalWindowFunc, produced map[string]struct{}) bool {
	if wf == nil || len(wf.Spec.Args) == 0 || len(produced) == 0 {
		return false
	}
	_, ok := produced[strings.TrimSpace(wf.Spec.Args[0])]
	return ok
}

// logicalWindowAggToDBSP transforms LogicalWindowAgg (DuckDB standard window aggregate) to DBSP operators
func logicalWindowAggToDBSPWithContext(wa *LogicalWindowAgg, ctes map[string]*op.Node) (*op.Node, error) {
	keyFn := buildPartitionKeyFn(wa.PartitionBy)

	agg, aggInit, err := buildSingleAggFunc(strings.ToUpper(wa.AggName), wa.AggCol)
	if err != nil {
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
		windowOp.KeepInput = true
		windowOp.EmitValue = true
		return attachLogicalWindowAggInputWithContext(wa, windowOp, ctes)
	}

	// For SQL window aggregates with ORDER BY, use frame-based WindowAggOp.
	// If frame spec is omitted, default to cumulative frame:
	// ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW.
	if wa.OrderBy != "" {
		frameSpec := &op.FrameSpecLite{
			Type:      "ROWS",
			StartType: "UNBOUNDED PRECEDING",
			EndType:   "CURRENT ROW",
		}
		if wa.FrameSpec != nil {
			frameSpec = &op.FrameSpecLite{
				Type:       wa.FrameSpec.Type,
				StartType:  wa.FrameSpec.StartType,
				StartValue: wa.FrameSpec.StartValue,
				EndType:    wa.FrameSpec.EndType,
				EndValue:   wa.FrameSpec.EndValue,
			}
		}

		windowOp := op.NewWindowAggOp(op.WindowSpecLite{}, keyFn, wa.PartitionBy, aggInit, agg)
		windowOp.OrderByCol = wa.OrderBy
		windowOp.FrameSpec = frameSpec
		windowOp.KeepInput = true
		windowOp.EmitValue = true

		node, err := attachLogicalWindowAggInputWithContext(wa, windowOp, ctes)
		if err != nil {
			return nil, err
		}
		return wrapWindowAggOutputAlias(node, wa.OutputCol), nil
	}

	// Fallback to GroupAggOp for simple aggregations without frame
	g := op.NewGroupAggOp(keyFn, aggInit, agg)
	if len(wa.PartitionBy) == 1 {
		g.SetKeyColName(wa.PartitionBy[0])
	}

	node, err := attachLogicalWindowAggInputWithContext(wa, g, ctes)
	if err != nil {
		return nil, err
	}
	return wrapWindowAggOutputAlias(node, wa.OutputCol), nil
}

func wrapWindowAggOutputAlias(node *op.Node, outputCol string) *op.Node {
	if node == nil || strings.TrimSpace(outputCol) == "" {
		return node
	}
	alias := strings.TrimSpace(outputCol)
	aliasOp := &op.MapOp{F: func(td types.TupleDelta) []types.TupleDelta {
		tuple := td.Tuple
		if tuple == nil {
			tuple = types.Tuple{}
			td.Tuple = tuple
		}
		if v, ok := tuple["agg_result"]; ok {
			tuple[alias] = v
		} else if v, ok := tuple["agg_delta"]; ok {
			tuple[alias] = v
		} else if v, ok := tuple["avg_delta"]; ok {
			tuple[alias] = v
		} else if v, ok := tuple["count_delta"]; ok {
			tuple[alias] = v
		} else if v, ok := tuple["min"]; ok {
			tuple[alias] = v
		} else if v, ok := tuple["max"]; ok {
			tuple[alias] = v
		}
		return []types.TupleDelta{td}
	}}
	return &op.Node{Op: &op.ChainedOp{Ops: []op.Operator{node.Op, aliasOp}}, Inputs: node.Inputs, Source: node.Source, PartitionBy: node.PartitionBy}
}

func attachLogicalWindowAggInputWithContext(wa *LogicalWindowAgg, aggOp op.Operator, ctes map[string]*op.Node) (*op.Node, error) {
	if wa == nil {
		return &op.Node{Op: aggOp}, nil
	}
	return attachInputToAgg(wa.Input, aggOp, ctes)
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
