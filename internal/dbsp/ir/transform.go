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
	if len(keys) == 1 {
		key := keys[0]
		return func(t types.Tuple) any { return t[key] }
	}
	keyCols := append([]string(nil), keys...)
	return func(t types.Tuple) any {
		kt := make(types.Tuple, len(keyCols))
		for _, col := range keyCols {
			kt[col] = t[col]
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
	parts := strings.Split(strings.ToUpper(predicateSQL), " IS NULL")
	if len(parts) < 1 {
		return func(t types.Tuple) bool { return false }
	}
	// Extract original column name (preserve case)
	idx := strings.Index(strings.ToUpper(predicateSQL), " IS NULL")
	col := strings.TrimSpace(predicateSQL[:idx])

	return func(t types.Tuple) bool {
		val, exists := t[col]
		return !exists || val == nil
	}
}

// buildIsNotNullFunc handles "column IS NOT NULL"
func buildIsNotNullFunc(predicateSQL string) func(types.Tuple) bool {
	parts := strings.Split(strings.ToUpper(predicateSQL), " IS NOT NULL")
	if len(parts) < 1 {
		return func(t types.Tuple) bool { return false }
	}
	// Extract original column name (preserve case)
	idx := strings.Index(strings.ToUpper(predicateSQL), " IS NOT NULL")
	col := strings.TrimSpace(predicateSQL[:idx])

	return func(t types.Tuple) bool {
		val, exists := t[col]
		return exists && val != nil
	}
}

// buildEqualFunc handles "column = value"
func buildEqualFunc(predicateSQL string) func(types.Tuple) bool {
	parts := strings.Split(predicateSQL, "=")
	if len(parts) != 2 {
		return func(t types.Tuple) bool { return true }
	}

	col := strings.TrimSpace(parts[0])
	val := strings.TrimSpace(parts[1])
	val = strings.Trim(val, "'\"")

	return func(t types.Tuple) bool {
		tupleVal, ok := t[col]
		// NULL values should not match in equality comparisons
		if !ok || tupleVal == nil {
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

	col := strings.TrimSpace(parts[0])
	val := strings.TrimSpace(parts[1])
	val = strings.Trim(val, "'\"")

	return func(t types.Tuple) bool {
		tupleVal, ok := t[col]
		// NULL values should not match in inequality comparisons
		if !ok || tupleVal == nil {
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

	col := strings.TrimSpace(parts[0])
	valStr := strings.TrimSpace(parts[1])
	threshold, err := strconv.ParseFloat(valStr, 64)
	if err != nil {
		return func(t types.Tuple) bool { return false }
	}

	return func(t types.Tuple) bool {
		tupleVal, ok := t[col]
		// NULL values should not match in comparisons
		if !ok || tupleVal == nil {
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

	col := strings.TrimSpace(parts[0])
	valStr := strings.TrimSpace(parts[1])
	threshold, err := strconv.ParseFloat(valStr, 64)
	if err != nil {
		return func(t types.Tuple) bool { return false }
	}

	return func(t types.Tuple) bool {
		tupleVal, ok := t[col]
		// NULL values should not match in comparisons
		if !ok || tupleVal == nil {
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

	col := strings.TrimSpace(parts[0])
	valStr := strings.TrimSpace(parts[1])
	threshold, err := strconv.ParseFloat(valStr, 64)
	if err != nil {
		return func(t types.Tuple) bool { return false }
	}

	return func(t types.Tuple) bool {
		tupleVal, ok := t[col]
		// NULL values should not match in comparisons
		if !ok || tupleVal == nil {
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

	col := strings.TrimSpace(parts[0])
	valStr := strings.TrimSpace(parts[1])
	threshold, err := strconv.ParseFloat(valStr, 64)
	if err != nil {
		return func(t types.Tuple) bool { return false }
	}

	return func(t types.Tuple) bool {
		tupleVal, ok := t[col]
		// NULL values should not match in comparisons
		if !ok || tupleVal == nil {
			return false
		}
		return compareLessOrEqual(tupleVal, threshold)
	}
}

// Helper comparison functions
func compareEqual(tupleVal any, val string) bool {
	// String comparison
	if strVal, ok := tupleVal.(string); ok {
		return strVal == val
	}
	// Numeric comparison
	if numVal, err := strconv.ParseFloat(val, 64); err == nil {
		switch tv := tupleVal.(type) {
		case int:
			return float64(tv) == numVal
		case int64:
			return float64(tv) == numVal
		case float64:
			return tv == numVal
		}
	}
	return fmt.Sprintf("%v", tupleVal) == val
}

func compareGreater(tupleVal any, threshold float64) bool {
	switch tv := tupleVal.(type) {
	case int:
		return float64(tv) > threshold
	case int64:
		return float64(tv) > threshold
	case float64:
		return tv > threshold
	}
	return false
}

func compareGreaterOrEqual(tupleVal any, threshold float64) bool {
	switch tv := tupleVal.(type) {
	case int:
		return float64(tv) >= threshold
	case int64:
		return float64(tv) >= threshold
	case float64:
		return tv >= threshold
	}
	return false
}

func compareLess(tupleVal any, threshold float64) bool {
	switch tv := tupleVal.(type) {
	case int:
		return float64(tv) < threshold
	case int64:
		return float64(tv) < threshold
	case float64:
		return tv < threshold
	}
	return false
}

func compareLessOrEqual(tupleVal any, threshold float64) bool {
	switch tv := tupleVal.(type) {
	case int:
		return float64(tv) <= threshold
	case int64:
		return float64(tv) <= threshold
	case float64:
		return tv <= threshold
	}
	return false
}

// LogicalToDBSP transforms a LogicalNode into a runtime DBSP operator Node.
// For Phase1 it supports LogicalScan -> LogicalFilter -> LogicalProject -> LogicalGroupAgg pattern.
func LogicalToDBSP(l LogicalNode) (*op.Node, error) {
	switch n := l.(type) {
	case *LogicalProject:
		// Transform projection to MapOp that filters columns
		columns := n.Columns
		projectOp := &op.MapOp{
			F: func(td types.TupleDelta) []types.TupleDelta {
				// Create new tuple with only selected columns
				projected := make(types.Tuple)
				for _, col := range columns {
					if val, ok := td.Tuple[col]; ok {
						projected[col] = val
					}
				}
				return []types.TupleDelta{{Tuple: projected, Count: td.Count}}
			},
		}

		// Check if input needs processing
		if n.Input != nil {
			// Recursively transform input first
			switch in := n.Input.(type) {
			case *LogicalFilter:
				// Build filter predicate
				predicateFn := BuildPredicateFunc(in.PredicateSQL)
				filterOp := &op.MapOp{
					F: func(td types.TupleDelta) []types.TupleDelta {
						if predicateFn(td.Tuple) {
							return []types.TupleDelta{td}
						}
						return nil
					},
				}

				// Check if filter has JOIN as input
				if join, ok := in.Input.(*LogicalJoin); ok {
					// Transform JOIN
					joinNode, err := logicalJoinToDBSP(join)
					if err != nil {
						return nil, err
					}
					// Unary pipeline applied on top of JOIN output
					chainedOp := &op.ChainedOp{Ops: []op.Operator{filterOp, projectOp}}
					return &op.Node{Op: chainedOp, Inputs: []*op.Node{joinNode}}, nil
				}

				// Chain: filter first, then project
				return &op.Node{Op: &op.ChainedOp{Ops: []op.Operator{filterOp, projectOp}}}, nil

			case *LogicalJoin:
				// Transform JOIN and chain with project
				joinNode, err := logicalJoinToDBSP(in)
				if err != nil {
					return nil, err
				}
				chainedOp := &op.ChainedOp{Ops: []op.Operator{projectOp}}
				return &op.Node{Op: chainedOp, Inputs: []*op.Node{joinNode}}, nil
			}
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
		// For now, we don't chain inputs (single-op nodes)
		return &op.Node{Op: mapOp}, nil

	case *LogicalGroupAgg:
		// Expect input to be scan or filter
		switch in := n.Input.(type) {
		case *LogicalScan:
			// Direct scan input
			if n.WindowSpec == nil {
				// Non-windowed grouping: support composite keys.
				keyFn := buildGroupKeyFn(n.Keys)

				// Multi-aggregate path (Phase A): SUM(col) + COUNT(col)
				if len(n.Aggs) > 0 {
					aggSlots := make([]op.AggSlot, 0, len(n.Aggs))
					for _, a := range n.Aggs {
						name := strings.ToUpper(a.Name)
						switch name {
						case "SUM":
							aggSlots = append(aggSlots, op.AggSlot{
								Init: func() any { return float64(0) },
								Fn:   &op.SumAgg{ColName: a.Col, DeltaCol: "agg_delta"},
							})
						case "COUNT":
							if a.Col == "" {
								return nil, fmt.Errorf("COUNT(*) cannot be combined with other aggregates yet")
							}
							aggSlots = append(aggSlots, op.AggSlot{
								Init: func() any { return int64(0) },
								Fn:   &op.CountAgg{ColName: a.Col, DeltaCol: "count_delta"},
							})
						default:
							return nil, fmt.Errorf("unsupported agg %s in multi-aggregate", a.Name)
						}
					}
					g := op.NewGroupAggMultiOp(keyFn, aggSlots)
					g.SetGroupKeyColNames(n.Keys)
					return &op.Node{Op: g}, nil
				}

				var agg op.AggFunc
				var aggInit func() any
				switch n.AggName {
				case "SUM", "sum":
					agg = &op.SumAgg{ColName: n.AggCol}
					aggInit = func() any { return float64(0) }
				case "COUNT", "count":
					agg = &op.CountAgg{ColName: n.AggCol}
					aggInit = func() any { return int64(0) }
				case "MIN", "min":
					agg = &op.MinAgg{ColName: n.AggCol}
					aggInit = func() any { return op.NewSortedMultiset() }
				case "MAX", "max":
					agg = &op.MaxAgg{ColName: n.AggCol}
					aggInit = func() any { return op.NewSortedMultiset() }
				default:
					return nil, fmt.Errorf("unsupported agg %s", n.AggName)
				}

				g := op.NewGroupAggOp(keyFn, aggInit, agg)
				g.SetGroupKeyColNames(n.Keys)
				return &op.Node{Op: g}, nil
			} // Windowed aggregation: use WindowAggOp so that each input delta
			// Multi-aggregate windowed grouping is not supported yet.
			if len(n.Aggs) > 0 {
				return nil, fmt.Errorf("multi-aggregate windowed GROUP BY not supported yet")
			}
			// only affects its corresponding window(s) and group key.
			ws := n.WindowSpec
			if ws == nil {
				return nil, fmt.Errorf("windowSpec must not be nil in windowed branch")
			}

			// If there are group keys, we currently support a single key column
			// in addition to the window.
			groupKeyFn := buildGroupKeyFn(n.Keys)

			var agg op.AggFunc
			var aggInit func() any
			switch n.AggName {
			case "SUM", "sum":
				agg = &op.SumAgg{ColName: n.AggCol}
				aggInit = func() any { return float64(0) }
			case "COUNT", "count":
				agg = &op.CountAgg{ColName: n.AggCol}
				aggInit = func() any { return int64(0) }
			case "AVG", "avg":
				agg = &op.AvgAgg{ColName: n.AggCol}
				aggInit = func() any { return nil }
			case "MIN", "min":
				agg = &op.MinAgg{ColName: n.AggCol}
				aggInit = func() any { return op.NewSortedMultiset() }
			case "MAX", "max":
				agg = &op.MaxAgg{ColName: n.AggCol}
				aggInit = func() any { return op.NewSortedMultiset() }
			default:
				return nil, fmt.Errorf("unsupported agg %s", n.AggName)
			}

			waSpec := op.WindowSpecLite{
				TimeCol:    ws.TimeCol,
				SizeMillis: ws.SizeMillis,
			}
			g := op.NewWindowAggOp(waSpec, groupKeyFn, n.Keys, aggInit, agg)
			return &op.Node{Op: g}, nil

		case *LogicalFilter:
			// Filter before GroupAgg - create chained MapOp
			if n.WindowSpec == nil {
				// Non-windowed grouping: support composite keys.
				keyFn := buildGroupKeyFn(n.Keys)

				// Multi-aggregate path (Phase A): SUM(col) + COUNT(col)
				if len(n.Aggs) > 0 {
					aggSlots := make([]op.AggSlot, 0, len(n.Aggs))
					for _, a := range n.Aggs {
						name := strings.ToUpper(a.Name)
						switch name {
						case "SUM":
							aggSlots = append(aggSlots, op.AggSlot{Init: func() any { return float64(0) }, Fn: &op.SumAgg{ColName: a.Col, DeltaCol: "agg_delta"}})
						case "COUNT":
							if a.Col == "" {
								return nil, fmt.Errorf("COUNT(*) cannot be combined with other aggregates yet")
							}
							aggSlots = append(aggSlots, op.AggSlot{Init: func() any { return int64(0) }, Fn: &op.CountAgg{ColName: a.Col, DeltaCol: "count_delta"}})
						default:
							return nil, fmt.Errorf("unsupported agg %s in multi-aggregate", a.Name)
						}
					}

					predicateFn := BuildPredicateFunc(in.PredicateSQL)
					g := op.NewGroupAggMultiOp(keyFn, aggSlots)
					g.SetGroupKeyColNames(n.Keys)
					filterOp := &op.MapOp{
						F: func(td types.TupleDelta) []types.TupleDelta {
							if predicateFn(td.Tuple) {
								return []types.TupleDelta{td}
							}
							return nil
						},
					}

					if join, ok := in.Input.(*LogicalJoin); ok {
						joinNode, err := logicalJoinToDBSP(join)
						if err != nil {
							return nil, err
						}
						chainedOp := &op.ChainedOp{Ops: []op.Operator{filterOp, g}}
						return &op.Node{Op: chainedOp, Inputs: []*op.Node{joinNode}}, nil
					}

					chainedOp := &op.ChainedOp{Ops: []op.Operator{filterOp, g}}
					return &op.Node{Op: chainedOp}, nil
				}

				var agg op.AggFunc
				var aggInit func() any
				switch n.AggName {
				case "SUM", "sum":
					agg = &op.SumAgg{ColName: n.AggCol}
					aggInit = func() any { return float64(0) }
				case "COUNT", "count":
					agg = &op.CountAgg{ColName: n.AggCol}
					aggInit = func() any { return int64(0) }
				case "AVG", "avg":
					agg = &op.AvgAgg{ColName: n.AggCol}
					aggInit = func() any { return nil }
				case "MIN", "min":
					agg = &op.MinAgg{ColName: n.AggCol}
					aggInit = func() any { return op.NewSortedMultiset() }
				case "MAX", "max":
					agg = &op.MaxAgg{ColName: n.AggCol}
					aggInit = func() any { return op.NewSortedMultiset() }
				default:
					return nil, fmt.Errorf("unsupported agg %s", n.AggName)
				}

				// Create filter function
				predicateFn := BuildPredicateFunc(in.PredicateSQL)
				g := op.NewGroupAggOp(keyFn, aggInit, agg)
				g.SetGroupKeyColNames(n.Keys)

				filterOp := &op.MapOp{
					F: func(td types.TupleDelta) []types.TupleDelta {
						if predicateFn(td.Tuple) {
							return []types.TupleDelta{td}
						}
						return nil
					},
				}

				// If filter is applied on top of a JOIN, JOIN must be a real 2-input node.
				// Wire it as: joinNode -> (filter then aggregate)
				if join, ok := in.Input.(*LogicalJoin); ok {
					joinNode, err := logicalJoinToDBSP(join)
					if err != nil {
						return nil, err
					}
					chainedOp := &op.ChainedOp{Ops: []op.Operator{filterOp, g}}
					return &op.Node{Op: chainedOp, Inputs: []*op.Node{joinNode}}, nil
				}

				// Default: filter then aggregate
				chainedOp := &op.ChainedOp{Ops: []op.Operator{filterOp, g}}
				return &op.Node{Op: chainedOp}, nil
			}

			// Windowed aggregation with a filter.
			ws := n.WindowSpec
			if ws == nil {
				return nil, fmt.Errorf("windowSpec must not be nil in windowed filter branch")
			}
			groupKeyFn := buildGroupKeyFn(n.Keys)

			var agg op.AggFunc
			var aggInit func() any
			switch n.AggName {
			case "SUM", "sum":
				agg = &op.SumAgg{ColName: n.AggCol}
				aggInit = func() any { return float64(0) }
			case "COUNT", "count":
				agg = &op.CountAgg{ColName: n.AggCol}
				aggInit = func() any { return int64(0) }
			case "AVG", "avg":
				agg = &op.AvgAgg{ColName: n.AggCol}
				aggInit = func() any { return op.AvgMonoid{} }
			case "MIN", "min":
				agg = &op.MinAgg{ColName: n.AggCol}
				aggInit = func() any { return op.NewSortedMultiset() }
			case "MAX", "max":
				agg = &op.MaxAgg{ColName: n.AggCol}
				aggInit = func() any { return op.NewSortedMultiset() }
			default:
				return nil, fmt.Errorf("unsupported agg %s", n.AggName)
			}

			// Create filter function
			predicateFn := BuildPredicateFunc(in.PredicateSQL)

			waSpec := op.WindowSpecLite{TimeCol: ws.TimeCol, SizeMillis: ws.SizeMillis}
			wa := op.NewWindowAggOp(waSpec, groupKeyFn, n.Keys, aggInit, agg)
			filterOp := &op.MapOp{
				F: func(td types.TupleDelta) []types.TupleDelta {
					if predicateFn(td.Tuple) {
						return []types.TupleDelta{td}
					}
					return nil
				},
			}

			// Create a ChainedOp that applies filter then window aggregate
			chainedOp := &op.ChainedOp{
				Ops: []op.Operator{filterOp, wa},
			}

			return &op.Node{Op: chainedOp}, nil

		case *LogicalJoin:
			// JOIN followed by GroupAgg
			keyFn := buildGroupKeyFn(n.Keys)

			var agg op.AggFunc
			var aggInit func() any
			switch n.AggName {
			case "SUM", "sum":
				agg = &op.SumAgg{ColName: n.AggCol}
				aggInit = func() any { return float64(0) }
			case "COUNT", "count":
				agg = &op.CountAgg{ColName: n.AggCol}
				aggInit = func() any { return int64(0) }
			case "AVG", "avg":
				agg = &op.AvgAgg{ColName: n.AggCol}
				aggInit = func() any { return op.AvgMonoid{} }
			case "MIN", "min":
				agg = &op.MinAgg{ColName: n.AggCol}
				aggInit = func() any { return op.NewSortedMultiset() }
			case "MAX", "max":
				agg = &op.MaxAgg{ColName: n.AggCol}
				aggInit = func() any { return op.NewSortedMultiset() }
			default:
				return nil, fmt.Errorf("unsupported agg %s", n.AggName)
			}

			g := op.NewGroupAggOp(keyFn, aggInit, agg)
			g.SetGroupKeyColNames(n.Keys)

			joinNode, err := logicalJoinToDBSP(in)
			if err != nil {
				return nil, err
			}
			// Unary aggregate applied on top of JOIN output
			chainedOp := &op.ChainedOp{Ops: []op.Operator{g}}
			return &op.Node{Op: chainedOp, Inputs: []*op.Node{joinNode}}, nil

		default:
			return nil, fmt.Errorf("unsupported input node to GroupAgg: %T", in)
		}

	case *LogicalWindowFunc:
		// Transform window function to appropriate operator
		return logicalWindowFuncToDBSP(n)

	case *LogicalWindowAgg:
		// Transform window aggregate function to appropriate operator
		return logicalWindowAggToDBSP(n)

	case *LogicalJoin:
		// Transform JOIN to BinaryOp
		return logicalJoinToDBSP(n)

	case *LogicalSort:
		// Transform ORDER BY to SortOp
		return logicalSortToDBSP(n)

	case *LogicalLimit:
		// Transform LIMIT to LimitOp
		return logicalLimitToDBSP(n)

	default:
		return nil, fmt.Errorf("unsupported logical node: %T", n)
	}
}

// logicalWindowFuncToDBSP transforms LogicalWindowFunc to DBSP operators
func logicalWindowFuncToDBSP(wf *LogicalWindowFunc) (*op.Node, error) {
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
		// For now, we return the operator directly
		// In a full implementation, we'd chain with the input
		return &op.Node{Op: g}, nil
	}

	return &op.Node{Op: g}, nil
}

// logicalWindowAggToDBSP transforms LogicalWindowAgg (DuckDB standard window aggregate) to DBSP operators
func logicalWindowAggToDBSP(wa *LogicalWindowAgg) (*op.Node, error) {
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
		agg = &op.SumAgg{ColName: wa.AggCol}
		aggInit = func() any { return float64(0) }
	case "AVG":
		agg = &op.AvgAgg{ColName: wa.AggCol}
		aggInit = func() any { return op.AvgMonoid{} }
	case "COUNT":
		agg = &op.CountAgg{}
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
		return &op.Node{Op: windowOp}, nil
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

		return &op.Node{Op: windowOp}, nil
	}

	// Fallback to GroupAggOp for simple aggregations without frame
	g := op.NewGroupAggOp(keyFn, aggInit, agg)
	if len(wa.PartitionBy) == 1 {
		g.SetKeyColName(wa.PartitionBy[0])
	}

	return &op.Node{Op: g}, nil
}

// logicalJoinToDBSP transforms LogicalJoin to BinaryOp (JoinOp)
func logicalJoinToDBSP(join *LogicalJoin) (*op.Node, error) {
	// For now, only support single equi-join condition
	if len(join.Conditions) != 1 {
		return nil, fmt.Errorf("only single join condition supported")
	}

	cond := join.Conditions[0]

	// Create key extraction functions for left and right
	leftKeyFn := func(t types.Tuple) any {
		key := t[cond.LeftCol]
		// NULL keys should not match
		if key == nil {
			return nil
		}
		return key
	}

	rightKeyFn := func(t types.Tuple) any {
		key := t[cond.RightCol]
		// NULL keys should not match
		if key == nil {
			return nil
		}
		return key
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

	// For now JOIN inputs are scans (2-way join); model them as true 2-input DAG sources.
	leftScan, ok := join.Left.(*LogicalScan)
	if !ok {
		return nil, fmt.Errorf("JOIN left input must be LogicalScan (got %T)", join.Left)
	}
	rightScan, ok := join.Right.(*LogicalScan)
	if !ok {
		return nil, fmt.Errorf("JOIN right input must be LogicalScan (got %T)", join.Right)
	}

	leftNode := &op.Node{Source: leftScan.Table}
	rightNode := &op.Node{Source: rightScan.Table}
	return &op.Node{Op: joinOp, Inputs: []*op.Node{leftNode, rightNode}}, nil
}

// logicalSortToDBSP transforms LogicalSort to SortOp
func logicalSortToDBSP(sort *LogicalSort) (*op.Node, error) {
	sortOp := op.NewSortOp(sort.OrderColumns, sort.Descending)
	return &op.Node{Op: sortOp}, nil
}

// logicalLimitToDBSP transforms LogicalLimit to LimitOp
func logicalLimitToDBSP(limit *LogicalLimit) (*op.Node, error) {
	limitOp := op.NewLimitOp(limit.Limit, limit.Offset)
	return &op.Node{Op: limitOp}, nil
}
