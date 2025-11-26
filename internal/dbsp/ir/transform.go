package ir

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/ariyn/dbsp/internal/dbsp/op"
	"github.com/ariyn/dbsp/internal/dbsp/types"
)

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
		if !ok {
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
		if !ok {
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
		if !ok {
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
		if !ok {
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
		if !ok {
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
		if !ok {
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

		// Check if we need to chain with filter
		if filter, ok := n.Input.(*LogicalFilter); ok {
			// Build filter predicate
			predicateFn := BuildPredicateFunc(filter.PredicateSQL)
			filterOp := &op.MapOp{
				F: func(td types.TupleDelta) []types.TupleDelta {
					if predicateFn(td.Tuple) {
						return []types.TupleDelta{td}
					}
					return nil
				},
			}
			// Chain: filter first, then project
			return &op.Node{Op: &op.ChainedOp{Ops: []op.Operator{filterOp, projectOp}}}, nil
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
				// Non-windowed grouping: keep existing single-key restriction
				if len(n.Keys) != 1 {
					return nil, fmt.Errorf("only single-group-key supported")
				}
				key := n.Keys[0]
				keyFn := func(t types.Tuple) any { return t[key] }

				var agg op.AggFunc
				var aggInit func() any
				switch n.AggName {
				case "SUM", "sum":
					agg = &op.SumAgg{ColName: n.AggCol}
					aggInit = func() any { return float64(0) }
				case "COUNT", "count":
					agg = &op.CountAgg{}
					aggInit = func() any { return int64(0) }
				default:
					return nil, fmt.Errorf("unsupported agg %s", n.AggName)
				}

				g := op.NewGroupAggOp(keyFn, aggInit, agg)
				return &op.Node{Op: g}, nil
			}

			// Windowed aggregation: for the first version we only support
			// a single tumbling window over a time column, without extra
			// group-by keys. We reuse GroupAggOp with a window key function.
			ws := n.WindowSpec
			if ws == nil {
				return nil, fmt.Errorf("windowSpec must not be nil in windowed branch")
			}
			keyFn := buildWindowKeyFn(ws.TimeCol, ws.SizeMillis)

			var agg op.AggFunc
			var aggInit func() any
			switch n.AggName {
			case "SUM", "sum":
				agg = &op.SumAgg{ColName: n.AggCol}
				aggInit = func() any { return float64(0) }
			case "COUNT", "count":
				agg = &op.CountAgg{}
				aggInit = func() any { return int64(0) }
			default:
				return nil, fmt.Errorf("unsupported agg %s", n.AggName)
			}

			g := op.NewGroupAggOp(keyFn, aggInit, agg)
			return &op.Node{Op: g}, nil

		case *LogicalFilter:
			// Filter before GroupAgg - create chained MapOp
			if n.WindowSpec == nil {
				// Non-windowed grouping with a single group key
				if len(n.Keys) != 1 {
					return nil, fmt.Errorf("only single-group-key supported")
				}
				key := n.Keys[0]
				keyFn := func(t types.Tuple) any { return t[key] }

				var agg op.AggFunc
				var aggInit func() any
				switch n.AggName {
				case "SUM", "sum":
					agg = &op.SumAgg{ColName: n.AggCol}
					aggInit = func() any { return float64(0) }
				case "COUNT", "count":
					agg = &op.CountAgg{}
					aggInit = func() any { return int64(0) }
				default:
					return nil, fmt.Errorf("unsupported agg %s", n.AggName)
				}

				// Create filter function
				predicateFn := BuildPredicateFunc(in.PredicateSQL)

				// Create a combined operator: filter then aggregate
				// For Phase1, we embed filtering in the GroupAgg by wrapping the input
				g := op.NewGroupAggOp(keyFn, aggInit, agg)

				// Wrap with MapOp for filtering
				filterOp := &op.MapOp{
					F: func(td types.TupleDelta) []types.TupleDelta {
						if predicateFn(td.Tuple) {
							return []types.TupleDelta{td}
						}
						return nil
					},
				}

				// Create a ChainedOp that applies filter then aggregate
				chainedOp := &op.ChainedOp{
					Ops: []op.Operator{filterOp, g},
				}

				return &op.Node{Op: chainedOp}, nil
			}

			// Windowed aggregation with a filter. For now we only support
			// a single tumbling window (no extra group keys).
			ws := n.WindowSpec
			if ws == nil {
				return nil, fmt.Errorf("windowSpec must not be nil in windowed filter branch")
			}
			keyFn := buildWindowKeyFn(ws.TimeCol, ws.SizeMillis)

			var agg op.AggFunc
			var aggInit func() any
			switch n.AggName {
			case "SUM", "sum":
				agg = &op.SumAgg{ColName: n.AggCol}
				aggInit = func() any { return float64(0) }
			case "COUNT", "count":
				agg = &op.CountAgg{}
				aggInit = func() any { return int64(0) }
			default:
				return nil, fmt.Errorf("unsupported agg %s", n.AggName)
			}

			// Create filter function
			predicateFn := BuildPredicateFunc(in.PredicateSQL)

			g := op.NewGroupAggOp(keyFn, aggInit, agg)

			// Wrap with MapOp for filtering
			filterOp := &op.MapOp{
				F: func(td types.TupleDelta) []types.TupleDelta {
					if predicateFn(td.Tuple) {
						return []types.TupleDelta{td}
					}
					return nil
				},
			}

			// Create a ChainedOp that applies filter then aggregate
			chainedOp := &op.ChainedOp{
				Ops: []op.Operator{filterOp, g},
			}

			return &op.Node{Op: chainedOp}, nil

		default:
			return nil, fmt.Errorf("unsupported input node to GroupAgg: %T", in)
		}
	default:
		return nil, fmt.Errorf("unsupported logical node: %T", n)
	}
}
