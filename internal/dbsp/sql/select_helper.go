package sqlconv

import (
	"errors"
	"strings"

	"github.com/Ariyn/tree-sitter-duckdb/bindings/go/ast"
	"github.com/ariyn/dbsp/internal/dbsp/ir"
)

// extractSelectColumns collects plain column names from SELECT list for projection.
// It ignores aggregate functions (handled separately) and stops projection when '*'
// is present.
func extractSelectColumns(sel *ast.Select) ([]string, error) {
	var cols []string
	for _, item := range sel.SelectList {
		switch e := item.Expr.(type) {
		case *ast.StarExpr:
			// SELECT * - no projection needed
			return nil, nil
		case *ast.ColName:
			// Use full column name (table.column if table qualifier exists)
			colName := e.Name
			if e.Table != "" {
				colName = e.Table + "." + e.Name
			}
			cols = append(cols, colName)
		case *ast.FuncExpr:
			// aggregate or other function - handled elsewhere, skip for projection
			continue
		default:
			// For now we don't support arbitrary expressions in projection
			return nil, errors.New("unsupported SELECT expression (only columns, aggregates, or * supported)")
		}
	}
	return cols, nil
}

// extractProjectionSpecs parses the SELECT list into plain column projections and
// computed expressions (which require an alias).
// Aggregates are ignored (handled separately).
// If '*' is present, it returns (nil, nil, nil) meaning "no projection".
func extractProjectionSpecs(sel *ast.Select) ([]string, []ir.ProjectExpr, error) {
	var cols []string
	var exprs []ir.ProjectExpr
	for _, item := range sel.SelectList {
		switch e := item.Expr.(type) {
		case *ast.StarExpr:
			return nil, nil, nil
		case *ast.ColName:
			colName := e.Name
			if e.Table != "" {
				colName = e.Table + "." + e.Name
			}
			cols = append(cols, colName)
		case *ast.FuncExpr:
			// aggregate or other function - handled elsewhere
			continue
		default:
			// Computed expression: require alias so output column is stable.
			if strings.TrimSpace(item.As) == "" {
				return nil, nil, errors.New("unsupported SELECT expression without alias (use AS <name>)")
			}
			exprSQL := strings.TrimSpace(item.Expr.String())
			exprs = append(exprs, ir.ProjectExpr{ExprSQL: exprSQL, As: item.As})
		}
	}
	return cols, exprs, nil
}

// findSingleAggregate scans SELECT expressions and returns a single supported
// aggregate function name and its column. It keeps existing constraints
// (single aggregate, single column arg).
func findSingleAggregate(selectList ast.SelectExprs) (string, string, error) {
	var aggFunc string
	var aggCol string

	for _, item := range selectList {
		f, ok := item.Expr.(*ast.FuncExpr)
		if !ok {
			continue
		}

		// Extract function name from the SQL string representation
		// because the parser may not correctly populate f.Name
		sqlStr := f.String()
		// Format: FUNCNAME(arg) or funcname(arg)
		parenIdx := strings.Index(sqlStr, "(")
		if parenIdx == -1 {
			continue
		}
		name := strings.ToUpper(strings.TrimSpace(sqlStr[:parenIdx]))

		// Ignore window functions such as TUMBLE here; we only care about
		// true aggregates like SUM/COUNT/AVG/MIN/MAX.
		if name != "SUM" && name != "COUNT" && name != "AVG" && name != "MIN" && name != "MAX" {
			continue
		}
		if aggFunc != "" && name != "" {
			return "", "", errors.New("multiple aggregate functions not supported yet")
		}

		// Extract column from the string between parentheses
		endParen := strings.LastIndex(sqlStr, ")")
		if endParen == -1 || endParen <= parenIdx {
			return "", "", errors.New("malformed function call")
		}
		argStr := strings.TrimSpace(sqlStr[parenIdx+1 : endParen])
		if argStr == "" {
			return "", "", errors.New("empty aggregate argument")
		}

		aggFunc = name
		aggCol = argStr
	}

	if aggFunc == "" {
		return "", "", errors.New("no aggregate function found")
	}

	return aggFunc, aggCol, nil
}

// findSingleAggregateFromQuery extracts aggregate function from SQL string
// This is a workaround for tree-sitter parser bugs with function calls
func findSingleAggregateFromQuery(query string) (string, string, error) {
	queryUpper := strings.ToUpper(query)

	// Look for aggregate functions: SUM(col), COUNT(col), AVG(col), MIN(col), MAX(col)
	aggFuncs := []string{"SUM", "COUNT", "AVG", "MIN", "MAX"}

	for _, funcName := range aggFuncs {
		pattern := funcName + "("
		idx := strings.Index(queryUpper, pattern)
		if idx == -1 {
			continue
		}

		// Find matching closing parenthesis
		start := idx + len(pattern)
		depth := 1
		end := start

		for end < len(query) && depth > 0 {
			if query[end] == '(' {
				depth++
			} else if query[end] == ')' {
				depth--
			}
			end++
		}

		if depth != 0 {
			return "", "", errors.New("malformed aggregate function")
		}

		// Extract column name between parentheses
		colName := strings.TrimSpace(query[start : end-1])
		if colName == "" {
			return "", "", errors.New("empty aggregate argument")
		}

		return funcName, colName, nil
	}

	return "", "", errors.New("no aggregate function found")
}

// AggCall represents a parsed aggregate call from a SELECT list.
// Name is upper-cased (e.g., SUM, COUNT). Col is the raw argument string.
type AggCall struct {
	Name string
	Col  string
}

// findAggregatesFromQuery extracts all supported aggregate calls from the
// SELECT list portion of the SQL query string.
//
// This is intentionally string-based to work around tree-sitter parser bugs.
//
// Scope for multi-aggregate support:
// - Supports: SUM(col), COUNT(col)
// - Rejects: COUNT(*) (handled by a separate TODO)
func findAggregatesFromQuery(query string) ([]AggCall, error) {
	selectClause, err := extractSelectClause(query)
	if err != nil {
		return nil, err
	}

	items := splitByCommaOutsideParens(selectClause)
	var out []AggCall
	for _, rawItem := range items {
		expr := strings.TrimSpace(rawItem)
		if expr == "" {
			continue
		}
		// Ignore window functions / analytic aggregates here.
		if strings.Contains(strings.ToUpper(expr), " OVER ") {
			continue
		}

		call, ok, err := parseAggCall(expr)
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}
		// COUNT(*) is allowed in general, but multi-aggregate support may
		// disallow mixing it with other aggregates at a higher level.
		out = append(out, call)
	}
	if len(out) == 0 {
		return nil, errors.New("no aggregate function found")
	}
	return out, nil
}

func extractSelectClause(query string) (string, error) {
	upper := strings.ToUpper(query)
	selectIdx := strings.Index(upper, "SELECT")
	if selectIdx == -1 {
		return "", errors.New("query must contain SELECT")
	}

	// Find the first FROM at depth 0 after SELECT.
	depth := 0
	fromIdx := -1
	for i := selectIdx + len("SELECT"); i < len(query)-3; i++ {
		switch query[i] {
		case '(':
			depth++
		case ')':
			if depth > 0 {
				depth--
			}
		}
		if depth != 0 {
			continue
		}
		if hasKeywordAtWordBoundary(upper, i, "FROM") {
			fromIdx = i
			break
		}
	}
	if fromIdx == -1 {
		return "", errors.New("query must contain FROM")
	}
	clause := strings.TrimSpace(query[selectIdx+len("SELECT") : fromIdx])
	return clause, nil
}

func splitByCommaOutsideParens(s string) []string {
	var parts []string
	depth := 0
	start := 0
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '(':
			depth++
		case ')':
			if depth > 0 {
				depth--
			}
		case ',':
			if depth == 0 {
				parts = append(parts, s[start:i])
				start = i + 1
			}
		}
	}
	if start < len(s) {
		parts = append(parts, s[start:])
	}
	return parts
}

func hasKeywordAtWordBoundary(upper string, i int, kw string) bool {
	if i < 0 || i+len(kw) > len(upper) {
		return false
	}
	if upper[i:i+len(kw)] != kw {
		return false
	}
	// Left boundary
	if i > 0 {
		c := upper[i-1]
		if (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_' {
			return false
		}
	}
	// Right boundary
	if i+len(kw) < len(upper) {
		c := upper[i+len(kw)]
		if (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_' {
			return false
		}
	}
	return true
}

func parseAggCall(expr string) (AggCall, bool, error) {
	// Find the first '(' and its matching ')'.
	open := strings.Index(expr, "(")
	if open == -1 {
		return AggCall{}, false, nil
	}
	name := strings.ToUpper(strings.TrimSpace(expr[:open]))
	// Allow whitespace before paren: COUNT (x)
	name = strings.TrimSpace(name)
	if name == "" {
		return AggCall{}, false, nil
	}

	depth := 0
	close := -1
	for i := open; i < len(expr); i++ {
		switch expr[i] {
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				close = i
				break
			}
		}
	}
	if close == -1 {
		return AggCall{}, false, errors.New("malformed function call")
	}
	arg := strings.TrimSpace(expr[open+1 : close])
	arg = strings.Trim(arg, "`\"'")
	if arg == "" {
		return AggCall{}, false, errors.New("empty aggregate argument")
	}

	// Only consider real aggregates.
	if name != "SUM" && name != "COUNT" && name != "AVG" && name != "MIN" && name != "MAX" {
		return AggCall{}, false, nil
	}

	return AggCall{Name: name, Col: arg}, true, nil
}
