package sqlconv

import (
	"errors"
	"strings"

	"github.com/Ariyn/tree-sitter-duckdb/bindings/go/ast"
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
			cols = append(cols, e.Name)
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
