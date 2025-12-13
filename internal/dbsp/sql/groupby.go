package sqlconv

import (
	"strings"

	"github.com/Ariyn/tree-sitter-duckdb/bindings/go/ast"
	"github.com/ariyn/dbsp/internal/dbsp/ir"
)

// parseGroupBy parses GROUP BY expressions, extracting grouping columns and time windows.
// Supports both standard columns and time window functions like TUMBLE(), HOP(), SESSION()
func parseGroupBy(groupBy ast.GroupBy) ([]string, *ir.WindowSpec, error) {
	var groupCols []string
	var groupExprs []string

	for _, gbExpr := range groupBy {
		switch e := gbExpr.(type) {
		case *ast.ColName:
			// Use full column name (table.column if table qualifier exists)
			colName := e.Name
			if e.Table != "" {
				colName = e.Table + "." + e.Name
			}
			groupCols = append(groupCols, colName)
			groupExprs = append(groupExprs, colName)
		default:
			// Try to handle as string expression (for window functions)
			exprStr := e.String()
			exprStr = strings.Trim(exprStr, "'\"")
			groupExprs = append(groupExprs, exprStr)
		}
	}

	// Check for time window functions in group expressions
	timeWindowSpec, remainingCols, err := ParseTimeWindowFromGroupBy(groupExprs)
	if err != nil {
		return nil, nil, err
	}

	if timeWindowSpec != nil {
		// Return remaining columns (non-window columns)
		return remainingCols, nil, nil
	}

	return groupCols, nil, nil
}
