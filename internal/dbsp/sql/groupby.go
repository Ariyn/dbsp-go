package sqlconv

import (
	"strings"

	"github.com/Ariyn/tree-sitter-duckdb/bindings/go/ast"
	"github.com/ariyn/dbsp/internal/dbsp/ir"
)

// parseGroupBy parses GROUP BY expressions, extracting grouping columns and time windows.
// Supports both standard columns and time window functions like TUMBLE(), HOP(), SESSION()
func parseGroupBy(groupBy ast.GroupBy) ([]string, *ir.WindowSpec, error) {
	groupCols, windowSpec, _, err := parseGroupByWithTimeWindow(groupBy)
	return groupCols, windowSpec, err
}

// parseGroupByWithTimeWindow parses GROUP BY expressions, extracting grouping columns and
// time-based windows (TUMBLE/HOP/SESSION).
//
// It returns:
// - groupCols: grouping columns excluding the time window function (if present)
// - windowSpec: legacy simple tumbling window spec (unused currently)
// - timeWindowSpec: event-time window spec for WindowAggOp (if present)
func parseGroupByWithTimeWindow(groupBy ast.GroupBy) ([]string, *ir.WindowSpec, *ir.TimeWindowSpec, error) {
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
			if norm := normalizeGroupIdentifier(colName); norm != "" {
				colName = norm
			}
			groupCols = append(groupCols, colName)
			groupExprs = append(groupExprs, colName)
		default:
			// Try to handle as string expression (for window functions)
			exprStr := e.String()
			exprStr = strings.Trim(exprStr, "'\"")
			if id := normalizeGroupIdentifier(exprStr); id != "" {
				groupCols = append(groupCols, id)
			}
			groupExprs = append(groupExprs, exprStr)
		}
	}

	// Check for time window functions in group expressions
	timeWindowSpec, remainingCols, err := ParseTimeWindowFromGroupBy(groupExprs)
	if err != nil {
		return nil, nil, nil, err
	}

	if timeWindowSpec != nil {
		// Return remaining columns (non-window columns)
		return remainingCols, nil, timeWindowSpec, nil
	}

	return groupCols, nil, nil, nil
}

func normalizeGroupIdentifier(expr string) string {
	e := strings.TrimSpace(expr)
	e = strings.Trim(e, "'\"")
	if e == "" {
		return ""
	}
	for i := 0; i < len(e); i++ {
		c := e[i]
		if (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '_' || c == '.' {
			continue
		}
		return ""
	}
	return e
}
