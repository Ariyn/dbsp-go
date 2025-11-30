package sqlconv

import (
	"errors"

	"github.com/Ariyn/tree-sitter-duckdb/bindings/go/ast"
	"github.com/ariyn/dbsp/internal/dbsp/ir"
)

// parseGroupBy parses GROUP BY expressions, extracting grouping columns.
// DuckDB standard window functions are handled in SELECT clause, not GROUP BY.
func parseGroupBy(groupBy ast.GroupBy) ([]string, *ir.WindowSpec, error) {
	var groupCols []string

	for _, gbExpr := range groupBy {
		switch e := gbExpr.(type) {
		case *ast.ColName:
			groupCols = append(groupCols, e.Name)
		default:
			return nil, nil, errors.New("unsupported GROUP BY expression")
		}
	}

	return groupCols, nil, nil
}
