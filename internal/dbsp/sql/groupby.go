package sqlconv

import (
	"errors"
	"strings"

	"github.com/Ariyn/tree-sitter-duckdb/bindings/go/ast"
	"github.com/ariyn/dbsp/internal/dbsp/ir"
)

// parseGroupBy parses GROUP BY expressions, extracting grouping columns and
// at most one tumbling window specification.
func parseGroupBy(groupBy ast.GroupBy) ([]string, *ir.WindowSpec, error) {
	var (
		groupCols  []string
		windowSpec *ir.WindowSpec
	)

	for _, gbExpr := range groupBy {
		switch e := gbExpr.(type) {
		case *ast.ColName:
			groupCols = append(groupCols, e.Name)
		case *ast.FuncExpr:
			// Minimal support for: GROUP BY TUMBLE(ts_col, INTERVAL '5' MINUTE)
			funcName := strings.ToUpper(e.Name)
			if funcName != "TUMBLE" {
				return nil, nil, errors.New("unsupported GROUP BY function (only TUMBLE supported)")
			}
			if len(e.Args) != 2 {
				return nil, nil, errors.New("TUMBLE expects two arguments: time column, interval literal")
			}

			// First argument: time column
			col, ok := e.Args[0].(*ast.ColName)
			if !ok {
				return nil, nil, errors.New("TUMBLE time column must be a simple column name")
			}
			timeCol := col.Name

			// Second argument: we expect an interval literal, e.g., INTERVAL '5' MINUTE
			intervalSQL := e.Args[1].String()
			sizeMillis, err := parseSimpleIntervalToMillis(intervalSQL)
			if err != nil {
				return nil, nil, err
			}

			if windowSpec != nil {
				return nil, nil, errors.New("multiple TUMBLE windows in GROUP BY not supported")
			}
			windowSpec = &ir.WindowSpec{
				TimeCol:    timeCol,
				SizeMillis: sizeMillis,
			}
		default:
			return nil, nil, errors.New("unsupported GROUP BY expression")
		}
	}

	return groupCols, windowSpec, nil
}
