package sqlconv

import (
	"errors"

	"github.com/ariyn/dbsp/internal/dbsp/ir"
	"github.com/xwb1989/sqlparser"
)

// parseGroupBy parses GROUP BY expressions, extracting grouping columns and
// at most one tumbling window specification.
func parseGroupBy(groupBy sqlparser.GroupBy) ([]string, *ir.WindowSpec, error) {
	var (
		groupCols  []string
		windowSpec *ir.WindowSpec
	)

	for _, gbExpr := range groupBy {
		switch e := gbExpr.(type) {
		case *sqlparser.ColName:
			groupCols = append(groupCols, e.Name.String())
		case *sqlparser.FuncExpr:
			// Minimal support for: GROUP BY TUMBLE(ts_col, INTERVAL '5' MINUTE)
			funcName := e.Name.String()
			if funcName != "tumble" && funcName != "TUMBLE" {
				return nil, nil, errors.New("unsupported GROUP BY function (only TUMBLE supported)")
			}
			if len(e.Exprs) != 2 {
				return nil, nil, errors.New("TUMBLE expects two arguments: time column, interval literal")
			}

			// First argument: time column
			arg1, ok := e.Exprs[0].(*sqlparser.AliasedExpr)
			if !ok {
				return nil, nil, errors.New("unsupported TUMBLE time column expression")
			}
			col, ok := arg1.Expr.(*sqlparser.ColName)
			if !ok {
				return nil, nil, errors.New("TUMBLE time column must be a simple column name")
			}
			timeCol := col.Name.String()

			// Second argument: we expect an interval literal, e.g., INTERVAL '5' MINUTE
			arg2, ok := e.Exprs[1].(*sqlparser.AliasedExpr)
			if !ok {
				return nil, nil, errors.New("unsupported TUMBLE interval expression")
			}
			intervalSQL := sqlparser.String(arg2.Expr)
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
