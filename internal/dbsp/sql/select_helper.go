package sqlconv

import (
	"errors"
	"strings"

	"github.com/xwb1989/sqlparser"
)

// extractSelectColumns collects plain column names from SELECT list for projection.
// It ignores aggregate functions (handled separately) and stops projection when '*'
// is present.
func extractSelectColumns(sel *sqlparser.Select) ([]string, error) {
	var cols []string
	for _, expr := range sel.SelectExprs {
		switch e := expr.(type) {
		case *sqlparser.StarExpr:
			// SELECT * - no projection needed
			return nil, nil
		case *sqlparser.AliasedExpr:
			switch ae := e.Expr.(type) {
			case *sqlparser.ColName:
				cols = append(cols, ae.Name.String())
			case *sqlparser.FuncExpr:
				// aggregate or other function - handled elsewhere, skip for projection
				continue
			default:
				// For now we don't support arbitrary expressions in projection
				return nil, errors.New("unsupported SELECT expression (only columns, aggregates, or * supported)")
			}
		default:
			return nil, errors.New("unsupported SELECT expression type")
		}
	}
	return cols, nil
}

// findSingleAggregate scans SELECT expressions and returns a single supported
// aggregate function name and its column. It keeps existing constraints
// (single aggregate, single column arg).
func findSingleAggregate(selectExprs sqlparser.SelectExprs) (string, string, error) {
	var aggFunc string
	var aggCol string

	for _, expr := range selectExprs {
		se, ok := expr.(*sqlparser.AliasedExpr)
		if !ok {
			continue
		}
		f, ok := se.Expr.(*sqlparser.FuncExpr)
		if !ok {
			continue
		}
		name := strings.ToUpper(sqlparser.String(f.Name))
		// Ignore window functions such as TUMBLE here; we only care about
		// true aggregates like SUM/COUNT/AVG/MIN/MAX.
		if name != "SUM" && name != "COUNT" && name != "AVG" && name != "MIN" && name != "MAX" {
			continue
		}
		if aggFunc != "" && name != "" {
			return "", "", errors.New("multiple aggregate functions not supported yet")
		}
		if len(f.Exprs) != 1 {
			return "", "", errors.New("only single-arg aggregates supported")
		}
		ae, ok := f.Exprs[0].(*sqlparser.AliasedExpr)
		if !ok {
			return "", "", errors.New("unsupported func arg")
		}
		switch a := ae.Expr.(type) {
		case *sqlparser.ColName:
			aggFunc = name
			aggCol = a.Name.String()
		default:
			return "", "", errors.New("unsupported aggregate arg type")
		}
	}

	if aggFunc == "" {
		return "", "", errors.New("no aggregate function found")
	}

	return aggFunc, aggCol, nil
}
