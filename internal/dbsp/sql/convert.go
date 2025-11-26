package sqlconv

import (
	"errors"
	"strconv"
	"strings"

	"github.com/ariyn/dbsp/internal/dbsp/diff"
	"github.com/ariyn/dbsp/internal/dbsp/ir"
	"github.com/ariyn/dbsp/internal/dbsp/op"
	"github.com/xwb1989/sqlparser"
)

// ParseQueryToLogicalPlan parses a tiny subset of SQL into a LogicalNode.
func ParseQueryToLogicalPlan(query string) (ir.LogicalNode, error) {
	stmt, err := sqlparser.Parse(query)
	if err != nil {
		return nil, err
	}
	sel, ok := stmt.(*sqlparser.Select)
	if !ok {
		return nil, errors.New("only SELECT supported")
	}

	if len(sel.From) != 1 {
		return nil, errors.New("only single FROM table supported")
	}

	// Start with scan
	scan := &ir.LogicalScan{Table: "t"}
	var currentNode ir.LogicalNode = scan

	// Add filter if WHERE clause exists
	if sel.Where != nil {
		whereSQL := sqlparser.String(sel.Where.Expr)
		currentNode = &ir.LogicalFilter{
			PredicateSQL: whereSQL,
			Input:        currentNode,
		}
	}

	// Extract SELECT columns (for projection)
	var selectCols []string
	for _, expr := range sel.SelectExprs {
		switch e := expr.(type) {
		case *sqlparser.StarExpr:
			// SELECT * - no projection needed
			selectCols = nil
		case *sqlparser.AliasedExpr:
			switch ae := e.Expr.(type) {
			case *sqlparser.ColName:
				// Regular column
				selectCols = append(selectCols, ae.Name.String())
			case *sqlparser.FuncExpr:
				// Aggregate function - will be handled in GROUP BY section
				// Don't add to selectCols
			}
		}
	}

	// Check for GROUP BY
	if len(sel.GroupBy) == 0 {
		// No GROUP BY - check if we need projection
		if len(selectCols) > 0 {
			// Add projection
			currentNode = &ir.LogicalProject{
				Columns: selectCols,
				Input:   currentNode,
			}
		}
		return currentNode, nil
	}

	// Handle GROUP BY with aggregation
	gbExpr := sel.GroupBy[0]
	var (
		groupCol   string
		windowSpec *ir.WindowSpec
	)

	switch e := gbExpr.(type) {
	case *sqlparser.ColName:
		// Regular GROUP BY column
		groupCol = e.Name.String()
	case *sqlparser.FuncExpr:
		// Minimal support for: GROUP BY TUMBLE(ts_col, INTERVAL '5' MINUTE)
		funcName := e.Name.String()
		if funcName != "tumble" && funcName != "TUMBLE" {
			return nil, errors.New("unsupported GROUP BY function (only TUMBLE supported)")
		}
		if len(e.Exprs) != 2 {
			return nil, errors.New("TUMBLE expects two arguments: time column, interval literal")
		}

		// First argument: time column
		arg1, ok := e.Exprs[0].(*sqlparser.AliasedExpr)
		if !ok {
			return nil, errors.New("unsupported TUMBLE time column expression")
		}
		col, ok := arg1.Expr.(*sqlparser.ColName)
		if !ok {
			return nil, errors.New("TUMBLE time column must be a simple column name")
		}
		timeCol := col.Name.String()

		// Second argument: we expect an interval literal, e.g., INTERVAL '5' MINUTE
		arg2, ok := e.Exprs[1].(*sqlparser.AliasedExpr)
		if !ok {
			return nil, errors.New("unsupported TUMBLE interval expression")
		}
		// For now, be very strict and require the literal SQL text to be of the form:
		// INTERVAL 'N' SECOND|MINUTE
		intervalSQL := sqlparser.String(arg2.Expr)
		// A tiny parser for the interval; we only support SECOND and MINUTE
		sizeMillis, err := parseSimpleIntervalToMillis(intervalSQL)
		if err != nil {
			return nil, err
		}

		windowSpec = &ir.WindowSpec{
			TimeCol:    timeCol,
			SizeMillis: sizeMillis,
		}

		// For now, we treat the window itself as the only grouping key.
		groupCol = "__window__"
	default:
		return nil, errors.New("unsupported GROUP BY expression")
	}

	// find aggregate: scan SELECT list for SUM/COUNT and ignore window
	var aggFunc string
	var aggCol string
	for _, expr := range sel.SelectExprs {
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
		// true aggregates like SUM/COUNT.
		if name != "SUM" && name != "COUNT" {
			continue
		}
		if len(f.Exprs) != 1 {
			return nil, errors.New("only single-arg aggregates supported")
		}
		ae, ok := f.Exprs[0].(*sqlparser.AliasedExpr)
		if !ok {
			return nil, errors.New("unsupported func arg")
		}
		switch a := ae.Expr.(type) {
		case *sqlparser.ColName:
			aggFunc = name
			aggCol = a.Name.String()
		default:
			return nil, errors.New("unsupported aggregate arg type")
		}
	}
	if aggFunc == "" {
		return nil, errors.New("no aggregate function found")
	}

	// Build GroupAgg with input from current node (which may include filter)
	lg := &ir.LogicalGroupAgg{
		Keys:       []string{groupCol},
		AggName:    aggFunc,
		AggCol:     aggCol,
		WindowSpec: windowSpec,
		Input:      currentNode,
	}
	return lg, nil
}

// parseSimpleIntervalToMillis parses a very small subset of SQL interval
// literals of the form: INTERVAL 'N' SECOND|MINUTE. It returns the
// corresponding duration in milliseconds.
func parseSimpleIntervalToMillis(intervalSQL string) (int64, error) {
	// We keep this implementation intentionally simple and strict to avoid
	// pulling in a full interval parser. We expect something like:
	// INTERVAL '5' SECOND
	// INTERVAL '10' MINUTE
	upper := strings.ToUpper(strings.TrimSpace(intervalSQL))
	if !strings.HasPrefix(upper, "INTERVAL") {
		return 0, errors.New("interval must start with INTERVAL")
	}
	// Remove leading INTERVAL
	rest := strings.TrimSpace(upper[len("INTERVAL"):])
	// Expect a quoted integer literal followed by a unit
	if !strings.HasPrefix(rest, "'") {
		return 0, errors.New("INTERVAL literal must contain quoted number")
	}
	endQuote := strings.Index(rest[1:], "'")
	if endQuote <= 0 {
		return 0, errors.New("invalid INTERVAL literal")
	}
	numStr := rest[1 : 1+endQuote]
	restUnit := strings.TrimSpace(rest[1+endQuote+1:])
	if restUnit == "" {
		return 0, errors.New("INTERVAL must specify a unit")
	}
	// Parse integer value
	val, err := strconv.ParseInt(numStr, 10, 64)
	if err != nil {
		return 0, err
	}
	// Map unit to milliseconds
	switch restUnit {
	case "SECOND", "SECONDS":
		return val * 1000, nil
	case "MINUTE", "MINUTES":
		return val * 60 * 1000, nil
	default:
		return 0, errors.New("unsupported INTERVAL unit (only SECOND/MINUTE supported)")
	}
}

// ParseQueryToDBSP builds a LogicalPlan then transforms it to a DBSP operator node.
func ParseQueryToDBSP(query string) (*op.Node, error) {
	lp, err := ParseQueryToLogicalPlan(query)
	if err != nil {
		return nil, err
	}
	return ir.LogicalToDBSP(lp)
}

// ParseQueryToIncrementalDBSP parses SQL, builds DBSP graph, and applies differentiation.
// This produces an incremental view maintenance graph that processes delta batches.
func ParseQueryToIncrementalDBSP(query string) (*op.Node, error) {
	// First get the base DBSP graph
	baseNode, err := ParseQueryToDBSP(query)
	if err != nil {
		return nil, err
	}

	// Apply differentiation to get incremental version
	// Note: For Phase1 with GroupAgg, this returns the same node since
	// GroupAgg already handles incremental updates internally
	return diff.Differentiate(baseNode)
}
