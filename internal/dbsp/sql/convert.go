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
	// Check for window functions FIRST (before sqlparser)
	// Since xwb1989/sqlparser doesn't support OVER clause parsing,
	// we do simple string matching
	queryUpper := strings.ToUpper(query)
	if strings.Contains(queryUpper, " OVER ") {
		wf, err := parseWindowFunctionFromQuery(query)
		if err != nil {
			return nil, err
		}
		if wf != nil {
			// Build scan node
			scan := &ir.LogicalScan{Table: "t"}
			wf.Input = scan
			return wf, nil
		}
	}

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
	var (
		groupCols  []string
		windowSpec *ir.WindowSpec
	)

	// Parse all GROUP BY expressions
	for _, gbExpr := range sel.GroupBy {
		switch e := gbExpr.(type) {
		case *sqlparser.ColName:
			// Regular GROUP BY column
			groupCols = append(groupCols, e.Name.String())
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
		default:
			return nil, errors.New("unsupported GROUP BY expression")
		}
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
		// true aggregates like SUM/COUNT/AVG/MIN/MAX.
		if name != "SUM" && name != "COUNT" && name != "AVG" && name != "MIN" && name != "MAX" {
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
		Keys:       groupCols,
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

// parseWindowFunctionFromQuery parses window function from query string
// This is a simple parser since xwb1989/sqlparser doesn't support OVER clause
func parseWindowFunctionFromQuery(query string) (*ir.LogicalWindowFunc, error) {
	// Find the OVER clause pattern: LAG(col, offset) OVER (PARTITION BY x ORDER BY y)
	queryUpper := strings.ToUpper(query)

	// Find LAG function
	lagIdx := strings.Index(queryUpper, "LAG(")
	if lagIdx == -1 {
		return nil, nil // No LAG function
	}

	// Find OVER clause
	overIdx := strings.Index(queryUpper[lagIdx:], "OVER")
	if overIdx == -1 {
		return nil, errors.New("LAG function requires OVER clause")
	}
	overIdx += lagIdx

	// Extract LAG arguments
	lagStart := lagIdx + 4 // after "LAG("
	lagEnd := strings.Index(query[lagStart:], ")")
	if lagEnd == -1 {
		return nil, errors.New("LAG function not closed properly")
	}
	lagEnd += lagStart

	lagArgsStr := strings.TrimSpace(query[lagStart:lagEnd])
	lagArgsParts := strings.Split(lagArgsStr, ",")

	var lagCol string
	offset := 1

	if len(lagArgsParts) > 0 {
		lagCol = strings.TrimSpace(lagArgsParts[0])
	} else {
		return nil, errors.New("LAG requires at least one argument")
	}

	if len(lagArgsParts) > 1 {
		offsetStr := strings.TrimSpace(lagArgsParts[1])
		if offsetVal, err := strconv.Atoi(offsetStr); err == nil {
			offset = offsetVal
		}
	}

	// Extract OVER clause content
	overStart := strings.Index(query[overIdx:], "(")
	if overStart == -1 {
		return nil, errors.New("OVER clause must have parentheses")
	}
	overStart += overIdx + 1

	overEnd := strings.Index(query[overStart:], ")")
	if overEnd == -1 {
		return nil, errors.New("OVER clause not closed properly")
	}
	overEnd += overStart

	overContent := query[overStart:overEnd]
	overContentUpper := strings.ToUpper(overContent)

	// Parse PARTITION BY
	var partitionBy []string
	partIdx := strings.Index(overContentUpper, "PARTITION BY")
	if partIdx != -1 {
		partStart := partIdx + 12 // after "PARTITION BY"

		// Find end of PARTITION BY clause (either ORDER BY or end)
		orderIdx := strings.Index(overContentUpper[partStart:], "ORDER BY")
		var partEnd int
		if orderIdx != -1 {
			partEnd = partStart + orderIdx
		} else {
			partEnd = len(overContent)
		}

		partCols := strings.TrimSpace(overContent[partStart:partEnd])
		for _, col := range strings.Split(partCols, ",") {
			col = strings.TrimSpace(col)
			if col != "" {
				partitionBy = append(partitionBy, col)
			}
		}
	}

	// Parse ORDER BY
	var orderBy string
	orderIdx := strings.Index(overContentUpper, "ORDER BY")
	if orderIdx == -1 {
		return nil, errors.New("LAG requires ORDER BY in OVER clause")
	}

	orderStart := orderIdx + 8 // after "ORDER BY"
	orderContent := strings.TrimSpace(overContent[orderStart:])

	// Take first column (ignore ASC/DESC for now)
	orderParts := strings.Fields(orderContent)
	if len(orderParts) > 0 {
		orderBy = orderParts[0]
	} else {
		return nil, errors.New("ORDER BY must specify a column")
	}

	// Determine output column name from alias
	outputCol := "lag_" + lagCol

	// Try to extract alias from query
	asIdx := strings.Index(queryUpper, " AS ")
	if asIdx != -1 {
		afterAs := strings.TrimSpace(query[asIdx+4:])
		// Extract alias (word before FROM)
		fromIdx := strings.Index(strings.ToUpper(afterAs), " FROM")
		if fromIdx != -1 {
			outputCol = strings.TrimSpace(afterAs[:fromIdx])
		}
	}

	return &ir.LogicalWindowFunc{
		Spec: ir.WindowFuncSpec{
			FuncName:    "LAG",
			Args:        []string{lagCol},
			PartitionBy: partitionBy,
			OrderBy:     orderBy,
			Offset:      offset,
		},
		OutputCol: outputCol,
	}, nil
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
