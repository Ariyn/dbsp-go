package sqlconv

import (
	"errors"
	"strconv"
	"strings"

	"github.com/Ariyn/tree-sitter-duckdb/bindings/go/ast"
	"github.com/Ariyn/tree-sitter-duckdb/bindings/go/parser"
	"github.com/ariyn/dbsp/internal/dbsp/diff"
	"github.com/ariyn/dbsp/internal/dbsp/ir"
	"github.com/ariyn/dbsp/internal/dbsp/op"
)

// ParseQueryToLogicalPlan parses a tiny subset of SQL into a LogicalNode.
func ParseQueryToLogicalPlan(query string) (ir.LogicalNode, error) {
	p := parser.NewParser()
	stmt, err := p.Parse(query)
	if err != nil {
		return nil, err
	}

	sel, ok := stmt.(*ast.Select)
	if !ok {
		return nil, errors.New("only SELECT supported")
	}

	// Check for window functions (LAG ... OVER ...)
	if wf, scan, err := parseWindowFunctionFromSelect(sel); wf != nil || err != nil {
		if err != nil {
			return nil, err
		}
		wf.Input = scan
		return wf, nil
	}

	// Check for window aggregate functions (SUM(...) OVER ...)
	if waf, scan, err := parseWindowAggregateFromSelect(sel); waf != nil || err != nil {
		if err != nil {
			return nil, err
		}
		waf.Input = scan
		return waf, nil
	}

	if len(sel.From) != 1 {
		return nil, errors.New("only single FROM table supported")
	}

	// Check if we have a JOIN in the FROM clause
	fromExpr := sel.From[0]
	if joinExpr, ok := fromExpr.(*ast.JoinTableExpr); ok {
		// Handle JOIN
		return parseJoin(sel, joinExpr)
	}

	// Start with scan - extract table name from FROM
	tableName := "t"
	if tableExpr, ok := sel.From[0].(*ast.TableName); ok {
		tableName = tableExpr.Name
	}
	scan := &ir.LogicalScan{Table: tableName}
	var currentNode ir.LogicalNode = scan

	// Add filter if WHERE clause exists
	if sel.Where != nil {
		whereSQL := sel.Where.String()
		// Remove surrounding quotes that tree-sitter adds
		whereSQL = strings.Trim(whereSQL, "'\"")
		currentNode = &ir.LogicalFilter{
			PredicateSQL: whereSQL,
			Input:        currentNode,
		}
	}

	selectCols, err := extractSelectColumns(sel)
	if err != nil {
		return nil, err
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

	groupCols, windowSpec, err = parseGroupBy(sel.GroupBy)
	if err != nil {
		return nil, err
	}

	// Use original query string to find aggregate because parser has bugs
	aggFunc, aggCol, err := findSingleAggregateFromQuery(query)
	if err != nil {
		return nil, err
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
	// INTERVAL '5' SECOND  (old sqlparser format)
	// INTERVAL 5 MINUTE    (tree-sitter format, may be quoted)
	upper := strings.ToUpper(strings.TrimSpace(intervalSQL))

	// Remove outer quotes if present (from tree-sitter String() method)
	upper = strings.Trim(upper, "'\"")

	if !strings.HasPrefix(upper, "INTERVAL") {
		return 0, errors.New("interval must start with INTERVAL")
	}
	// Remove leading INTERVAL
	rest := strings.TrimSpace(upper[len("INTERVAL"):])

	// Parse two formats:
	// 1. INTERVAL '5' MINUTE (quoted number)
	// 2. INTERVAL 5 MINUTE (unquoted number)
	var numStr, restUnit string

	if strings.HasPrefix(rest, "'") {
		// Format 1: quoted number
		endQuote := strings.Index(rest[1:], "'")
		if endQuote <= 0 {
			return 0, errors.New("invalid INTERVAL literal")
		}
		numStr = rest[1 : 1+endQuote]
		restUnit = strings.TrimSpace(rest[1+endQuote+1:])
	} else {
		// Format 2: unquoted number
		parts := strings.Fields(rest)
		if len(parts) < 2 {
			return 0, errors.New("INTERVAL must have number and unit")
		}
		numStr = parts[0]
		restUnit = parts[1]
	}

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

// parseWindowFunctionFromSelect parses window function from AST Select
func parseWindowFunctionFromSelect(sel *ast.Select) (*ir.LogicalWindowFunc, *ir.LogicalScan, error) {
	// Find LAG function with OVER clause in SELECT list
	var lagFunc *ast.FuncExpr
	var outputCol string

	for _, item := range sel.SelectList {
		if funcExpr, ok := item.Expr.(*ast.FuncExpr); ok {
			if strings.ToUpper(funcExpr.Name) == "LAG" && funcExpr.Over != nil {
				lagFunc = funcExpr
				if item.As != "" {
					outputCol = item.As
				}
				break
			}
		}
	}

	if lagFunc == nil {
		return nil, nil, nil // No LAG function
	}

	// Extract LAG arguments
	if len(lagFunc.Args) < 1 {
		return nil, nil, errors.New("LAG requires at least one argument")
	}

	lagCol := lagFunc.Args[0].String()
	offset := 1

	if len(lagFunc.Args) > 1 {
		if lit, ok := lagFunc.Args[1].(*ast.Literal); ok && lit.Type == "INTEGER" {
			if val, err := strconv.Atoi(lit.Value); err == nil {
				offset = val
			}
		}
	}

	// Parse PARTITION BY from OVER clause
	var partitionBy []string
	for _, expr := range lagFunc.Over.PartitionBy {
		partitionBy = append(partitionBy, expr.String())
	}

	// Parse ORDER BY from OVER clause
	var orderBy string
	if len(lagFunc.Over.OrderBy) == 0 {
		return nil, nil, errors.New("LAG requires ORDER BY in OVER clause")
	}
	orderBy = lagFunc.Over.OrderBy[0].Expr.String()

	// Determine output column name
	if outputCol == "" {
		outputCol = "lag_" + lagCol
	}

	// Extract table name from FROM clause
	tableName := "t"
	if len(sel.From) > 0 {
		if tableExpr, ok := sel.From[0].(*ast.TableName); ok {
			tableName = tableExpr.Name
		}
	}

	wf := &ir.LogicalWindowFunc{
		Spec: ir.WindowFuncSpec{
			FuncName:    "LAG",
			Args:        []string{lagCol},
			PartitionBy: partitionBy,
			OrderBy:     orderBy,
			Offset:      offset,
		},
		OutputCol: outputCol,
	}

	scan := &ir.LogicalScan{Table: tableName}
	return wf, scan, nil
}

// parseWindowAggregateFromSelect parses window aggregate function from SELECT clause.
// Supports: SUM(col) OVER (PARTITION BY ... ORDER BY ... RANGE BETWEEN ...)
func parseWindowAggregateFromSelect(sel *ast.Select) (*ir.LogicalWindowAgg, *ir.LogicalScan, error) {
	var aggFunc *ast.FuncExpr
	var outputCol string

	for _, item := range sel.SelectList {
		if funcExpr, ok := item.Expr.(*ast.FuncExpr); ok {
			if funcExpr.Over != nil {
				// This is a window aggregate function
				funcName := strings.ToUpper(funcExpr.Name)
				if funcName == "SUM" || funcName == "AVG" || funcName == "COUNT" || funcName == "MIN" || funcName == "MAX" {
					aggFunc = funcExpr
					if item.As != "" {
						outputCol = item.As
					}
					break
				}
			}
		}
	}

	if aggFunc == nil {
		return nil, nil, nil // No window aggregate function
	}

	// Extract aggregate column
	aggCol := ""
	if len(aggFunc.Args) > 0 {
		aggCol = aggFunc.Args[0].String()
	}

	// Parse PARTITION BY from OVER clause
	var partitionBy []string
	for _, expr := range aggFunc.Over.PartitionBy {
		partitionBy = append(partitionBy, expr.String())
	}

	// Parse ORDER BY from OVER clause
	var orderBy string
	if len(aggFunc.Over.OrderBy) > 0 {
		orderBy = aggFunc.Over.OrderBy[0].Expr.String()
	}

	// Parse frame specification (ROWS/RANGE/GROUPS BETWEEN ...)
	var frameSpec *ir.FrameSpec
	var timeWindowSpec *ir.TimeWindowSpec
	
	if aggFunc.Over.Frame != nil {
		frameSpec = parseFrameSpec(aggFunc.Over.Frame)
		
		// Check if frame uses time-based RANGE with INTERVAL
		// This indicates a time window (e.g., RANGE BETWEEN INTERVAL '5' MINUTE PRECEDING AND CURRENT ROW)
		if frameSpec != nil && strings.ToUpper(frameSpec.Type) == "RANGE" {
			if strings.Contains(strings.ToUpper(frameSpec.StartValue), "INTERVAL") ||
				strings.Contains(strings.ToUpper(frameSpec.EndValue), "INTERVAL") {
				// This is a time-based window
				// For now, we'll treat RANGE with INTERVAL as a sliding window
				// Extract the interval from StartValue
				if frameSpec.StartValue != "" {
					interval, err := parseIntervalArg(frameSpec.StartValue)
					if err == nil && orderBy != "" {
						timeWindowSpec = &ir.TimeWindowSpec{
							WindowType: "SLIDING",
							TimeCol:    orderBy,
							SizeMillis: interval,
							SlideMillis: interval / 2, // Default: half of size
						}
					}
				}
			}
		}
	}

	if outputCol == "" {
		outputCol = strings.ToLower(aggFunc.Name) + "_" + aggCol
	}

	// Extract table name from FROM clause
	tableName := "t"
	if len(sel.From) > 0 {
		if tableExpr, ok := sel.From[0].(*ast.TableName); ok {
			tableName = tableExpr.Name
		}
	}

	waf := &ir.LogicalWindowAgg{
		AggName:        strings.ToUpper(aggFunc.Name),
		AggCol:         aggCol,
		PartitionBy:    partitionBy,
		OrderBy:        orderBy,
		FrameSpec:      frameSpec,
		TimeWindowSpec: timeWindowSpec,
		OutputCol:      outputCol,
	}

	scan := &ir.LogicalScan{Table: tableName}
	return waf, scan, nil
}

// parseFrameSpec parses frame specification from AST WindowFrame
func parseFrameSpec(frame *ast.WindowFrame) *ir.FrameSpec {
	if frame == nil {
		return nil
	}

	spec := &ir.FrameSpec{
		Type: strings.ToUpper(frame.Type), // ROWS, RANGE, or GROUPS
	}

	// Parse frame bounds
	if frame.Start != nil {
		spec.StartType = strings.ToUpper(frame.Start.Type)
		if frame.Start.Value != nil {
			spec.StartValue = frame.Start.Value.String()
		}
	}

	if frame.End != nil {
		spec.EndType = strings.ToUpper(frame.End.Type)
		if frame.End.Value != nil {
			spec.EndValue = frame.End.Value.String()
		}
	}

	return spec
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

// parseJoin handles JOIN in the FROM clause
func parseJoin(sel *ast.Select, joinExpr *ast.JoinTableExpr) (ir.LogicalNode, error) {
	// Extract left and right table names
	leftTable, ok := joinExpr.LeftExpr.(*ast.TableName)
	if !ok {
		return nil, errors.New("JOIN left side must be a table")
	}

	rightTable, ok := joinExpr.RightExpr.(*ast.TableName)
	if !ok {
		return nil, errors.New("JOIN right side must be a table")
	}

	// Parse ON conditions (only equi-join supported)
	conditions, err := parseJoinConditions(joinExpr.On)
	if err != nil {
		return nil, err
	}

	// Create LogicalJoin
	leftScan := &ir.LogicalScan{Table: leftTable.Name}
	rightScan := &ir.LogicalScan{Table: rightTable.Name}

	join := &ir.LogicalJoin{
		LeftTable:  leftTable.Name,
		RightTable: rightTable.Name,
		Conditions: conditions,
		Left:       leftScan,
		Right:      rightScan,
	}

	var currentNode ir.LogicalNode = join

	// Add WHERE filter if present
	if sel.Where != nil {
		whereSQL := sel.Where.String()
		whereSQL = strings.Trim(whereSQL, "'\"")
		currentNode = &ir.LogicalFilter{
			PredicateSQL: whereSQL,
			Input:        currentNode,
		}
	}

	// Extract select columns
	selectCols, err := extractSelectColumns(sel)
	if err != nil {
		return nil, err
	}

	// Check for GROUP BY
	if len(sel.GroupBy) == 0 {
		// No GROUP BY - add projection if needed
		if len(selectCols) > 0 {
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

	groupCols, windowSpec, err = parseGroupBy(sel.GroupBy)
	if err != nil {
		return nil, err
	}

	// Find aggregate function from query string
	aggFunc, aggCol, err := findSingleAggregateFromQuery(sel.String())
	if err != nil {
		return nil, err
	}

	// Build GroupAgg with input from current node
	lg := &ir.LogicalGroupAgg{
		Keys:       groupCols,
		AggName:    aggFunc,
		AggCol:     aggCol,
		WindowSpec: windowSpec,
		Input:      currentNode,
	}

	return lg, nil
}

// parseJoinConditions parses ON clause into JoinConditions
func parseJoinConditions(onExpr ast.Expr) ([]ir.JoinCondition, error) {
	if onExpr == nil {
		return nil, errors.New("JOIN requires ON clause")
	}

	// For now, only support simple equality: a.col = b.col
	binExpr, ok := onExpr.(*ast.BinaryExpr)
	if !ok || binExpr.Operator != "=" {
		return nil, errors.New("JOIN only supports equi-join (=)")
	}

	leftCol := binExpr.Left.String()
	rightCol := binExpr.Right.String()

	// Remove quotes added by tree-sitter
	leftCol = strings.Trim(leftCol, "'\"")
	rightCol = strings.Trim(rightCol, "'\"")

	condition := ir.JoinCondition{
		LeftCol:  leftCol,
		RightCol: rightCol,
	}

	return []ir.JoinCondition{condition}, nil
}
