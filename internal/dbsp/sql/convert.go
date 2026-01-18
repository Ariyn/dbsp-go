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
		// tree-sitter가 TUMBLE/HOP/SESSION 문법을 못 먹는 경우가 있어
		// 문자열 기반 fallback으로 time-window GROUP BY만 구제한다.
		if lp, ok, ferr := parseTimeWindowGroupByFallback(query); ferr != nil {
			return nil, ferr
		} else if ok {
			return lp, nil
		}
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
		return parseJoin(sel, joinExpr, query)
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

	selectCols, selectExprs, err := extractProjectionSpecs(sel)
	if err != nil {
		return nil, err
	}

	// Check for GROUP BY
	if len(sel.GroupBy) == 0 {
		// No GROUP BY - check if we need projection
		if len(selectCols) > 0 || len(selectExprs) > 0 {
			// Add projection
			currentNode = &ir.LogicalProject{
				Columns: selectCols,
				Exprs:   selectExprs,
				Input:   currentNode,
			}
		}
		return currentNode, nil
	}

	// Handle GROUP BY with aggregation
	var (
		groupCols  []string
		windowSpec *ir.WindowSpec
		timeWindowSpec *ir.TimeWindowSpec
	)

	groupCols, windowSpec, timeWindowSpec, err = parseGroupByWithTimeWindow(sel.GroupBy)
	if err != nil {
		return nil, err
	}

	// If this GROUP BY has an event-time window function (TUMBLE/HOP/SESSION),
	// model it as a LogicalWindowAgg rather than a normal LogicalGroupAgg.
	if timeWindowSpec != nil {
		aggs, err := findAggregatesFromQuery(query)
		if err != nil {
			return nil, err
		}
		if len(aggs) != 1 {
			return nil, errors.New("time-window GROUP BY supports exactly one aggregate")
		}
		aggName := strings.ToUpper(strings.TrimSpace(aggs[0].Name))
		aggCol := strings.TrimSpace(aggs[0].Col)
		if aggName == "COUNT" && aggCol == "*" {
			aggCol = ""
		}

		outputCol := extractAggAliasFromQuery(query, aggs[0])
		if strings.TrimSpace(outputCol) == "" {
			outputCol = strings.ToLower(aggName) + "_" + strings.ReplaceAll(aggs[0].Col, " ", "")
		}

		wa := &ir.LogicalWindowAgg{
			AggName:        aggName,
			AggCol:         aggCol,
			PartitionBy:    groupCols,
			TimeWindowSpec: timeWindowSpec,
			OutputCol:      outputCol,
			Input:          currentNode,
		}
		return wa, nil
	}

	// Use original query string to find aggregates because parser has bugs
	aggs, err := findAggregatesFromQuery(query)
	if err != nil {
		return nil, err
	}
	if len(aggs) > 1 {
		for _, a := range aggs {
			name := strings.ToUpper(a.Name)
			if name != "SUM" && name != "COUNT" {
				return nil, errors.New("multiple aggregate functions not supported yet")
			}
			if name == "COUNT" && strings.TrimSpace(a.Col) == "*" {
				return nil, errors.New("COUNT(*) cannot be combined with other aggregates yet")
			}
		}
	}

	// Build GroupAgg with input from current node (which may include filter)
	lg := &ir.LogicalGroupAgg{
		Keys:       groupCols,
		WindowSpec: windowSpec,
		Input:      currentNode,
	}
	// Preserve legacy single-aggregate fields for backward-compatible output.
	if len(aggs) == 1 {
		lg.AggName = aggs[0].Name
		if strings.ToUpper(aggs[0].Name) == "COUNT" && strings.TrimSpace(aggs[0].Col) == "*" {
			lg.AggCol = ""
		} else {
			lg.AggCol = aggs[0].Col
		}
	} else {
		lg.Aggs = make([]ir.AggSpec, 0, len(aggs))
		for _, a := range aggs {
			col := a.Col
			if strings.ToUpper(a.Name) == "COUNT" && strings.TrimSpace(col) == "*" {
				col = ""
			}
			lg.Aggs = append(lg.Aggs, ir.AggSpec{Name: a.Name, Col: col})
		}
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
	case "HOUR", "HOURS":
		return val * 60 * 60 * 1000, nil
	case "DAY", "DAYS":
		return val * 24 * 60 * 60 * 1000, nil
	default:
		return 0, errors.New("unsupported INTERVAL unit")
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
							WindowType:  "SLIDING",
							TimeCol:     orderBy,
							SizeMillis:  interval,
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
func parseJoin(sel *ast.Select, joinExpr *ast.JoinTableExpr, rawQuery string) (ir.LogicalNode, error) {
	// Extract left and right table names
	leftTable, ok := joinExpr.LeftExpr.(*ast.TableName)
	if !ok {
		return nil, errors.New("JOIN left side must be a table")
	}

	rightTable, ok := joinExpr.RightExpr.(*ast.TableName)
	if !ok {
		return nil, errors.New("JOIN right side must be a table")
	}

	// Parse ON conditions.
	// NOTE: tree-sitter may fail to populate joinExpr.On for compound predicates
	// like: a.id=b.id AND a.k=b.k. In that case, fall back to string parsing.
	var conditions []ir.JoinCondition
	var err error
	if joinExpr.On != nil {
		conditions, err = parseJoinConditions(joinExpr.On)
	} else {
		// Some tree-sitter versions omit the ON segment from joinExpr/sel string
		// for compound predicates. Fall back to the original SQL query string.
		conditions, err = parseJoinConditionsFromSQL(rawQuery)
	}
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
	selectCols, selectExprs, err := extractProjectionSpecs(sel)
	if err != nil {
		return nil, err
	}

	// Check for GROUP BY
	if len(sel.GroupBy) == 0 {
		// No GROUP BY - add projection if needed
		if len(selectCols) > 0 || len(selectExprs) > 0 {
			currentNode = &ir.LogicalProject{
				Columns: selectCols,
				Exprs:   selectExprs,
				Input:   currentNode,
			}
		}
		return currentNode, nil
	}

	// Handle GROUP BY with aggregation
	var (
		groupCols  []string
		windowSpec *ir.WindowSpec
		timeWindowSpec *ir.TimeWindowSpec
	)

	groupCols, windowSpec, timeWindowSpec, err = parseGroupByWithTimeWindow(sel.GroupBy)
	if err != nil {
		return nil, err
	}

	if timeWindowSpec != nil {
		aggs, err := findAggregatesFromQuery(rawQuery)
		if err != nil {
			return nil, err
		}
		if len(aggs) != 1 {
			return nil, errors.New("time-window GROUP BY supports exactly one aggregate")
		}
		aggName := strings.ToUpper(strings.TrimSpace(aggs[0].Name))
		aggCol := strings.TrimSpace(aggs[0].Col)
		if aggName == "COUNT" && aggCol == "*" {
			aggCol = ""
		}
		outputCol := extractAggAliasFromQuery(rawQuery, aggs[0])
		if strings.TrimSpace(outputCol) == "" {
			outputCol = strings.ToLower(aggName) + "_" + strings.ReplaceAll(aggs[0].Col, " ", "")
		}

		wa := &ir.LogicalWindowAgg{
			AggName:        aggName,
			AggCol:         aggCol,
			PartitionBy:    groupCols,
			TimeWindowSpec: timeWindowSpec,
			OutputCol:      outputCol,
			Input:          currentNode,
		}
		return wa, nil
	}

	// Find aggregates from query string
	aggs, err := findAggregatesFromQuery(sel.String())
	if err != nil {
		return nil, err
	}
	if len(aggs) > 1 {
		for _, a := range aggs {
			name := strings.ToUpper(a.Name)
			if name != "SUM" && name != "COUNT" {
				return nil, errors.New("multiple aggregate functions not supported yet")
			}
			if name == "COUNT" && strings.TrimSpace(a.Col) == "*" {
				return nil, errors.New("COUNT(*) cannot be combined with other aggregates yet")
			}
		}
	}

	// Build GroupAgg with input from current node
	lg := &ir.LogicalGroupAgg{
		Keys:       groupCols,
		WindowSpec: windowSpec,
		Input:      currentNode,
	}
	if len(aggs) == 1 {
		lg.AggName = aggs[0].Name
		if strings.ToUpper(aggs[0].Name) == "COUNT" && strings.TrimSpace(aggs[0].Col) == "*" {
			lg.AggCol = ""
		} else {
			lg.AggCol = aggs[0].Col
		}
	} else {
		lg.Aggs = make([]ir.AggSpec, 0, len(aggs))
		for _, a := range aggs {
			col := a.Col
			if strings.ToUpper(a.Name) == "COUNT" && strings.TrimSpace(col) == "*" {
				col = ""
			}
			lg.Aggs = append(lg.Aggs, ir.AggSpec{Name: a.Name, Col: col})
		}
	}

	return lg, nil
}

// parseJoinConditions parses ON clause into JoinConditions
func parseJoinConditions(onExpr ast.Expr) ([]ir.JoinCondition, error) {
	if onExpr == nil {
		return nil, errors.New("JOIN requires ON clause")
	}

	// Support conjunctions: a.x=b.x AND a.y=b.y
	if binExpr, ok := onExpr.(*ast.BinaryExpr); ok {
		switch strings.ToUpper(strings.TrimSpace(binExpr.Operator)) {
		case "AND":
			left, err := parseJoinConditions(binExpr.Left)
			if err != nil {
				return nil, err
			}
			right, err := parseJoinConditions(binExpr.Right)
			if err != nil {
				return nil, err
			}
			return append(left, right...), nil
		case "=":
			leftCol := binExpr.Left.String()
			rightCol := binExpr.Right.String()
			// Remove quotes added by tree-sitter
			leftCol = strings.Trim(leftCol, "'\"")
			rightCol = strings.Trim(rightCol, "'\"")

			condition := ir.JoinCondition{LeftCol: leftCol, RightCol: rightCol}
			return []ir.JoinCondition{condition}, nil
		default:
			return nil, errors.New("JOIN only supports equi-join (=) and AND of equi-joins")
		}
	}

	return nil, errors.New("JOIN only supports equi-join (=) and AND of equi-joins")
}

func parseJoinConditionsFromSQL(sql string) ([]ir.JoinCondition, error) {
	upper := strings.ToUpper(sql)

	// Find ON keyword at word boundary.
	onIdx := -1
	depth := 0
	for i := 0; i < len(sql)-1; i++ {
		switch sql[i] {
		case '(':
			depth++
		case ')':
			if depth > 0 {
				depth--
			}
		}
		if depth != 0 {
			continue
		}
		if i+2 <= len(sql) && upper[i:i+2] == "ON" {
			leftOK := i == 0 || !isWordChar(upper[i-1])
			rightOK := i+2 == len(sql) || !isWordChar(upper[i+2])
			if leftOK && rightOK {
				onIdx = i
				break
			}
		}
	}
	if onIdx == -1 {
		return nil, errors.New("JOIN requires ON clause")
	}
	start := onIdx + len("ON")
	end := len(sql)
	// Stop at the next clause keyword (depth-insensitive; join ON is expected simple).
	for _, kw := range []string{" WHERE ", " GROUP BY ", " ORDER BY ", " LIMIT ", " HAVING "} {
		if idx := strings.Index(upper[start:], kw); idx != -1 {
			abs := start + idx
			if abs < end {
				end = abs
			}
		}
	}
	condSQL := strings.TrimSpace(sql[start:end])
	if condSQL == "" {
		return nil, errors.New("JOIN requires ON clause")
	}

	parts := splitByAndOutsideParens(condSQL)
	conds := make([]ir.JoinCondition, 0, len(parts))
	for _, p := range parts {
		expr := strings.TrimSpace(p)
		if expr == "" {
			continue
		}
		left, right, ok := splitOnceOutsideParens(expr, '=')
		if !ok {
			return nil, errors.New("JOIN only supports equi-join (=) and AND of equi-joins")
		}
		left = strings.Trim(strings.TrimSpace(left), "'\"")
		right = strings.Trim(strings.TrimSpace(right), "'\"")
		conds = append(conds, ir.JoinCondition{LeftCol: left, RightCol: right})
	}
	if len(conds) == 0 {
		return nil, errors.New("JOIN only supports equi-join (=) and AND of equi-joins")
	}
	return conds, nil
}

func splitByAndOutsideParens(s string) []string {
	upper := strings.ToUpper(s)
	var parts []string
	depth := 0
	start := 0
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '(':
			depth++
		case ')':
			if depth > 0 {
				depth--
			}
		}
		if depth != 0 {
			continue
		}
		if i+3 <= len(s) && upper[i:i+3] == "AND" {
			// Ensure word boundary
			leftOK := i == 0 || !isWordChar(upper[i-1])
			rightOK := i+3 == len(s) || !isWordChar(upper[i+3])
			if leftOK && rightOK {
				parts = append(parts, s[start:i])
				start = i + 3
				i += 2
			}
		}
	}
	parts = append(parts, s[start:])
	return parts
}

func splitOnceOutsideParens(s string, sep byte) (string, string, bool) {
	depth := 0
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '(':
			depth++
		case ')':
			if depth > 0 {
				depth--
			}
		default:
			if depth == 0 && s[i] == sep {
				return s[:i], s[i+1:], true
			}
		}
	}
	return "", "", false
}

func isWordChar(c byte) bool {
	return (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_'
}
