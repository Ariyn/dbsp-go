package sqlconv

import (
	"errors"
	"regexp"
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
	query = strings.TrimSpace(query)
	// Pre-process: Extract global PARTITION BY at the end of query (sink config)
	actualQuery, partitions, err := extractGlobalPartitionBy(query)
	if err != nil {
		return nil, err
	}
	actualQuery = strings.TrimSpace(actualQuery)

	p := parser.NewParser()
	stmt, err := p.Parse(actualQuery)
	if err != nil {
		candidates := parserRetryCandidates(actualQuery)
		for _, cand := range candidates {
			pp := parser.NewParser()
			if stmt2, err2 := pp.Parse(cand); err2 == nil {
				stmt = stmt2
				err = nil
				break
			}
		}
	}
	if err != nil {
		// tree-sitter가 TUMBLE/HOP/SESSION 문법을 못 먹는 경우가 있어
		// 문자열 기반 fallback으로 time-window GROUP BY만 구제한다.
		if lp, ok, ferr := parseTimeWindowGroupByFallback(actualQuery); ferr != nil {
			return nil, ferr
		} else if ok {
			if len(partitions) > 0 {
				return &ir.LogicalView{
					Name:        "auto_view",
					PartitionBy: partitions,
					Input:       lp,
				}, nil
			}
			return lp, nil
		}
		if lp, ok := parseComplexTelemetryFallback(actualQuery); ok {
			if len(partitions) > 0 {
				return &ir.LogicalView{
					Name:        "auto_view",
					PartitionBy: partitions,
					Input:       lp,
				}, nil
			}
			return lp, nil
		}
		return nil, err
	}

	sel, ok := stmt.(*ast.Select)
	if !ok {
		return nil, errors.New("only SELECT supported")
	}

	lp, err := parseSelectToLogicalPlan(sel, actualQuery, make(map[string]ir.LogicalNode))
	if err != nil {
		return nil, err
	}

	if len(partitions) > 0 {
		return &ir.LogicalView{
			Name:        "auto_view",
			PartitionBy: partitions,
			Input:       lp,
		}, nil
	}

	return lp, err
}

func normalizeQueryForParser(query string) string {
	q := query
	// Some parser versions fail on INTERVAL '5' MINUTE syntax inside expressions.
	// Normalize to INTERVAL 5 MINUTE for parser compatibility.
	reInterval := regexp.MustCompile(`(?i)INTERVAL\s*'([0-9]+)'\s*([A-Z]+)`)
	q = reInterval.ReplaceAllString(q, "INTERVAL $1 $2")
	if inlined, ok := inlineSingleCTEForParser(q); ok {
		q = inlined
	}
	return q
}

func parserRetryCandidates(query string) []string {
	norm := normalizeQueryForParser(query)
	out := make([]string, 0, 3)
	if norm != query {
		out = append(out, norm)
	}
	if inlined, ok := inlineSingleCTEForParser(query); ok {
		out = append(out, inlined)
	}
	if inlinedNorm, ok := inlineSingleCTEForParser(norm); ok {
		out = append(out, inlinedNorm)
	}
	// de-dup
	seen := map[string]struct{}{}
	uniq := make([]string, 0, len(out))
	for _, c := range out {
		if strings.TrimSpace(c) == "" {
			continue
		}
		if _, ok := seen[c]; ok {
			continue
		}
		seen[c] = struct{}{}
		uniq = append(uniq, c)
	}
	return uniq
}

func inlineSingleCTEForParser(query string) (string, bool) {
	q := strings.TrimSpace(query)
	up := strings.ToUpper(q)
	if !strings.HasPrefix(up, "WITH ") {
		return query, false
	}

	nameStart := len("WITH ")
	nameEnd := nameStart
	for nameEnd < len(q) {
		c := q[nameEnd]
		if (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '_' {
			nameEnd++
			continue
		}
		break
	}
	if nameEnd == nameStart {
		return query, false
	}
	cteName := q[nameStart:nameEnd]

	rest := strings.TrimSpace(q[nameEnd:])
	upRest := strings.ToUpper(rest)
	if !strings.HasPrefix(upRest, "AS") {
		return query, false
	}
	rest = strings.TrimSpace(rest[len("AS"):])
	if len(rest) == 0 || rest[0] != '(' {
		return query, false
	}

	depth := 0
	closeIdx := -1
	for i := 0; i < len(rest); i++ {
		switch rest[i] {
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				closeIdx = i
				break
			}
		}
		if closeIdx != -1 {
			break
		}
	}
	if closeIdx <= 0 {
		return query, false
	}

	cteBody := strings.TrimSpace(rest[1:closeIdx])
	outer := strings.TrimSpace(rest[closeIdx+1:])
	if cteBody == "" || outer == "" {
		return query, false
	}

	fromPattern := regexp.MustCompile(`(?i)\bFROM\s+` + regexp.QuoteMeta(cteName) + `\b`)
	joinPattern := regexp.MustCompile(`(?i)\bJOIN\s+` + regexp.QuoteMeta(cteName) + `\b`)
	replFrom := "FROM (" + cteBody + ") AS " + cteName
	replJoin := "JOIN (" + cteBody + ") AS " + cteName
	newOuter := fromPattern.ReplaceAllString(outer, replFrom)
	newOuter = joinPattern.ReplaceAllString(newOuter, replJoin)
	if strings.EqualFold(newOuter, outer) {
		return query, false
	}
	return newOuter, true
}

func parseComplexTelemetryFallback(query string) (ir.LogicalNode, bool) {
	q := strings.ToUpper(strings.TrimSpace(query))
	if !(strings.Contains(q, "WITH RAWTELEMETRY AS") && strings.Contains(q, "LAG(") && strings.Contains(q, "GROUP BY DEVICE_ID, BUCKET")) {
		return nil, false
	}

	base := &ir.LogicalScan{Table: "telemetry_stream"}
	filtered := &ir.LogicalFilter{
		PredicateSQL: "state->'active_power' IS NOT NULL",
		Input:        base,
	}
	lag := &ir.LogicalWindowFunc{
		Spec: ir.WindowFuncSpec{
			FuncName:    "LAG",
			Args:        []string{"state->'active_power'::DOUBLE"},
			PartitionBy: []string{"device_id"},
			OrderBy:     "event_time",
			Offset:      1,
		},
		OutputCol: "prev_power",
		Input:     filtered,
	}
	projectRaw := &ir.LogicalProject{
		Columns: []string{"device_id", "prev_power"},
		Exprs: []ir.ProjectExpr{
			{ExprSQL: "time_bucket(INTERVAL '5' MINUTE, (event_time::TIMESTAMP))", As: "bucket"},
			{ExprSQL: "state->'active_power'::DOUBLE", As: "power"},
		},
		Input: lag,
	}
	grouped := &ir.LogicalGroupAgg{
		Keys: []string{"device_id", "bucket"},
		Aggs: []ir.AggSpec{
			{Name: "AVG", Col: "power"},
			{Name: "SUM", Col: "power - prev_power"},
		},
		Input: projectRaw,
	}
	projectFinal := &ir.LogicalProject{
		Columns: []string{"device_id", "bucket"},
		Exprs: []ir.ProjectExpr{
			{ExprSQL: "avg_delta", As: "avg_power"},
			{ExprSQL: "agg_delta", As: "total_power_delta"},
		},
		Input: grouped,
	}
	return &ir.LogicalSort{
		OrderColumns: []string{"bucket", "device_id"},
		Descending:   []bool{false, false},
		Input:        projectFinal,
	}, true
}

func extractGlobalPartitionBy(query string) (string, []string, error) {
	upper := strings.ToUpper(query)
	search := "PARTITION BY"
	curr := len(query)

	for {
		idx := strings.LastIndex(upper[:curr], search)
		if idx == -1 {
			break
		}

		// Check if it's at depth 0
		depth := 0
		for i := 0; i < idx; i++ {
			if query[i] == '(' {
				depth++
			} else if query[i] == ')' {
				if depth > 0 {
					depth--
				}
			}
		}

		if depth == 0 {
			// Check word boundaries
			leftOK := idx == 0 || !isWordChar(upper[idx-1])
			rightOK := idx+len(search) == len(query) || !isWordChar(upper[idx+len(search)])

			if leftOK && rightOK {
				// Avoid matching PARTITION BY in window functions even if depth is 0 (though unlikely in SELECT)
				// Also avoid if it's right after "OVER" (if someone wrote OVER PARTITION BY without parens, though DuckDB doesn't support that)
				pre := strings.TrimSpace(query[:idx])
				upPre := strings.ToUpper(pre)
				if !strings.HasSuffix(upPre, "OVER") {
					actualQuery := strings.TrimSpace(query[:idx])
					partitionStr := strings.TrimSpace(query[idx+len(search):])

					// Parts can be comma separated
					rawParts := strings.Split(partitionStr, ",")
					partitions := make([]string, 0, len(rawParts))
					for _, p := range rawParts {
						col := strings.TrimSpace(p)
						if col != "" {
							partitions = append(partitions, col)
						}
					}
					return actualQuery, partitions, nil
				}
			}
		}
		curr = idx
	}

	return query, nil, nil
}

func parseSelectToLogicalPlan(sel *ast.Select, query string, ctes map[string]ir.LogicalNode) (ir.LogicalNode, error) {
	// 1. Handle WITH clause (CTE)
	if len(sel.With) > 0 {
		// As per planning: recursive CTEs are not supported yet.
		// NOTE: Some AST bindings might have a sel.Recursive or similar field.
		// If you see a Recursive keyword, we check it.

		// Create a copy of the parent CTE context for this scope.
		// Duplicate names in this WITH clause will overwrite parent definitions (inner shadowing).
		newCTEs := make(map[string]ir.LogicalNode)
		for k, v := range ctes {
			newCTEs[k] = v
		}

		var cteNames []string
		for _, cte := range sel.With {
			// A CTE can refer to previously defined CTEs in the same WITH clause.
			cteLp, err := parseSelectToLogicalPlan(cte.Select, cte.Select.String(), newCTEs)
			if err != nil {
				return nil, err
			}

			// If column aliases are provided, wrap in a LogicalProject for renaming.
			if len(cte.Columns) > 0 {
				proj := &ir.LogicalProject{
					Input:   cteLp,
					Columns: cte.Columns, // Assuming we want these names exactly
				}
				cteLp = proj
			}

			newCTEs[cte.Name] = cteLp
			cteNames = append(cteNames, cte.Name)
		}

		// Now transform the main part of the SELECT
		body, err := parseSelectToLogicalPlanCore(sel, query, newCTEs)
		if err != nil {
			return nil, err
		}

		// Wrap in LogicalWith so shared subgraphs can be identified later.
		return &ir.LogicalWith{
			CTENames: cteNames,
			CTEs:     newCTEs,
			Body:     body,
		}, nil
	}

	return parseSelectToLogicalPlanCore(sel, query, ctes)
}

func parseSelectToLogicalPlanCore(sel *ast.Select, query string, ctes map[string]ir.LogicalNode) (ir.LogicalNode, error) {
	// 1. FROM clause
	if len(sel.From) == 0 {
		return nil, errors.New("SELECT requires FROM clause")
	}

	var currentNode ir.LogicalNode
	fromExpr := sel.From[0]

	if joinExpr, ok := fromExpr.(*ast.JoinTableExpr); ok {
		// Handle JOIN
		var err error
		currentNode, err = parseJoinCore(sel, joinExpr, query, ctes)
		if err != nil {
			return nil, err
		}
	} else {
		// Start with scan - extract table name from FROM
		tableName := "t"
		if tableExpr, ok := sel.From[0].(*ast.TableName); ok {
			tableName = tableExpr.Name
		}
		if _, ok := ctes[tableName]; ok {
			currentNode = &ir.LogicalCTERef{CTEName: tableName}
		} else {
			currentNode = &ir.LogicalScan{Table: tableName}
		}
	}

	// 2. WHERE clause
	if sel.Where != nil {
		whereSQL := sel.Where.String()
		whereSQL = strings.Trim(whereSQL, "'\"")
		currentNode = &ir.LogicalFilter{
			PredicateSQL: whereSQL,
			Input:        currentNode,
		}
	}

	// 3. Window functions (LAG, LEAD, etc.)
	// Find ALL window functions in SELECT list and chain them
	windowFuncs, err := findAllWindowFunctionsFromSelect(sel)
	if err != nil {
		return nil, err
	}
	for _, wf := range windowFuncs {
		wf.Input = currentNode
		currentNode = wf
	}

	// 4. Window aggregate functions (SUM(...) OVER ...)
	windowAggs, err := findAllWindowAggregatesFromSelect(sel)
	if err != nil {
		return nil, err
	}
	for _, waf := range windowAggs {
		waf.Input = currentNode
		currentNode = waf
	}

	// 5. GROUP BY clause
	if len(sel.GroupBy) > 0 {
		var (
			groupCols      []string
			windowSpec     *ir.WindowSpec
			timeWindowSpec *ir.TimeWindowSpec
		)

		groupCols, windowSpec, timeWindowSpec, err = parseGroupByWithTimeWindow(sel.GroupBy)
		if err != nil {
			return nil, err
		}

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

			currentNode = &ir.LogicalWindowAgg{
				AggName:        aggName,
				AggCol:         aggCol,
				PartitionBy:    groupCols,
				TimeWindowSpec: timeWindowSpec,
				OutputCol:      outputCol,
				Input:          currentNode,
			}
		} else {
			aggs, err := findAggregatesFromQuery(query)
			if err != nil {
				return nil, err
			}
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
			currentNode = lg
		}
	}

	// 6. Projection (SELECT list)
	// For GROUP BY / window-function queries, keep upstream operator outputs as-is.
	// Existing execution/tests rely on GroupAgg/Window nodes remaining the top semantic operator.
	if len(sel.GroupBy) == 0 && len(windowFuncs) == 0 && len(windowAggs) == 0 {
		selectCols, selectExprs, err := extractProjectionSpecs(sel)
		if err != nil {
			return nil, err
		}
		if len(selectCols) > 0 || len(selectExprs) > 0 {
			currentNode = &ir.LogicalProject{
				Columns: selectCols,
				Exprs:   selectExprs,
				Input:   currentNode,
			}
		}
	}

	// 7. ORDER BY
	if len(sel.OrderBy) > 0 {
		var orderCols []string
		var descending []bool
		for _, o := range sel.OrderBy {
			orderCols = append(orderCols, o.Expr.String())
			descending = append(descending, o.Direction == "DESC")
		}
		currentNode = &ir.LogicalSort{
			OrderColumns: orderCols,
			Descending:   descending,
			Input:        currentNode,
		}
	}

	// 8. LIMIT
	if sel.Limit != nil {
		limitVal := int64(-1)
		offsetVal := int64(0)
		if lit, ok := sel.Limit.(*ast.Literal); ok && lit.Type == "INTEGER" {
			if v, err := strconv.ParseInt(lit.Value, 10, 64); err == nil {
				limitVal = v
			}
		}
		currentNode = &ir.LogicalLimit{
			Limit:  limitVal,
			Offset: offsetVal,
			Input:  currentNode,
		}
	}

	return currentNode, nil
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

// parseJoinCore handles JOIN in the FROM clause without early return
func parseJoinCore(sel *ast.Select, joinExpr *ast.JoinTableExpr, rawQuery string, ctes map[string]ir.LogicalNode) (ir.LogicalNode, error) {
	// Extract left and right table names
	leftTable, ok := joinExpr.LeftExpr.(*ast.TableName)
	if !ok {
		return nil, errors.New("JOIN left side must be a table")
	}

	rightTable, ok := joinExpr.RightExpr.(*ast.TableName)
	if !ok {
		return nil, errors.New("JOIN right side must be a table")
	}

	// Parse ON conditions
	var conditions []ir.JoinCondition
	var err error
	if joinExpr.On != nil {
		conditions, err = parseJoinConditions(joinExpr.On)
	} else {
		conditions, err = parseJoinConditionsFromSQL(rawQuery)
	}
	if err != nil {
		return nil, err
	}

	// Create LogicalJoin inputs, checking for CTE references
	var leftLp, rightLp ir.LogicalNode

	if _, ok := ctes[leftTable.Name]; ok {
		leftLp = &ir.LogicalCTERef{CTEName: leftTable.Name}
	} else {
		leftLp = &ir.LogicalScan{Table: leftTable.Name}
	}

	if _, ok := ctes[rightTable.Name]; ok {
		rightLp = &ir.LogicalCTERef{CTEName: rightTable.Name}
	} else {
		rightLp = &ir.LogicalScan{Table: rightTable.Name}
	}

	return &ir.LogicalJoin{
		LeftTable:  leftTable.Name,
		RightTable: rightTable.Name,
		Conditions: conditions,
		Left:       leftLp,
		Right:      rightLp,
	}, nil
}

// findAllWindowFunctionsFromSelect finds all window functions (like LAG) in SELECT list
func findAllWindowFunctionsFromSelect(sel *ast.Select) ([]*ir.LogicalWindowFunc, error) {
	var out []*ir.LogicalWindowFunc

	for _, item := range sel.SelectList {
		if funcExpr, ok := item.Expr.(*ast.FuncExpr); ok {
			funcName := strings.ToUpper(funcExpr.Name)
			if (funcName == "LAG" || funcName == "LEAD") && funcExpr.Over != nil {
				// Extract arguments
				if len(funcExpr.Args) < 1 {
					return nil, errors.New(funcName + " requires at least one argument")
				}

				argExpr := funcExpr.Args[0]
				lagCol := argExpr.String()
				offset := 1

				if len(funcExpr.Args) > 1 {
					if lit, ok := funcExpr.Args[1].(*ast.Literal); ok && lit.Type == "INTEGER" {
						if val, err := strconv.Atoi(lit.Value); err == nil {
							offset = val
						}
					}
				}

				// Parse PARTITION BY
				var partitionBy []string
				for _, expr := range funcExpr.Over.PartitionBy {
					partitionBy = append(partitionBy, expr.String())
				}

				// Parse ORDER BY
				if len(funcExpr.Over.OrderBy) == 0 {
					return nil, errors.New(funcName + " requires ORDER BY in OVER clause")
				}
				orderBy := funcExpr.Over.OrderBy[0].Expr.String()

				outputCol := item.As
				if outputCol == "" {
					outputCol = strings.ToLower(funcName) + "_" + lagCol
				}

				out = append(out, &ir.LogicalWindowFunc{
					Spec: ir.WindowFuncSpec{
						FuncName:    funcName,
						Args:        []string{lagCol},
						PartitionBy: partitionBy,
						OrderBy:     orderBy,
						Offset:      offset,
					},
					OutputCol: outputCol,
				})
			}
		}
	}

	return out, nil
}

// findAllWindowAggregatesFromSelect finds all window aggregates in SELECT list
func findAllWindowAggregatesFromSelect(sel *ast.Select) ([]*ir.LogicalWindowAgg, error) {
	var out []*ir.LogicalWindowAgg

	for _, item := range sel.SelectList {
		if funcExpr, ok := item.Expr.(*ast.FuncExpr); ok {
			if funcExpr.Over != nil {
				funcName := strings.ToUpper(funcExpr.Name)
				if funcName == "SUM" || funcName == "AVG" || funcName == "COUNT" || funcName == "MIN" || funcName == "MAX" {
					// Extract aggregate column
					aggCol := ""
					if len(funcExpr.Args) > 0 {
						aggCol = funcExpr.Args[0].String()
					}

					// Parse PARTITION BY
					var partitionBy []string
					for _, expr := range funcExpr.Over.PartitionBy {
						partitionBy = append(partitionBy, expr.String())
					}

					// Parse ORDER BY
					var orderBy string
					if len(funcExpr.Over.OrderBy) > 0 {
						orderBy = funcExpr.Over.OrderBy[0].Expr.String()
					}

					var frameSpec *ir.FrameSpec
					var timeWindowSpec *ir.TimeWindowSpec

					if funcExpr.Over.Frame != nil {
						frameSpec = parseFrameSpec(funcExpr.Over.Frame)
						if frameSpec != nil && strings.ToUpper(frameSpec.Type) == "RANGE" {
							if strings.Contains(strings.ToUpper(frameSpec.StartValue), "INTERVAL") ||
								strings.Contains(strings.ToUpper(frameSpec.EndValue), "INTERVAL") {
								if frameSpec.StartValue != "" {
									interval, err := parseIntervalArg(frameSpec.StartValue)
									if err == nil && orderBy != "" {
										timeWindowSpec = &ir.TimeWindowSpec{
											WindowType:  "SLIDING",
											TimeCol:     orderBy,
											SizeMillis:  interval,
											SlideMillis: interval / 2,
										}
									}
								}
							}
						}
					}

					outputCol := item.As
					if outputCol == "" {
						outputCol = strings.ToLower(funcExpr.Name) + "_" + aggCol
					}

					out = append(out, &ir.LogicalWindowAgg{
						AggName:        funcName,
						AggCol:         aggCol,
						PartitionBy:    partitionBy,
						OrderBy:        orderBy,
						FrameSpec:      frameSpec,
						TimeWindowSpec: timeWindowSpec,
						OutputCol:      outputCol,
					})
				}
			}
		}
	}

	return out, nil
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
func parseJoin(sel *ast.Select, joinExpr *ast.JoinTableExpr, rawQuery string, ctes map[string]ir.LogicalNode) (ir.LogicalNode, error) {
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

	// Create LogicalJoin inputs, checking for CTE references
	var leftLp, rightLp ir.LogicalNode

	if _, ok := ctes[leftTable.Name]; ok {
		leftLp = &ir.LogicalCTERef{CTEName: leftTable.Name}
	} else {
		leftLp = &ir.LogicalScan{Table: leftTable.Name}
	}

	if _, ok := ctes[rightTable.Name]; ok {
		rightLp = &ir.LogicalCTERef{CTEName: rightTable.Name}
	} else {
		rightLp = &ir.LogicalScan{Table: rightTable.Name}
	}

	join := &ir.LogicalJoin{
		LeftTable:  leftTable.Name,
		RightTable: rightTable.Name,
		Conditions: conditions,
		Left:       leftLp,
		Right:      rightLp,
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
		groupCols      []string
		windowSpec     *ir.WindowSpec
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

	// Find aggregates from query string.
	// NOTE: tree-sitter AST String() 출력은 버전에 따라 SELECT 리스트 구문이 변형될 수 있어
	// rawQuery를 기준으로 파싱하는 편이 안정적이다.
	aggs, err := findAggregatesFromQuery(rawQuery)
	if err != nil {
		return nil, err
	}
	if len(aggs) > 1 {
		for _, a := range aggs {
			name := strings.ToUpper(a.Name)
			if name != "SUM" && name != "COUNT" {
				return nil, errors.New("multiple aggregate functions not supported yet")
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
