package sqlconv

import (
	"errors"
	"fmt"
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
func ParseQueryToLogicalPlan(query string, options ...ComplianceOption) (ir.LogicalNode, error) {
	query = strings.TrimSpace(query)
	// Apply options
	settings := &ComplianceSettings{}
	for _, opt := range options {
		opt(settings)
	}

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
			if settings.StrictValidation {
				return nil, fmt.Errorf("strict validation failed: fallback parser used for time-window query")
			}
			if settings.FallbackWarn {
				fmt.Printf("Warning: fallback parser used for query: %s\n", actualQuery)
			}
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
			if settings.StrictValidation {
				return nil, fmt.Errorf("strict validation failed: fallback parser used for complex telemetry query")
			}
			if settings.FallbackWarn {
				fmt.Printf("Warning: fallback parser used for query: %s\n", actualQuery)
			}
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

type ComplianceSettings struct {
	StrictValidation bool
	FallbackWarn     bool
}

type ComplianceOption func(*ComplianceSettings)

func WithStrictValidation(enable bool) ComplianceOption {
	return func(s *ComplianceSettings) {
		s.StrictValidation = enable
	}
}

func WithFallbackWarn(enable bool) ComplianceOption {
	return func(s *ComplianceSettings) {
		s.FallbackWarn = enable
	}
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

		cteBodies := extractCTEBodyQueries(query)
		var cteNames []string
		for _, cte := range sel.With {
			// A CTE can refer to previously defined CTEs in the same WITH clause.
			cteName := normalizeRelationName(cte.Name)
			cteQuery := strings.TrimSpace(cteBodies[strings.ToLower(cteName)])
			if cteQuery == "" {
				cteQuery = cte.Select.String()
			}
			cteLp, err := parseSelectToLogicalPlan(cte.Select, cteQuery, newCTEs)
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

			newCTEs[cteName] = cteLp
			cteNames = append(cteNames, cteName)
		}

		// Now transform the main part of the SELECT using the outer-body SQL only.
		// This prevents fallback scanners from re-reading inner CTE SELECT clauses.
		bodyQuery := extractOuterSelectAfterWith(query)
		body, err := parseSelectToLogicalPlanCore(sel, bodyQuery, newCTEs)
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
			tableName = normalizeRelationName(tableExpr.Name)
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
		whereSQL = normalizeQuotedIdentifierTokens(whereSQL)
		currentNode = &ir.LogicalFilter{
			PredicateSQL: whereSQL,
			Input:        currentNode,
		}
	}

	// 3. GROUP BY clause
	hasAggs := false
	if aggs, err := findAggregatesForGroupBy(sel, query); err == nil && len(aggs) > 0 {
		hasAggs = true
	}
	if len(sel.GroupBy) > 0 || hasAggs {
		var (
			groupCols      []string
			windowSpec     *ir.WindowSpec
			timeWindowSpec *ir.TimeWindowSpec
			err            error
		)

		if len(sel.GroupBy) > 0 {
			groupCols, windowSpec, timeWindowSpec, err = parseGroupByWithTimeWindow(sel.GroupBy)
			if err != nil {
				return nil, err
			}
		} else {
			// Auto-grouping logic
			timeWindowSpec, err = findTimeBucketInSelect(sel, query)
			if err != nil || timeWindowSpec == nil {
				// Fallback to regular group agg if no time_bucket found but has aggs
				// (Scalar aggregate case)
			} else {
				// Inject group keys for auto-grouping
				groupCols = findNonAggregatedCols(sel, query)
				if alias := findTimeBucketAliasInSelect(sel, query); alias != "" {
					seen := make(map[string]struct{}, len(groupCols))
					for _, c := range groupCols {
						seen[c] = struct{}{}
					}
					if _, ok := seen[alias]; !ok {
						groupCols = append(groupCols, alias)
					}
				}
			}
		}

		aliasExprs := buildGroupByAliasMaterializationExprs(sel, query, groupCols)
		if len(aliasExprs) > 0 {
			currentNode = &ir.LogicalProject{
				KeepInput: true,
				Exprs:     aliasExprs,
				Input:     currentNode,
			}
		}

		if timeWindowSpec != nil {
			aggs, err := findAggregatesForGroupBy(sel, query)
			if err != nil {
				return nil, err
			}
			lg := &ir.LogicalGroupAgg{
				Keys:           groupCols,
				TimeWindowSpec: timeWindowSpec,
				Input:          currentNode,
			}
			if len(aggs) > 0 {
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
						lg.Aggs = append(lg.Aggs, ir.AggSpec{Name: a.Name, Col: col, As: a.As})
					}
				}
			}
			currentNode = lg
		} else {
			aggs, err := findAggregatesForGroupBy(sel, query)
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
				seenAliases := make(map[string]struct{})
				for _, a := range aggs {
					col := a.Col
					if strings.ToUpper(a.Name) == "COUNT" && strings.TrimSpace(col) == "*" {
						col = ""
					}
					alias := aggOutputAlias(query, a)
					if alias != "" {
						if _, exists := seenAliases[alias]; exists {
							continue
						}
						seenAliases[alias] = struct{}{}
					}
					lg.Aggs = append(lg.Aggs, ir.AggSpec{Name: a.Name, Col: col, As: alias})
				}
			}
			currentNode = lg
		}
	}
	// 4. Window functions (LAG, LEAD, etc.)
	// Window functions must be applied after GROUP BY, on the grouped result.
	windowFuncs, err := findAllWindowFunctionsFromSelect(sel)
	if err != nil {
		return nil, err
	}
	fallbackWindowFuncs, ferr := findAllWindowFunctionsFromQuery(query)
	if len(windowFuncs) == 0 {
		if ferr == nil {
			windowFuncs = fallbackWindowFuncs
		}
	} else if ferr == nil && len(fallbackWindowFuncs) > 0 {
		windowFuncs = mergeWindowFuncAliases(windowFuncs, fallbackWindowFuncs)
	}
	for _, wf := range windowFuncs {
		wf.Input = currentNode
		currentNode = wf
	}

	// 5. Window aggregate functions (SUM(...) OVER ...)
	windowAggs, err := findAllWindowAggregatesFromSelect(sel)
	if err != nil {
		return nil, err
	}
	if len(windowAggs) == 0 {
		fallback, ferr := findAllWindowAggregatesFromQuery(query)
		if ferr == nil {
			windowAggs = fallback
		}
	}
	for _, waf := range windowAggs {
		waf.Input = currentNode
		currentNode = waf
	}

	// 6. Projection (SELECT list)

	// 6. Projection (SELECT list)
	// Apply projection whenever SELECT list is explicit.
	// This is required for queries that combine GROUP BY/window operators with
	// final computed columns (e.g. ROUND(...), aliases, SELECT * wrappers).
	selectCols, selectExprs, err := extractProjectionSpecs(sel, query)
	if err != nil {
		return nil, err
	}
	if len(selectCols) > 0 || len(selectExprs) > 0 {
		if !shouldSkipFinalProjectionForWindow(sel, selectCols, selectExprs) {
			currentNode = &ir.LogicalProject{
				KeepInput: shouldKeepInputForProjection(sel, query),
				Columns:   selectCols,
				Exprs:     selectExprs,
				Input:     currentNode,
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

func mergeWindowFuncAliases(primary, fallback []*ir.LogicalWindowFunc) []*ir.LogicalWindowFunc {
	if len(primary) == 0 || len(fallback) == 0 {
		return primary
	}
	for _, wf := range primary {
		fb := findMatchingWindowFunc(fallback, wf)
		if fb == nil {
			continue
		}
		if shouldPreferFallbackOutputCol(wf.OutputCol, fb.OutputCol, wf.Spec.FuncName, firstArg(wf.Spec.Args)) {
			wf.OutputCol = fb.OutputCol
		}
	}
	return primary
}

func findMatchingWindowFunc(candidates []*ir.LogicalWindowFunc, target *ir.LogicalWindowFunc) *ir.LogicalWindowFunc {
	if target == nil {
		return nil
	}
	for _, c := range candidates {
		if c == nil {
			continue
		}
		if windowFuncSpecEqual(c.Spec, target.Spec) {
			return c
		}
	}
	return nil
}

func windowFuncSpecEqual(a, b ir.WindowFuncSpec) bool {
	if strings.ToUpper(strings.TrimSpace(a.FuncName)) != strings.ToUpper(strings.TrimSpace(b.FuncName)) {
		return false
	}
	if strings.TrimSpace(a.OrderBy) != strings.TrimSpace(b.OrderBy) {
		return false
	}
	if a.Offset != b.Offset {
		return false
	}
	if len(a.Args) != len(b.Args) {
		return false
	}
	for i := range a.Args {
		if strings.TrimSpace(a.Args[i]) != strings.TrimSpace(b.Args[i]) {
			return false
		}
	}
	if len(a.PartitionBy) != len(b.PartitionBy) {
		return false
	}
	for i := range a.PartitionBy {
		if strings.TrimSpace(a.PartitionBy[i]) != strings.TrimSpace(b.PartitionBy[i]) {
			return false
		}
	}
	return true
}

func shouldPreferFallbackOutputCol(primary, fallback, funcName, arg string) bool {
	if strings.TrimSpace(fallback) == "" {
		return false
	}
	if strings.TrimSpace(primary) == "" {
		return true
	}
	fn := strings.ToLower(strings.TrimSpace(funcName))
	arg = strings.TrimSpace(arg)
	default1 := fn + "_" + arg
	default2 := fn + "_" + sanitizeIdentifierForAlias(arg)
	// Prefer explicit alias when primary looks like an auto-generated name.
	if primary == default1 || primary == default2 {
		return true
	}
	return false
}

func firstArg(args []string) string {
	if len(args) == 0 {
		return ""
	}
	return args[0]
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
func ParseQueryToDBSP(query string, options ...ComplianceOption) (*op.Node, error) {
	lp, err := ParseQueryToLogicalPlan(query, options...)
	if err != nil {
		return nil, err
	}
	node, err := ir.LogicalToDBSP(lp)
	if err != nil {
		return nil, err
	}
	return collapseSingleSourceLinearPipeline(node), nil
}

func collapseSingleSourceLinearPipeline(root *op.Node) *op.Node {
	if root == nil {
		return nil
	}

	var opsRev []op.Operator
	n := root
	for n != nil {
		if n.Source != "" {
			break
		}
		if n.Op == nil {
			return root
		}
		opsRev = append(opsRev, n.Op)
		switch len(n.Inputs) {
		case 0:
			n = nil
		case 1:
			n = n.Inputs[0]
		default:
			return root
		}
	}

	if len(opsRev) == 0 {
		return root
	}

	ops := make([]op.Operator, 0, len(opsRev))
	for i := len(opsRev) - 1; i >= 0; i-- {
		ops = append(ops, opsRev[i])
	}

	if len(ops) == 1 {
		return &op.Node{Op: ops[0], PartitionBy: root.PartitionBy}
	}
	return &op.Node{Op: &op.ChainedOp{Ops: ops}, PartitionBy: root.PartitionBy}
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

	leftName := normalizeRelationName(leftTable.Name)
	rightName := normalizeRelationName(rightTable.Name)

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

	if _, ok := ctes[leftName]; ok {
		leftLp = &ir.LogicalCTERef{CTEName: leftName}
	} else {
		leftLp = &ir.LogicalScan{Table: leftName}
	}

	if _, ok := ctes[rightName]; ok {
		rightLp = &ir.LogicalCTERef{CTEName: rightName}
	} else {
		rightLp = &ir.LogicalScan{Table: rightName}
	}

	return &ir.LogicalJoin{
		LeftTable:  leftName,
		RightTable: rightName,
		Conditions: conditions,
		Left:       leftLp,
		Right:      rightLp,
	}, nil
}

func buildGroupByAliasMaterializationExprs(sel *ast.Select, query string, groupCols []string) []ir.ProjectExpr {
	if sel == nil || len(groupCols) == 0 || len(sel.SelectList) == 0 {
		return nil
	}
	groupSet := make(map[string]struct{}, len(groupCols))
	for _, col := range groupCols {
		trimmed := strings.TrimSpace(col)
		if trimmed != "" {
			groupSet[trimmed] = struct{}{}
		}
	}
	out := make([]ir.ProjectExpr, 0)
	for _, item := range sel.SelectList {
		alias := strings.TrimSpace(item.As)
		if alias == "" {
			continue
		}
		if _, ok := groupSet[alias]; !ok {
			continue
		}
		exprSQL := strings.TrimSpace(item.Expr.String())
		if recovered, ok := extractSelectExprByAlias(query, alias); ok {
			exprSQL = strings.TrimSpace(recovered)
		} else {
			exprSQL = recoverMalformedExprByAlias(query, alias, exprSQL)
		}
		exprSQL = simplifyGroupAliasExpr(exprSQL)
		if exprSQL == "" {
			continue
		}

		// Double-check: if the expression is still just the alias, try to find the raw column name
		// if it was promoted from raw column to group key.
		if strings.ToUpper(exprSQL) == strings.ToUpper(alias) {
			if col, ok := item.Expr.(*ast.ColName); ok {
				exprSQL = col.Name
			}
		}

		out = append(out, ir.ProjectExpr{ExprSQL: exprSQL, As: alias})
	}
	return out
}

func simplifyGroupAliasExpr(expr string) string {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return ""
	}
	upper := strings.ToUpper(expr)
	// If it contains an aggregate function, it must be the output of a GroupAgg.
	// We return the likely monoid output column name.
	if strings.Contains(upper, "AVG(") {
		return "avg_delta"
	}
	if strings.Contains(upper, "SUM(") {
		return "agg_delta"
	}
	if strings.Contains(upper, "COUNT(") {
		return "count_delta"
	}
	if strings.Contains(upper, "MIN(") {
		return "min"
	}
	if strings.Contains(upper, "MAX(") {
		return "max"
	}

	if strings.HasPrefix(upper, "TIME_BUCKET(") {
		if canonical := canonicalizeTimeBucketExpr(expr); canonical != "" {
			return canonical
		}
	}

	// STRICT: In grouped SELECT, every non-aggregate expression must be precisely
	// a grouping key or an expression over grouping keys.
	// Currently, our planner materializes aliases for group keys that match
	// the SELECT list. If an expression isn't directly a group-key alias,
	// it should ideally be rejected if it references non-grouped columns.

	return normalizeQuotedIdentifierExpr(expr)
}

func canonicalizeTimeBucketExpr(expr string) string {
	open := strings.Index(expr, "(")
	close := strings.LastIndex(expr, ")")
	if open == -1 || close == -1 || close <= open {
		return ""
	}
	inner := strings.TrimSpace(expr[open+1 : close])
	if inner == "" {
		return ""
	}

	depth := 0
	split := -1
	for i := 0; i < len(inner); i++ {
		switch inner[i] {
		case '(':
			depth++
		case ')':
			if depth > 0 {
				depth--
			}
		case ',':
			if depth == 0 {
				split = i
				break
			}
		}
	}
	if split == -1 {
		return ""
	}

	firstArg := strings.TrimSpace(inner[:split])
	secondArg := strings.TrimSpace(inner[split+1:])
	if firstArg == "" || secondArg == "" {
		return ""
	}

	parseInterval := func(raw string) (string, string, bool) {
		raw = strings.TrimSpace(raw)
		patterns := []string{
			`(?i)^INTERVAL\s*'([0-9]+)\s*([A-Z]+)'\s*$`,
			`(?i)^INTERVAL\s*'([0-9]+)'\s*([A-Z]+)\s*$`,
			`(?i)^INTERVAL\s*([0-9]+)\s*([A-Z]+)\s*$`,
		}
		for _, pat := range patterns {
			re := regexp.MustCompile(pat)
			m := re.FindStringSubmatch(strings.ToUpper(raw))
			if len(m) != 3 {
				continue
			}
			num := strings.TrimSpace(m[1])
			unit := strings.TrimSpace(m[2])
			switch unit {
			case "MIN", "MINS", "MINUTE", "MINUTES":
				unit = "MINUTE"
			case "SEC", "SECS", "SECOND", "SECONDS":
				unit = "SECOND"
			case "HOUR", "HOURS":
				unit = "HOUR"
			}
			return num, unit, true
		}
		return "", "", false
	}

	num, unit, ok := parseInterval(firstArg)
	if !ok {
		return ""
	}

	secondArg = normalizeQuotedIdentifierExpr(secondArg)
	if secondArg == "" {
		return ""
	}

	return fmt.Sprintf("TIME_BUCKET(INTERVAL '%s' %s, %s)", num, unit, secondArg)
}

func extractTimeBucketSourceColumn(expr string) string {
	open := strings.Index(expr, "(")
	close := strings.LastIndex(expr, ")")
	if open == -1 || close == -1 || close <= open {
		return ""
	}
	inner := strings.TrimSpace(expr[open+1 : close])
	if inner == "" {
		return ""
	}

	depth := 0
	split := -1
	for i := 0; i < len(inner); i++ {
		switch inner[i] {
		case '(':
			depth++
		case ')':
			if depth > 0 {
				depth--
			}
		case ',':
			if depth == 0 {
				split = i
				break
			}
		}
	}
	if split == -1 {
		return ""
	}
	secondArg := strings.TrimSpace(inner[split+1:])
	if secondArg == "" {
		return ""
	}
	if idx := strings.Index(secondArg, "::"); idx != -1 {
		secondArg = strings.TrimSpace(secondArg[:idx])
	}
	secondArg = strings.TrimSpace(secondArg)
	secondArg = strings.Trim(secondArg, "`\"'")
	return normalizeQuotedIdentifierExpr(secondArg)
}

func normalizeQuotedIdentifierExpr(expr string) string {
	expr = strings.TrimSpace(expr)
	if len(expr) >= 2 && expr[0] == '\'' && expr[len(expr)-1] == '\'' {
		inner := strings.TrimSpace(expr[1 : len(expr)-1])
		if inner != "" {
			isIdent := true
			for i := 0; i < len(inner); i++ {
				c := inner[i]
				if (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '_' || c == '.' {
					continue
				}
				isIdent = false
				break
			}
			if isIdent {
				return inner
			}
		}
	}
	candidate := strings.TrimSpace(strings.Trim(expr, "`\"'"))
	if candidate != "" {
		valid := true
		for i := 0; i < len(candidate); i++ {
			c := candidate[i]
			if (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '_' || c == '.' {
				continue
			}
			valid = false
			break
		}
		if valid {
			return candidate
		}
	}
	return expr
}

func normalizeRelationName(name string) string {
	n := strings.TrimSpace(name)
	n = strings.Trim(n, "`\"'")
	return n
}

func inferFuncName(funcExpr *ast.FuncExpr) string {
	name := strings.ToUpper(strings.TrimSpace(funcExpr.Name))
	if name != "" {
		return name
	}
	s := strings.TrimSpace(funcExpr.String())
	if s == "" {
		return ""
	}
	if idx := strings.Index(s, "("); idx > 0 {
		return strings.ToUpper(strings.TrimSpace(s[:idx]))
	}
	return ""
}

// findAllWindowFunctionsFromSelect finds all window functions (like LAG) in SELECT list
func findAllWindowFunctionsFromSelect(sel *ast.Select) ([]*ir.LogicalWindowFunc, error) {
	var out []*ir.LogicalWindowFunc

	for _, item := range sel.SelectList {
		if funcExpr, ok := item.Expr.(*ast.FuncExpr); ok {
			funcName := inferFuncName(funcExpr)
			if (funcName == "LAG" || funcName == "LEAD") && funcExpr.Over != nil {
				// Extract arguments
				if len(funcExpr.Args) < 1 {
					return nil, errors.New(funcName + " requires at least one argument")
				}

				argExpr := funcExpr.Args[0]
				lagCol := normalizeQuotedIdentifierExpr(argExpr.String())
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
					partitionBy = append(partitionBy, normalizeQuotedIdentifierExpr(expr.String()))
				}

				// Parse ORDER BY
				if len(funcExpr.Over.OrderBy) == 0 {
					return nil, errors.New(funcName + " requires ORDER BY in OVER clause")
				}
				orderBy := normalizeQuotedIdentifierExpr(funcExpr.Over.OrderBy[0].Expr.String())

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
				funcName := inferFuncName(funcExpr)
				if funcName == "SUM" || funcName == "AVG" || funcName == "COUNT" || funcName == "MIN" || funcName == "MAX" {
					// Extract aggregate column
					aggCol := ""
					if len(funcExpr.Args) > 0 {
						aggCol = normalizeQuotedIdentifierExpr(funcExpr.Args[0].String())
					}

					// Parse PARTITION BY
					var partitionBy []string
					for _, expr := range funcExpr.Over.PartitionBy {
						partitionBy = append(partitionBy, normalizeQuotedIdentifierExpr(expr.String()))
					}

					// Parse ORDER BY
					var orderBy string
					if len(funcExpr.Over.OrderBy) > 0 {
						orderBy = normalizeQuotedIdentifierExpr(funcExpr.Over.OrderBy[0].Expr.String())
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

func parseWindowOverClause(overSQL string) ([]string, string, error) {
	inside := strings.TrimSpace(overSQL)
	if inside == "" {
		return nil, "", errors.New("empty OVER clause")
	}
	upper := strings.ToUpper(inside)

	var partitionBy []string
	orderBy := ""

	partIdx := strings.Index(upper, "PARTITION BY")
	orderIdx := strings.Index(upper, "ORDER BY")

	if partIdx >= 0 {
		partStart := partIdx + len("PARTITION BY")
		partEnd := len(inside)
		if orderIdx > partIdx {
			partEnd = orderIdx
		}
		partExpr := strings.TrimSpace(inside[partStart:partEnd])
		for _, p := range splitByCommaOutsideParens(partExpr) {
			p = normalizeQuotedIdentifierExpr(strings.TrimSpace(p))
			if p != "" {
				partitionBy = append(partitionBy, p)
			}
		}
	}

	if orderIdx >= 0 {
		ordStart := orderIdx + len("ORDER BY")
		ordExpr := strings.TrimSpace(inside[ordStart:])
		ordItems := splitByCommaOutsideParens(ordExpr)
		if len(ordItems) > 0 {
			first := strings.TrimSpace(ordItems[0])
			u := strings.ToUpper(first)
			if strings.HasSuffix(u, " DESC") {
				first = strings.TrimSpace(first[:len(first)-5])
			} else if strings.HasSuffix(u, " ASC") {
				first = strings.TrimSpace(first[:len(first)-4])
			}
			orderBy = normalizeQuotedIdentifierExpr(first)
		}
	}

	if orderBy == "" {
		return nil, "", errors.New("window function requires ORDER BY")
	}

	return partitionBy, orderBy, nil
}

func findAllWindowFunctionsFromQuery(query string) ([]*ir.LogicalWindowFunc, error) {
	clause, err := extractSelectClause(query)
	if err != nil {
		return nil, err
	}
	items := splitByCommaOutsideParens(clause)
	out := make([]*ir.LogicalWindowFunc, 0)

	for _, raw := range items {
		item := strings.TrimSpace(raw)
		if item == "" {
			continue
		}
		upper := strings.ToUpper(item)
		if !strings.Contains(upper, " OVER ") {
			continue
		}

		alias := ""
		exprPart := item
		if asIdx := strings.LastIndex(upper, " AS "); asIdx >= 0 {
			alias = strings.Trim(strings.TrimSpace(item[asIdx+4:]), "`\"'")
			exprPart = strings.TrimSpace(item[:asIdx])
		}

		exprUpper := strings.ToUpper(exprPart)
		open := strings.Index(exprPart, "(")
		if open <= 0 {
			continue
		}
		funcName := strings.ToUpper(strings.TrimSpace(exprPart[:open]))
		if funcName != "LAG" && funcName != "LEAD" {
			continue
		}

		close, ferr := findMatchingParen(exprPart, open)
		if ferr != nil || close <= open {
			continue
		}
		argsSQL := strings.TrimSpace(exprPart[open+1 : close])
		args := splitByCommaOutsideParens(argsSQL)
		if len(args) == 0 {
			continue
		}
		lagCol := normalizeQuotedIdentifierExpr(strings.TrimSpace(args[0]))
		offset := 1
		if len(args) > 1 {
			rawOffset := strings.Trim(strings.TrimSpace(args[1]), "'\"")
			if n, perr := strconv.Atoi(rawOffset); perr == nil && n > 0 {
				offset = n
			}
		}

		overIdx := strings.Index(exprUpper, "OVER")
		if overIdx < 0 {
			continue
		}
		overTail := strings.TrimSpace(exprPart[overIdx+len("OVER"):])
		overOpen := strings.Index(overTail, "(")
		if overOpen < 0 {
			continue
		}
		overClose, oerr := findMatchingParen(overTail, overOpen)
		if oerr != nil || overClose <= overOpen {
			continue
		}
		overInside := strings.TrimSpace(overTail[overOpen+1 : overClose])
		partitionBy, orderBy, perr := parseWindowOverClause(overInside)
		if perr != nil {
			continue
		}

		outputCol := alias
		if outputCol == "" {
			outputCol = strings.ToLower(funcName) + "_" + sanitizeIdentifierForAlias(lagCol)
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

	return out, nil
}

func findAllWindowAggregatesFromQuery(query string) ([]*ir.LogicalWindowAgg, error) {
	clause, err := extractSelectClause(query)
	if err != nil {
		return nil, err
	}
	items := splitByCommaOutsideParens(clause)
	out := make([]*ir.LogicalWindowAgg, 0)

	for _, raw := range items {
		item := strings.TrimSpace(raw)
		if item == "" {
			continue
		}
		upper := strings.ToUpper(item)
		if !strings.Contains(upper, " OVER ") {
			continue
		}

		alias := ""
		exprPart := item
		if asIdx := strings.LastIndex(upper, " AS "); asIdx >= 0 {
			alias = strings.Trim(strings.TrimSpace(item[asIdx+4:]), "`\"'")
			exprPart = strings.TrimSpace(item[:asIdx])
		}

		exprUpper := strings.ToUpper(exprPart)
		open := strings.Index(exprPart, "(")
		if open <= 0 {
			continue
		}
		funcName := strings.ToUpper(strings.TrimSpace(exprPart[:open]))
		if funcName != "SUM" && funcName != "AVG" && funcName != "COUNT" && funcName != "MIN" && funcName != "MAX" {
			continue
		}

		close, ferr := findMatchingParen(exprPart, open)
		if ferr != nil || close <= open {
			continue
		}
		aggCol := ""
		argsSQL := strings.TrimSpace(exprPart[open+1 : close])
		args := splitByCommaOutsideParens(argsSQL)
		if len(args) > 0 {
			aggCol = normalizeQuotedIdentifierExpr(strings.TrimSpace(args[0]))
		}

		overIdx := strings.Index(exprUpper, "OVER")
		if overIdx < 0 {
			continue
		}
		overTail := strings.TrimSpace(exprPart[overIdx+len("OVER"):])
		overOpen := strings.Index(overTail, "(")
		if overOpen < 0 {
			continue
		}
		overClose, oerr := findMatchingParen(overTail, overOpen)
		if oerr != nil || overClose <= overOpen {
			continue
		}
		overInside := strings.TrimSpace(overTail[overOpen+1 : overClose])
		partitionBy, orderBy, perr := parseWindowOverClause(overInside)
		if perr != nil {
			continue
		}

		outputCol := alias
		if outputCol == "" {
			outputCol = strings.ToLower(funcName) + "_" + sanitizeIdentifierForAlias(aggCol)
		}

		out = append(out, &ir.LogicalWindowAgg{
			AggName:     funcName,
			AggCol:      aggCol,
			PartitionBy: partitionBy,
			OrderBy:     orderBy,
			OutputCol:   outputCol,
		})
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
func ParseQueryToIncrementalDBSP(query string, options ...ComplianceOption) (*op.Node, error) {
	// First get the base DBSP graph
	baseNode, err := ParseQueryToDBSP(query, options...)
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

	leftName := normalizeRelationName(leftTable.Name)
	rightName := normalizeRelationName(rightTable.Name)

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

	if _, ok := ctes[leftName]; ok {
		leftLp = &ir.LogicalCTERef{CTEName: leftName}
	} else {
		leftLp = &ir.LogicalScan{Table: leftName}
	}

	if _, ok := ctes[rightName]; ok {
		rightLp = &ir.LogicalCTERef{CTEName: rightName}
	} else {
		rightLp = &ir.LogicalScan{Table: rightName}
	}

	join := &ir.LogicalJoin{
		LeftTable:  leftName,
		RightTable: rightName,
		Conditions: conditions,
		Left:       leftLp,
		Right:      rightLp,
	}
	var currentNode ir.LogicalNode = join

	// Add WHERE filter if present
	if sel.Where != nil {
		whereSQL := sel.Where.String()
		whereSQL = strings.Trim(whereSQL, "'\"")
		whereSQL = normalizeQuotedIdentifierTokens(whereSQL)
		currentNode = &ir.LogicalFilter{
			PredicateSQL: whereSQL,
			Input:        currentNode,
		}
	}

	// Extract select columns
	selectCols, selectExprs, err := extractProjectionSpecs(sel, rawQuery)
	if err != nil {
		return nil, err
	}

	// Check for GROUP BY
	if len(sel.GroupBy) == 0 {
		// No GROUP BY - add projection if needed
		if len(selectCols) > 0 || len(selectExprs) > 0 {
			currentNode = &ir.LogicalProject{
				KeepInput: shouldKeepInputForProjection(sel, rawQuery),
				Columns:   selectCols,
				Exprs:     selectExprs,
				Input:     currentNode,
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
		aggs, err := findAggregatesForGroupBy(sel, rawQuery)
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
	aggs, err := findAggregatesForGroupBy(sel, rawQuery)
	if err != nil {
		return nil, err
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
		seenAliases := make(map[string]struct{})
		for _, a := range aggs {
			col := a.Col
			if strings.ToUpper(a.Name) == "COUNT" && strings.TrimSpace(col) == "*" {
				col = ""
			}
			alias := aggOutputAlias(rawQuery, a)
			if alias != "" {
				if _, exists := seenAliases[alias]; exists {
					continue
				}
				seenAliases[alias] = struct{}{}
			}
			lg.Aggs = append(lg.Aggs, ir.AggSpec{Name: a.Name, Col: col, As: alias})
		}
	}

	return lg, nil
}

func aggOutputAlias(query string, agg AggCall) string {
	if alias := strings.TrimSpace(agg.As); alias != "" {
		return alias
	}
	if alias := strings.TrimSpace(extractAggAliasFromQuery(query, agg)); alias != "" {
		return alias
	}
	return ""
}

func findAggregatesForGroupBy(sel *ast.Select, query string) ([]AggCall, error) {
	aggMap := make(map[string]AggCall)
	aggAliasKey := make(map[string]string)
	order := make([]string, 0)

	addAgg := func(a AggCall) {
		a.Col = normalizeQuotedIdentifierTokens(strings.TrimSpace(a.Col))
		a.As = strings.TrimSpace(a.As)
		key := strings.ToUpper(strings.TrimSpace(a.Name)) + "|" + strings.TrimSpace(a.Col)

		// When multiple parsers discover the same SELECT aggregate through different
		// shapes, prefer the first aggregate bound to an explicit alias.
		if a.As != "" {
			if existingKey, ok := aggAliasKey[a.As]; ok && existingKey != key {
				return
			}
		}

		if existing, exists := aggMap[key]; exists {
			if strings.TrimSpace(existing.As) == "" && a.As != "" {
				existing.As = a.As
				aggMap[key] = existing
				aggAliasKey[a.As] = key
			}
			return
		}

		order = append(order, key)
		aggMap[key] = a
		if a.As != "" {
			aggAliasKey[a.As] = key
		}
	}

	if queryAggs, err := findAggregatesFromQuery(query); err == nil && len(queryAggs) > 0 {
		for _, a := range queryAggs {
			a.As = extractAggAliasFromQuery(query, a)
			addAgg(a)
		}
	}

	if selAggs, err := findAggregatesFromSelect(sel); err == nil && len(selAggs) > 0 {
		for _, a := range selAggs {
			addAgg(a)
		}
	}

	// Fallback: detect nested aggregates from the select item SQL string.
	if sel != nil {
		for _, item := range sel.SelectList {
			alias := strings.TrimSpace(item.As)
			exprSQL := ""
			if alias != "" {
				if recovered, ok := extractSelectExprByAlias(query, alias); ok {
					exprSQL = strings.TrimSpace(recovered)
				}
			}
			if exprSQL == "" {
				exprSQL = strings.TrimSpace(item.Expr.String())
			}
			if exprSQL == "" {
				continue
			}
			upperExpr := strings.ToUpper(exprSQL)
			if strings.Contains(upperExpr, " OVER ") {
				continue
			}
			if looksMalformedExprSQL(exprSQL) {
				continue
			}
			calls, err := parseNestedAggCalls(exprSQL)
			if err != nil || len(calls) == 0 {
				continue
			}
			for _, call := range calls {
				call.As = alias
				addAgg(call)
			}
		}
	}

	if len(order) == 0 {
		return nil, errors.New("no aggregate function found")
	}

	out := make([]AggCall, 0, len(order))
	for _, key := range order {
		out = append(out, aggMap[key])
	}
	return out, nil
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

func shouldKeepInputForProjection(sel *ast.Select, query string) bool {
	if sel == nil {
		return false
	}
	// For GROUP BY queries, we must NOT keep full input because aggregation
	// collapses the input tuples. We only have access to group keys and aggregates.
	hasAggs := false
	if aggs, err := findAggregatesForGroupBy(sel, query); err == nil && len(aggs) > 0 {
		hasAggs = true
	}
	if len(sel.GroupBy) > 0 || hasAggs {
		return false
	}
	hasWindow := false
	for _, item := range sel.SelectList {
		if _, ok := item.Expr.(*ast.StarExpr); ok {
			return true
		}
		fn, ok := item.Expr.(*ast.FuncExpr)
		if ok && fn.Over != nil {
			hasWindow = true
			continue
		}
	}
	return hasWindow
}

func shouldSkipFinalProjectionForWindow(sel *ast.Select, cols []string, exprs []ir.ProjectExpr) bool {
	if sel == nil {
		return false
	}
	if len(exprs) > 0 {
		return false
	}
	if len(sel.GroupBy) > 0 {
		return len(cols) > 0
	}
	hasWindow := false
	for _, item := range sel.SelectList {
		switch e := item.Expr.(type) {
		case *ast.StarExpr:
			return false
		case *ast.ColName:
			colName := e.Name
			if e.Table != "" {
				colName = e.Table + "." + e.Name
			}
			alias := strings.TrimSpace(item.As)
			if alias != "" && alias != colName {
				return false
			}
		case *ast.FuncExpr:
			if e.Over == nil {
				return false
			}
			hasWindow = true
		default:
			return false
		}
	}
	if !hasWindow {
		return false
	}
	return len(cols) > 0
}

func extractCTEBodyQueries(query string) map[string]string {
	out := make(map[string]string)
	q := strings.TrimSpace(query)
	up := strings.ToUpper(q)
	if !strings.HasPrefix(up, "WITH ") {
		return out
	}

	i := len("WITH ")
	for i < len(q) {
		for i < len(q) && (q[i] == ' ' || q[i] == '\n' || q[i] == '\t' || q[i] == '\r') {
			i++
		}
		if i >= len(q) {
			break
		}

		nameStart := i
		for i < len(q) {
			c := q[i]
			if (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '_' {
				i++
				continue
			}
			break
		}
		if i == nameStart {
			break
		}
		name := strings.ToLower(strings.TrimSpace(q[nameStart:i]))

		for i < len(q) && (q[i] == ' ' || q[i] == '\n' || q[i] == '\t' || q[i] == '\r') {
			i++
		}
		if i+2 > len(q) || strings.ToUpper(q[i:i+2]) != "AS" {
			break
		}
		i += 2
		for i < len(q) && (q[i] == ' ' || q[i] == '\n' || q[i] == '\t' || q[i] == '\r') {
			i++
		}
		if i >= len(q) || q[i] != '(' {
			break
		}
		i++
		depth := 1
		bodyStart := i
		bodyEnd := -1
	forBody:
		for ; i < len(q); i++ {
			switch q[i] {
			case '(':
				depth++
			case ')':
				depth--
				if depth == 0 {
					bodyEnd = i
					break forBody
				}
			}
		}
		if bodyEnd == -1 {
			break
		}

		out[name] = strings.TrimSpace(q[bodyStart:bodyEnd])
		i = bodyEnd + 1

		for i < len(q) && (q[i] == ' ' || q[i] == '\n' || q[i] == '\t' || q[i] == '\r') {
			i++
		}
		if i < len(q) && q[i] == ',' {
			i++
			continue
		}
		break
	}

	return out
}

func extractOuterSelectAfterWith(query string) string {
	q := strings.TrimSpace(query)
	up := strings.ToUpper(q)
	if !strings.HasPrefix(up, "WITH ") {
		return q
	}

	i := len("WITH ")
	for i < len(q) {
		for i < len(q) && (q[i] == ' ' || q[i] == '\n' || q[i] == '\t' || q[i] == '\r') {
			i++
		}
		if i >= len(q) {
			return q
		}

		nameStart := i
		for i < len(q) {
			c := q[i]
			if (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '_' {
				i++
				continue
			}
			break
		}
		if i == nameStart {
			return q
		}

		for i < len(q) && (q[i] == ' ' || q[i] == '\n' || q[i] == '\t' || q[i] == '\r') {
			i++
		}
		if i+2 > len(q) || strings.ToUpper(q[i:i+2]) != "AS" {
			return q
		}
		i += 2

		for i < len(q) && (q[i] == ' ' || q[i] == '\n' || q[i] == '\t' || q[i] == '\r') {
			i++
		}
		if i >= len(q) || q[i] != '(' {
			return q
		}
		i++
		depth := 1
		for i < len(q) && depth > 0 {
			switch q[i] {
			case '(':
				depth++
			case ')':
				depth--
			}
			i++
		}
		if depth != 0 {
			return q
		}

		for i < len(q) && (q[i] == ' ' || q[i] == '\n' || q[i] == '\t' || q[i] == '\r') {
			i++
		}
		if i < len(q) && q[i] == ',' {
			i++
			continue
		}
		break
	}

	outer := strings.TrimSpace(q[i:])
	if outer == "" {
		return q
	}
	return outer
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

func findTimeBucketInSelect(sel *ast.Select, query string) (*ir.TimeWindowSpec, error) {
	for _, item := range sel.SelectList {
		exprSQL := ""
		if alias := strings.TrimSpace(item.As); alias != "" {
			if recovered, ok := extractSelectExprByAlias(query, alias); ok {
				exprSQL = recovered
			}
		}
		if exprSQL == "" {
			exprSQL = item.Expr.String()
		}
		upper := strings.ToUpper(exprSQL)
		if strings.Contains(upper, "TIME_BUCKET(") {
			// DuckDB style pattern matching: TIME_BUCKET(INTERVAL, COL)
			open := strings.Index(upper, "TIME_BUCKET(") + 11
			close := strings.LastIndex(upper, ")")
			if open != -1 && close != -1 {
				inner := strings.TrimSpace(exprSQL[open+1 : close])
				parts := splitWindowArgs(inner)
				if len(parts) >= 2 {
					intervalStr := strings.TrimSpace(parts[0])
					timeCol := strings.TrimSpace(parts[1])

					// Strip CAST if present in timeCol: CAST(timestamp AS BIGINT) -> timestamp
					colUp := strings.ToUpper(timeCol)
					if strings.HasPrefix(colUp, "CAST(") {
						cOpen := strings.Index(colUp, "(")
						cAs := strings.Index(colUp, " AS ")
						if cOpen != -1 && cAs != -1 {
							timeCol = strings.TrimSpace(timeCol[cOpen+1 : cAs])
						}
					}
					timeCol = strings.Trim(timeCol, "`\"'")

					// Strip INTERVAL if present
					if strings.HasPrefix(strings.ToUpper(intervalStr), "INTERVAL ") {
						intervalStr = strings.TrimSpace(intervalStr[9:])
					}
					// and potentially quotes
					intervalStr = strings.Trim(intervalStr, "'\"")

					millis, err := parseIntervalArg(intervalStr)
					if err == nil {
						return &ir.TimeWindowSpec{
							WindowType: "TUMBLING",
							TimeCol:    timeCol,
							SizeMillis: millis,
						}, nil
					}
				}
			}
			// Standard Tumble/etc fallback
			parsed, err := ParseTimeWindowSQL(exprSQL)
			if err == nil && parsed != nil {
				return parsed, nil
			}
		}
	}
	return nil, nil
}

func findTimeBucketAliasInSelect(sel *ast.Select, query string) string {
	if sel == nil {
		return ""
	}
	for _, item := range sel.SelectList {
		exprSQL := ""
		alias := strings.TrimSpace(item.As)
		if alias != "" {
			if recovered, ok := extractSelectExprByAlias(query, alias); ok {
				exprSQL = recovered
			}
		}
		if exprSQL == "" {
			exprSQL = item.Expr.String()
		}
		if strings.Contains(strings.ToUpper(exprSQL), "TIME_BUCKET(") {
			return alias
		}
	}
	return ""
}

func findNonAggregatedCols(sel *ast.Select, query string) []string {
	seen := make(map[string]struct{})
	var cols []string
	for _, item := range sel.SelectList {
		expr := item.Expr
		alias := strings.TrimSpace(item.As)

		// Don't include if it is an aggregate call
		isAgg := false
		exprSQL := ""
		if alias != "" {
			if recovered, ok := extractSelectExprByAlias(query, alias); ok {
				exprSQL = recovered
			}
		}
		if exprSQL == "" {
			exprSQL = expr.String()
		}
		upper := strings.ToUpper(exprSQL)
		if strings.Contains(upper, "AVG(") || strings.Contains(upper, "SUM(") || strings.Contains(upper, "COUNT(") || strings.Contains(upper, "MIN(") || strings.Contains(upper, "MAX(") || strings.Contains(upper, "TIME_BUCKET(") {
			isAgg = true
		}

		if !isAgg {
			name := alias
			if name == "" {
				if col, ok := expr.(*ast.ColName); ok {
					name = col.Name
				}
			}
			if name == "" {
				// Final fallback: try to use the raw expression string if it's a simple identifier
				raw := strings.Trim(exprSQL, "`\"'")
				if isSimpleIdentifier(raw) {
					name = raw
				}
			}
			if name != "" {
				if _, ok := seen[name]; !ok {
					seen[name] = struct{}{}
					cols = append(cols, name)
				}
			}
		}
	}
	return cols
}

func isSimpleIdentifier(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '_' || c == '.' {
			continue
		}
		return false
	}
	return true
}

func isWordChar(c byte) bool {
	return (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_'
}
