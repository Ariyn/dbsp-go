package sqlconv

import (
	"errors"
	"strings"

	"github.com/Ariyn/tree-sitter-duckdb/bindings/go/ast"
	"github.com/ariyn/dbsp/internal/dbsp/ir"
	"github.com/ariyn/dbsp/internal/dbsp/parse"
)

// extractSelectColumns collects plain column names from SELECT list for projection.
// It ignores aggregate functions (handled separately) and stops projection when '*'
// is present.
func extractSelectColumns(sel *ast.Select) ([]string, error) {
	var cols []string
	for _, item := range sel.SelectList {
		switch e := item.Expr.(type) {
		case *ast.StarExpr:
			// SELECT * - no projection needed
			return nil, nil
		case *ast.ColName:
			// Use full column name (table.column if table qualifier exists)
			colName := e.Name
			if e.Table != "" {
				colName = e.Table + "." + e.Name
			}
			cols = append(cols, colName)
		case *ast.FuncExpr:
			// aggregate or other function - handled elsewhere, skip for projection
			continue
		default:
			// For now we don't support arbitrary expressions in projection
			return nil, errors.New("unsupported SELECT expression (only columns, aggregates, or * supported)")
		}
	}
	return cols, nil
}

// extractProjectionSpecs parses the SELECT list into plain column projections and
// computed expressions (which require an alias).
// Aggregates are ignored (handled separately).
// If '*' is present, it returns (nil, nil, nil) meaning "no projection".
func extractProjectionSpecs(sel *ast.Select, query string) ([]string, []ir.ProjectExpr, error) {
	var cols []string
	var exprs []ir.ProjectExpr
	groupedQuery := len(sel.GroupBy) > 0
	for _, item := range sel.SelectList {
		switch e := item.Expr.(type) {
		case *ast.StarExpr:
			return nil, nil, nil
		case *ast.ColName:
			colName := e.Name
			if e.Table != "" {
				colName = e.Table + "." + e.Name
			}
			alias := strings.TrimSpace(item.As)
			if groupedQuery && alias != "" {
				cols = append(cols, alias)
				continue
			}
			if alias != "" && alias != colName {
				exprs = append(exprs, ir.ProjectExpr{ExprSQL: colName, As: alias})
				continue
			}
			cols = append(cols, colName)
		case *ast.FuncExpr:
			funcName := strings.ToUpper(strings.TrimSpace(e.Name))
			isAgg := ir.IsAggregate(funcName)

			// If it's a window function or window aggregate, its output column should be projected.
			// (Assuming the alias is provided or generated)
			if e.Over != nil {
				colName := item.As
				if colName == "" {
					// Fallback to name generation similar to findAllWindowFunctionsFromSelect
					funcName := strings.ToUpper(e.Name)
					argStr := ""
					if len(e.Args) > 0 {
						argStr = e.Args[0].String()
					}
					colName = strings.ToLower(funcName) + "_" + argStr
				}
				cols = append(cols, colName)
				continue
			}

			// For GROUP BY outputs, aggregate values are already materialized by GroupAgg/WindowAgg.
			// Project the aggregate output column by alias instead of re-evaluating SQL expression.
			if isAgg {
				colName := strings.TrimSpace(item.As)
				if colName == "" {
					if groupedQuery {
						switch funcName {
						case "COUNT":
							colName = "count_delta"
						case "AVG":
							colName = "avg_delta"
						case "MIN":
							colName = "min"
						case "MAX":
							colName = "max"
						default:
							colName = "agg_delta"
						}
					} else {
						argStr := ""
						if len(e.Args) > 0 {
							argStr = e.Args[0].String()
						}
						colName = strings.ToLower(funcName) + "_" + sanitizeIdentifierForAlias(argStr)
					}
				}
				cols = append(cols, colName)
				continue
			}

			// In GROUP BY queries, non-aggregate function items in SELECT are often
			// aliases of grouped expressions (e.g., TIME_BUCKET(...) AS bucket).
			// The grouped value should already exist upstream as the alias column.
			if groupedQuery {
				colName := strings.TrimSpace(item.As)
				if colName == "" {
					return nil, nil, errors.New("unsupported grouped function expression without alias (use AS <name>)")
				}
				cols = append(cols, colName)
				continue
			}

			if strings.TrimSpace(item.As) == "" {
				return nil, nil, errors.New("unsupported function expression without alias (use AS <name>)")
			}
			exprSQL := strings.TrimSpace(item.Expr.String())
			exprSQL = recoverMalformedExprByAlias(query, item.As, exprSQL)
			exprSQL = normalizeQuotedIdentifierTokens(exprSQL)
			exprs = append(exprs, ir.ProjectExpr{ExprSQL: exprSQL, As: item.As})
			continue
		default:
			// Computed expression: require alias so output column is stable.
			if strings.TrimSpace(item.As) == "" {
				return nil, nil, errors.New("unsupported SELECT expression without alias (use AS <name>)")
			}
			exprSQL := strings.TrimSpace(item.Expr.String())
			exprSQL = recoverMalformedExprByAlias(query, item.As, exprSQL)
			exprSQL = normalizeQuotedIdentifierTokens(exprSQL)
			exprs = append(exprs, ir.ProjectExpr{ExprSQL: exprSQL, As: item.As})
		}
	}
	return cols, exprs, nil
}

func sanitizeIdentifierForAlias(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return "value"
	}
	replacer := strings.NewReplacer(
		" ", "",
		"\t", "",
		"\n", "",
		"\r", "",
		"(", "",
		")", "",
		",", "_",
		".", "_",
		"-", "_",
		"+", "_",
		"*", "_",
		"/", "_",
		"'", "",
		"\"", "",
	)
	s = replacer.Replace(s)
	for strings.Contains(s, "__") {
		s = strings.ReplaceAll(s, "__", "_")
	}
	s = strings.Trim(s, "_")
	if s == "" {
		return "value"
	}
	return s
}

// findSingleAggregate scans SELECT expressions and returns a single supported
// aggregate function name and its column. It keeps existing constraints
// (single aggregate, single column arg).
func findSingleAggregate(selectList ast.SelectExprs) (string, string, error) {
	var aggFunc string
	var aggCol string

	for _, item := range selectList {
		f, ok := item.Expr.(*ast.FuncExpr)
		if !ok {
			continue
		}

		// Extract function name from the SQL string representation
		// because the parser may not correctly populate f.Name
		sqlStr := f.String()
		// Format: FUNCNAME(arg) or funcname(arg)
		parenIdx := strings.Index(sqlStr, "(")
		if parenIdx == -1 {
			continue
		}
		name := strings.ToUpper(strings.TrimSpace(sqlStr[:parenIdx]))

		// Ignore window functions such as TUMBLE here; we only care about
		// true aggregates like SUM/COUNT/AVG/MIN/MAX.
		if !ir.IsAggregate(name) {
			continue
		}
		if aggFunc != "" && name != "" {
			return "", "", errors.New("multiple aggregate functions not supported yet")
		}

		// Extract column from the string between parentheses
		endParen := strings.LastIndex(sqlStr, ")")
		if endParen == -1 || endParen <= parenIdx {
			return "", "", errors.New("malformed function call")
		}
		argStr := strings.TrimSpace(sqlStr[parenIdx+1 : endParen])
		if argStr == "" {
			return "", "", errors.New("empty aggregate argument")
		}

		aggFunc = name
		aggCol = argStr
	}

	if aggFunc == "" {
		return "", "", errors.New("no aggregate function found")
	}

	return aggFunc, aggCol, nil
}

// findSingleAggregateFromQuery extracts aggregate function from SQL string
// This is a workaround for tree-sitter parser bugs with function calls
func findSingleAggregateFromQuery(query string) (string, string, error) {
	queryUpper := strings.ToUpper(query)

	for funcName := range ir.KnownAggregates {
		pattern := funcName + "("
		idx := strings.Index(queryUpper, pattern)
		if idx == -1 {
			continue
		}

		// Find matching closing parenthesis
		start := idx + len(pattern)
		depth := 1
		end := start

		for end < len(query) && depth > 0 {
			if query[end] == '(' {
				depth++
			} else if query[end] == ')' {
				depth--
			}
			end++
		}

		if depth != 0 {
			return "", "", errors.New("malformed aggregate function")
		}

		// Extract column name between parentheses
		colName := strings.TrimSpace(query[start : end-1])
		if colName == "" {
			return "", "", errors.New("empty aggregate argument")
		}

		return funcName, colName, nil
	}

	return "", "", errors.New("no aggregate function found")
}

// AggCall represents a parsed aggregate call from a SELECT list.
// Name is upper-cased (e.g., SUM, COUNT). Col is the raw argument string.
type AggCall struct {
	Name string
	Col  string
	As   string
}

func findAggregatesFromSelect(sel *ast.Select) ([]AggCall, error) {
	if sel == nil {
		return nil, errors.New("select is nil")
	}
	out := make([]AggCall, 0)
	for _, item := range sel.SelectList {
		aggs := collectAggCallsFromExpr(item.Expr)
		for _, agg := range aggs {
			agg.As = strings.TrimSpace(item.As)
			out = append(out, agg)
		}
	}
	if len(out) == 0 {
		return nil, errors.New("no aggregate function found")
	}
	return out, nil
}

func collectAggCallsFromExpr(expr ast.Expr) []AggCall {
	switch e := expr.(type) {
	case *ast.FuncExpr:
		name := strings.ToUpper(strings.TrimSpace(e.Name))
		if ir.IsAggregate(name) {
			col := ""
			if len(e.Args) > 0 {
				col = normalizeQuotedIdentifierTokens(strings.TrimSpace(e.Args[0].String()))
			} else {
				col = "*"
			}
			return []AggCall{{Name: name, Col: col}}
		}
		out := make([]AggCall, 0)
		for _, arg := range e.Args {
			out = append(out, collectAggCallsFromExpr(arg)...)
		}
		return out
	case *ast.BinaryExpr:
		out := collectAggCallsFromExpr(e.Left)
		out = append(out, collectAggCallsFromExpr(e.Right)...)
		return out
	case *ast.UnaryExpr:
		return collectAggCallsFromExpr(e.Expr)
	case *ast.CaseExpr:
		out := make([]AggCall, 0)
		for _, when := range e.Whens {
			out = append(out, collectAggCallsFromExpr(when.Val)...)
			out = append(out, collectAggCallsFromExpr(when.Cond)...)
		}
		if e.Else != nil {
			out = append(out, collectAggCallsFromExpr(e.Else)...)
		}
		return out
	default:
		return nil
	}
}

// findAggregatesFromQuery extracts all supported aggregate calls from the
// SELECT list portion of the SQL query string.
//
// This is intentionally string-based to work around tree-sitter parser bugs.
//
// Scope for multi-aggregate support:
// - Supports: SUM(col), COUNT(col)
// - Rejects: COUNT(*) (handled by a separate TODO)
func findAggregatesFromQuery(query string) ([]AggCall, error) {
	selectClauses, err := extractSelectClauses(query)
	if err != nil {
		return nil, err
	}

	var out []AggCall
	for _, selectClause := range selectClauses {
		items := splitByCommaOutsideParens(selectClause)
		for _, rawItem := range items {
			expr := strings.TrimSpace(rawItem)
			if expr == "" {
				continue
			}
			// Ignore window functions / analytic aggregates here.
			if strings.Contains(strings.ToUpper(expr), " OVER ") {
				continue
			}

			call, ok, err := parseAggCall(expr)
			if err != nil {
				ok = false
			}
			if ok {
				// COUNT(*) is allowed in general, but multi-aggregate support may
				// disallow mixing it with other aggregates at a higher level.
				out = append(out, call)
				continue
			}

			nested, err := parseNestedAggCalls(expr)
			if err != nil {
				return nil, err
			}
			out = append(out, nested...)
		}
	}
	if len(out) == 0 {
		return nil, errors.New("no aggregate function found")
	}
	return out, nil
}

func extractSelectClauses(query string) ([]string, error) {
	upper := strings.ToUpper(query)
	var clauses []string

	for i := 0; i < len(query)-len("SELECT")+1; i++ {
		if !hasKeywordAtWordBoundary(upper, i, "SELECT") {
			continue
		}

		baseDepth := 0
		for j := 0; j < i; j++ {
			switch query[j] {
			case '(':
				baseDepth++
			case ')':
				if baseDepth > 0 {
					baseDepth--
				}
			}
		}

		depth := baseDepth
		fromIdx := -1
		for j := i + len("SELECT"); j < len(query)-len("FROM")+1; j++ {
			switch query[j] {
			case '(':
				depth++
			case ')':
				if depth > 0 {
					depth--
				}
			}
			if depth != baseDepth {
				continue
			}
			if hasKeywordAtWordBoundary(upper, j, "FROM") {
				fromIdx = j
				break
			}
		}

		if fromIdx == -1 {
			for j := i + len("SELECT"); j < len(query)-len("FROM")+1; j++ {
				if hasKeywordAtWordBoundary(upper, j, "FROM") {
					fromIdx = j
					break
				}
			}
			if fromIdx == -1 {
				continue
			}
		}

		clause := strings.TrimSpace(query[i+len("SELECT") : fromIdx])
		if clause != "" {
			clauses = append(clauses, clause)
		}
	}

	if len(clauses) == 0 {
		return nil, errors.New("query must contain SELECT")
	}
	return clauses, nil
}

func extractSelectClause(query string) (string, error) {
	upper := strings.ToUpper(query)
	selectIdx := strings.Index(upper, "SELECT")
	if selectIdx == -1 {
		return "", errors.New("query must contain SELECT")
	}

	// Find the first FROM at depth 0 after SELECT.
	depth := 0
	fromIdx := -1
	for i := selectIdx + len("SELECT"); i < len(query)-3; i++ {
		switch query[i] {
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
		if hasKeywordAtWordBoundary(upper, i, "FROM") {
			fromIdx = i
			break
		}
	}
	if fromIdx == -1 {
		for i := selectIdx + len("SELECT"); i < len(query)-3; i++ {
			if hasKeywordAtWordBoundary(upper, i, "FROM") {
				fromIdx = i
				break
			}
		}
		if fromIdx == -1 {
			return "", errors.New("query must contain FROM")
		}
	}
	clause := strings.TrimSpace(query[selectIdx+len("SELECT") : fromIdx])
	return clause, nil
}

func splitByCommaOutsideParens(s string) []string {
	return parse.SplitByComma(s)
}

func hasKeywordAtWordBoundary(upper string, i int, kw string) bool {
	return parse.HasKeywordAtWordBoundary(upper, i, kw)
}

func parseAggCall(expr string) (AggCall, bool, error) {
	// Find the first '(' and its matching ')'.
	open := strings.Index(expr, "(")
	if open == -1 {
		return AggCall{}, false, nil
	}
	name := strings.ToUpper(strings.TrimSpace(expr[:open]))
	// Allow whitespace before paren: COUNT (x)
	name = strings.TrimSpace(name)
	if name == "" {
		return AggCall{}, false, nil
	}

	depth := 0
	close := -1
	for i := open; i < len(expr); i++ {
		switch expr[i] {
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				close = i
				break
			}
		}
	}
	if close == -1 {
		return AggCall{}, false, errors.New("malformed function call")
	}
	arg := strings.TrimSpace(expr[open+1 : close])
	arg = strings.Trim(arg, "`\"'")
	arg = normalizeQuotedIdentifierTokens(arg)
	if arg == "" {
		return AggCall{}, false, errors.New("empty aggregate argument")
	}

	// Only consider real aggregates.
	if !ir.IsAggregate(name) {
		return AggCall{}, false, nil
	}

	return AggCall{Name: name, Col: arg}, true, nil
}

func parseNestedAggCalls(expr string) ([]AggCall, error) {
	upper := strings.ToUpper(expr)
	out := make([]AggCall, 0)

	for i := 0; i < len(expr); i++ {
		if !isAggNameStart(upper, i) {
			continue
		}

		name, open, ok := parseAggNameAt(upper, i)
		if !ok {
			continue
		}

		close, err := findMatchingParen(expr, open)
		if err != nil {
			continue
		}

		arg := strings.TrimSpace(expr[open+1 : close])
		arg = strings.Trim(arg, "`\"'")
		arg = normalizeQuotedIdentifierTokens(arg)
		if arg == "" {
			continue
		}

		out = append(out, AggCall{Name: name, Col: arg})
		i = close
	}

	return out, nil
}

func isAggNameStart(upper string, i int) bool {
	for name := range ir.KnownAggregates {
		if hasKeywordAtWordBoundary(upper, i, name) {
			return true
		}
	}
	return false
}

func parseAggNameAt(upper string, i int) (string, int, bool) {
	for name := range ir.KnownAggregates {
		if !hasKeywordAtWordBoundary(upper, i, name) {
			continue
		}
		j := i + len(name)
		for j < len(upper) && (upper[j] == ' ' || upper[j] == '\t' || upper[j] == '\n' || upper[j] == '\r') {
			j++
		}
		if j < len(upper) && upper[j] == '(' {
			return name, j, true
		}
	}
	return "", -1, false
}

func findMatchingParen(expr string, open int) (int, error) {
	return parse.FindMatchingParen(expr, open)
}

func selectItemContainsAgg(expr string, agg AggCall) bool {
	call, ok, err := parseAggCall(expr)
	if err == nil && ok {
		if strings.ToUpper(call.Name) == strings.ToUpper(agg.Name) && strings.TrimSpace(call.Col) == strings.TrimSpace(agg.Col) {
			return true
		}
	}

	nested, err := parseNestedAggCalls(expr)
	if err != nil {
		return false
	}
	for _, c := range nested {
		if strings.ToUpper(c.Name) == strings.ToUpper(agg.Name) && strings.TrimSpace(c.Col) == strings.TrimSpace(agg.Col) {
			return true
		}
	}
	return false
}

func normalizeQuotedIdentifierTokens(expr string) string {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return expr
	}

	var b strings.Builder
	b.Grow(len(expr))

	for i := 0; i < len(expr); {
		if expr[i] != '\'' {
			b.WriteByte(expr[i])
			i++
			continue
		}

		j := i + 1
		for j < len(expr) && expr[j] != '\'' {
			j++
		}
		if j >= len(expr) {
			b.WriteString(expr[i:])
			break
		}

		token := expr[i+1 : j]
		if isIdentifierToken(token) {
			b.WriteString(token)
		} else {
			b.WriteByte('\'')
			b.WriteString(token)
			b.WriteByte('\'')
		}
		i = j + 1
	}

	return strings.TrimSpace(b.String())
}

func recoverMalformedExprByAlias(query, alias, exprSQL string) string {
	if strings.TrimSpace(alias) == "" {
		return exprSQL
	}
	if !looksMalformedExprSQL(exprSQL) {
		return exprSQL
	}
	recovered, ok := extractSelectExprByAlias(query, alias)
	if !ok {
		return exprSQL
	}
	return strings.TrimSpace(recovered)
}

func looksMalformedExprSQL(expr string) bool {
	e := strings.TrimSpace(expr)
	if e == "" {
		return true
	}
	if strings.Contains(e, "'('") || strings.Contains(e, "')'") {
		return true
	}
	if e == "('(' - ')')" {
		return true
	}
	return false
}

func extractSelectExprByAlias(query, alias string) (string, bool) {
	if strings.TrimSpace(query) == "" || strings.TrimSpace(alias) == "" {
		return "", false
	}
	clause, err := extractSelectClause(query)
	if err != nil {
		return "", false
	}
	target := strings.ToUpper(strings.TrimSpace(alias))
	items := splitByCommaOutsideParens(clause)
	for _, raw := range items {
		item := strings.TrimSpace(raw)
		if item == "" {
			continue
		}
		up := strings.ToUpper(item)
		asIdx := strings.LastIndex(up, " AS ")
		if asIdx == -1 {
			continue
		}
		aliasPart := strings.TrimSpace(item[asIdx+4:])
		aliasPart = strings.Trim(aliasPart, "`\"'")
		if strings.ToUpper(aliasPart) != target {
			continue
		}
		exprPart := strings.TrimSpace(item[:asIdx])
		if exprPart == "" {
			continue
		}
		return exprPart, true
	}
	return "", false
}

func isIdentifierToken(token string) bool {
	token = strings.TrimSpace(token)
	if token == "" {
		return false
	}
	for i := 0; i < len(token); i++ {
		c := token[i]
		if (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '_' || c == '.' {
			continue
		}
		return false
	}
	return true
}
