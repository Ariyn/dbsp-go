package sqlconv

import (
	"errors"
	"strings"

	"github.com/ariyn/dbsp/internal/dbsp/ir"
)

// parseTimeWindowGroupByFallback attempts to parse a time-window GROUP BY query
// using only string processing.
//
// This exists because tree-sitter-duckdb currently struggles with GROUP BY
// TUMBLE/HOP/SESSION syntax.
//
// Supported shape (minimal):
//   SELECT <keys...>, <AGG>(<col>|*) [AS alias] FROM <table> [WHERE <pred>]
//   GROUP BY <keys...>, TUMBLE/HOP/SESSION(...)
//
// Returns (plan, true, nil) when it recognizes and parses a time-window GROUP BY.
func parseTimeWindowGroupByFallback(query string) (ir.LogicalNode, bool, error) {
	upper := strings.ToUpper(query)
	if !strings.Contains(upper, "GROUP BY") {
		return nil, false, nil
	}
	// Quick pre-filter: only attempt if a window function exists.
	if !(strings.Contains(upper, "TUMBLE(") || strings.Contains(upper, "HOP(") || strings.Contains(upper, "SESSION(") || strings.Contains(upper, "SLIDING(")) {
		return nil, false, nil
	}

	fromTable, err := extractSingleFromTable(query)
	if err != nil {
		return nil, false, err
	}

	groupByClause, ok, err := extractGroupByClause(query)
	if err != nil {
		return nil, false, err
	}
	if !ok {
		return nil, false, nil
	}
	itemsRaw := splitByCommaOutsideParens(groupByClause)
	items := make([]string, 0, len(itemsRaw))
	for _, it := range itemsRaw {
		it = strings.TrimSpace(strings.Trim(it, "`\"'"))
		if it == "" {
			continue
		}
		items = append(items, it)
	}

	timeSpec, keys, err := ParseTimeWindowFromGroupBy(items)
	if err != nil {
		return nil, false, err
	}
	if timeSpec == nil {
		return nil, false, nil
	}

	aggs, err := findAggregatesFromQuery(query)
	if err != nil {
		return nil, false, err
	}
	if len(aggs) != 1 {
		return nil, false, errors.New("time-window GROUP BY supports exactly one aggregate")
	}
	aggName := strings.ToUpper(strings.TrimSpace(aggs[0].Name))
	aggCol := strings.TrimSpace(aggs[0].Col)
	if aggName == "COUNT" && aggCol == "*" {
		aggCol = ""
	}

	outputCol := extractAggAliasFromQuery(query, aggs[0])
	if strings.TrimSpace(outputCol) == "" {
		outputCol = strings.ToLower(aggName)
	}

	scan := &ir.LogicalScan{Table: fromTable}
	var input ir.LogicalNode = scan
	if whereSQL, ok, err := extractWhereClause(query); err != nil {
		return nil, false, err
	} else if ok {
		input = &ir.LogicalFilter{PredicateSQL: whereSQL, Input: input}
	}

	wa := &ir.LogicalWindowAgg{
		AggName:        aggName,
		AggCol:         aggCol,
		PartitionBy:    keys,
		TimeWindowSpec: timeSpec,
		OutputCol:      outputCol,
		Input:          input,
	}

	return wa, true, nil
}

func extractGroupByClause(query string) (string, bool, error) {
	upper := strings.ToUpper(query)
	idx := indexKeywordDepth0(upper, "GROUP BY", 0)
	if idx == -1 {
		return "", false, nil
	}
	start := idx + len("GROUP BY")

	// End at the next top-level keyword if present.
	end := len(query)
	for _, kw := range []string{"HAVING", "ORDER BY", "LIMIT"} {
		kidx := indexKeywordDepth0(upper, kw, start)
		if kidx != -1 && kidx < end {
			end = kidx
		}
	}

	clause := strings.TrimSpace(query[start:end])
	return clause, true, nil
}

func extractWhereClause(query string) (string, bool, error) {
	upper := strings.ToUpper(query)
	whereIdx := indexKeywordDepth0(upper, "WHERE", 0)
	if whereIdx == -1 {
		return "", false, nil
	}
	start := whereIdx + len("WHERE")
	end := len(query)
	for _, kw := range []string{"GROUP BY", "ORDER BY", "LIMIT"} {
		kidx := indexKeywordDepth0(upper, kw, start)
		if kidx != -1 && kidx < end {
			end = kidx
		}
	}
	clause := strings.TrimSpace(query[start:end])
	clause = strings.Trim(clause, "'\"")
	return clause, true, nil
}

func extractSingleFromTable(query string) (string, error) {
	upper := strings.ToUpper(query)
	fromIdx := indexKeywordDepth0(upper, "FROM", 0)
	if fromIdx == -1 {
		return "", errors.New("query must contain FROM")
	}
	start := fromIdx + len("FROM")
	end := len(query)
	for _, kw := range []string{"WHERE", "GROUP BY", "ORDER BY", "LIMIT"} {
		kidx := indexKeywordDepth0(upper, kw, start)
		if kidx != -1 && kidx < end {
			end = kidx
		}
	}
	fromClause := strings.TrimSpace(query[start:end])
	if fromClause == "" {
		return "", errors.New("FROM clause is empty")
	}
	parts := strings.Fields(fromClause)
	if len(parts) == 0 {
		return "", errors.New("FROM clause is empty")
	}
	// Minimal: take first token as table name.
	table := strings.Trim(parts[0], "`\"'")
	return table, nil
}

func extractAggAliasFromQuery(query string, agg AggCall) string {
	selectClause, err := extractSelectClause(query)
	if err != nil {
		return ""
	}
	items := splitByCommaOutsideParens(selectClause)
	for _, rawItem := range items {
		expr := strings.TrimSpace(rawItem)
		if expr == "" {
			continue
		}
		call, ok, err := parseAggCall(expr)
		if err != nil {
			continue
		}
		if !ok {
			continue
		}
		if strings.ToUpper(call.Name) != strings.ToUpper(agg.Name) {
			continue
		}
		if strings.TrimSpace(call.Col) != strings.TrimSpace(agg.Col) {
			continue
		}

		upperItem := strings.ToUpper(expr)
		asIdx := strings.LastIndex(upperItem, " AS ")
		if asIdx == -1 {
			return ""
		}
		alias := strings.TrimSpace(expr[asIdx+len(" AS "):])
		alias = strings.Trim(alias, "`\"'")
		return alias
	}
	return ""
}

func indexKeywordDepth0(upperQuery string, keyword string, start int) int {
	depth := 0
	kw := strings.ToUpper(keyword)
	for i := start; i <= len(upperQuery)-len(kw); i++ {
		switch upperQuery[i] {
		case '(':
			depth++
			continue
		case ')':
			if depth > 0 {
				depth--
			}
			continue
		}
		if depth != 0 {
			continue
		}
		if hasKeywordAtWordBoundary(upperQuery, i, kw) {
			return i
		}
	}
	return -1
}
