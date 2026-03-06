package ir

import (
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/ariyn/dbsp/internal/dbsp/parse"
	"github.com/ariyn/dbsp/internal/dbsp/types"
)

// BuildExprFunc compiles a very small subset of SQL scalar expressions into
// an evaluator over a Tuple.
// Supported (minimal):
// - column reference: a, a.b
// - numeric literal: 1, 1.5
// - string literal: 'x'
// - arithmetic: + - * / with parentheses
// - CAST(expr AS BIGINT|DOUBLE|VARCHAR)
// - CASE WHEN <predicate> THEN <expr> ELSE <expr> END
func BuildExprFunc(exprSQL string) func(types.Tuple) (any, error) {
	exprSQL = strings.TrimSpace(exprSQL)
	upper := strings.ToUpper(exprSQL)
	if strings.HasPrefix(upper, "CASE ") || upper == "CASE" {
		return buildCaseExprFunc(exprSQL)
	}
	if strings.HasPrefix(upper, "CAST") {
		return buildCastExprFunc(exprSQL)
	}

	parser := newExprParser(exprSQL)
	node, err := parser.parse()
	return func(t types.Tuple) (any, error) {
		if err != nil {
			return nil, err
		}
		return node.eval(t)
	}
}

// --- CASE WHEN ---

func buildCaseExprFunc(exprSQL string) func(types.Tuple) (any, error) {
	// Minimal form: CASE WHEN <pred> THEN <expr> ELSE <expr> END
	upper := strings.ToUpper(exprSQL)
	if !strings.HasPrefix(strings.TrimSpace(upper), "CASE") {
		return func(types.Tuple) (any, error) { return nil, fmt.Errorf("invalid CASE expression") }
	}

	// Extract between WHEN/THEN/ELSE/END at depth 0.
	whenIdx := indexKeywordOutsideParens(upper, "WHEN")
	thenIdx := indexKeywordOutsideParens(upper, "THEN")
	elseIdx := indexKeywordOutsideParens(upper, "ELSE")
	endIdx := indexKeywordOutsideParens(upper, "END")
	if whenIdx == -1 || thenIdx == -1 || elseIdx == -1 || endIdx == -1 {
		return func(types.Tuple) (any, error) { return nil, fmt.Errorf("invalid CASE expression") }
	}
	if !(whenIdx < thenIdx && thenIdx < elseIdx && elseIdx < endIdx) {
		return func(types.Tuple) (any, error) { return nil, fmt.Errorf("invalid CASE expression") }
	}

	condSQL := strings.TrimSpace(exprSQL[whenIdx+len("WHEN") : thenIdx])
	thenSQL := strings.TrimSpace(exprSQL[thenIdx+len("THEN") : elseIdx])
	elseSQL := strings.TrimSpace(exprSQL[elseIdx+len("ELSE") : endIdx])

	condFn := BuildPredicateFunc(condSQL)
	thenFn := BuildExprFunc(thenSQL)
	elseFn := BuildExprFunc(elseSQL)

	return func(t types.Tuple) (any, error) {
		if condFn(t) {
			return thenFn(t)
		}
		return elseFn(t)
	}
}

func indexKeywordOutsideParens(upperSQL, kw string) int {
	return parse.IndexKeyword(upperSQL, kw)
}

// --- CAST ---

func buildCastExprFunc(exprSQL string) func(types.Tuple) (any, error) {
	upper := strings.ToUpper(strings.TrimSpace(exprSQL))
	if !strings.HasPrefix(upper, "CAST") {
		return func(types.Tuple) (any, error) { return nil, fmt.Errorf("invalid CAST expression") }
	}
	open := strings.Index(exprSQL, "(")
	close := strings.LastIndex(exprSQL, ")")
	if open == -1 || close == -1 || close <= open {
		return func(types.Tuple) (any, error) { return nil, fmt.Errorf("invalid CAST expression") }
	}
	inner := strings.TrimSpace(exprSQL[open+1 : close])
	upperInner := strings.ToUpper(inner)
	asIdx := indexKeywordOutsideParens(upperInner, "AS")
	if asIdx == -1 {
		return func(types.Tuple) (any, error) { return nil, fmt.Errorf("invalid CAST expression") }
	}
	valueSQL := strings.TrimSpace(inner[:asIdx])
	typeSQL := strings.TrimSpace(inner[asIdx+len("AS"):])
	if typeSQL == "" {
		return func(types.Tuple) (any, error) { return nil, fmt.Errorf("invalid CAST expression") }
	}
	valueFn := BuildExprFunc(valueSQL)
	castType := strings.ToUpper(typeSQL)

	return func(t types.Tuple) (any, error) {
		v, err := valueFn(t)
		if err != nil {
			return nil, err
		}
		switch castType {
		case "BIGINT", "INT", "INTEGER":
			return toInt64(v), nil
		case "DOUBLE", "FLOAT":
			return toFloat64(v), nil
		case "VARCHAR", "TEXT", "STRING":
			return fmt.Sprintf("%v", v), nil
		default:
			return nil, fmt.Errorf("unsupported CAST type %s", typeSQL)
		}
	}
}

func toFloat64(v any) float64 {
	return types.ToFloat64(v)
}

func toInt64(v any) int64 {
	return types.ToInt64(v)
}

// --- parser/AST ---

type tokenKind int

const (
	tokEOF tokenKind = iota
	tokIdent
	tokNumber
	tokString
	tokPlus
	tokMinus
	tokStar
	tokSlash
	tokLParen
	tokRParen
	tokArrow       // ->
	tokDoubleColon // ::
	tokComma
)

type token struct {
	kind tokenKind
	text string
	pos  int
}

type exprParser struct {
	src string
	pos int
	cur token
}

func newExprParser(src string) *exprParser {
	p := &exprParser{src: src}
	p.cur = p.nextToken()
	return p
}

func (p *exprParser) parse() (exprNode, error) {
	n, err := p.parseExpr()
	if err != nil {
		return nil, err
	}
	if p.cur.kind != tokEOF {
		return nil, p.errorAtCurrent(fmt.Sprintf("unexpected token %q", p.cur.text))
	}
	return n, nil
}

func (p *exprParser) parseExpr() (exprNode, error) {
	return p.parseAddSub()
}

func (p *exprParser) parseAddSub() (exprNode, error) {
	left, err := p.parseMulDiv()
	if err != nil {
		return nil, err
	}
	for p.cur.kind == tokPlus || p.cur.kind == tokMinus {
		op := p.cur
		p.cur = p.nextToken()
		right, err := p.parseMulDiv()
		if err != nil {
			return nil, err
		}
		left = &binOpNode{op: op.kind, left: left, right: right}
		_ = op
	}
	return left, nil
}

func (p *exprParser) parseMulDiv() (exprNode, error) {
	left, err := p.parseUnary()
	if err != nil {
		return nil, err
	}
	for p.cur.kind == tokStar || p.cur.kind == tokSlash {
		op := p.cur
		p.cur = p.nextToken()
		right, err := p.parseUnary()
		if err != nil {
			return nil, err
		}
		left = &binOpNode{op: op.kind, left: left, right: right}
	}
	return left, nil
}

func (p *exprParser) parseUnary() (exprNode, error) {
	if p.cur.kind == tokPlus {
		p.cur = p.nextToken()
		return p.parseUnary()
	}
	if p.cur.kind == tokMinus {
		p.cur = p.nextToken()
		inner, err := p.parseUnary()
		if err != nil {
			return nil, err
		}
		return &unaryNode{inner: inner}, nil
	}
	return p.parseCast()
}

func (p *exprParser) parseCast() (exprNode, error) {
	left, err := p.parseJSON()
	if err != nil {
		return nil, err
	}
	for p.cur.kind == tokDoubleColon {
		p.cur = p.nextToken()
		if p.cur.kind != tokIdent {
			return nil, p.errorAtCurrent("expected type after ::")
		}
		typeName := p.cur.text
		p.cur = p.nextToken()
		left = &castNode{inner: left, targetType: strings.ToUpper(typeName)}
	}
	return left, nil
}

func (p *exprParser) parseJSON() (exprNode, error) {
	left, err := p.parsePrimary()
	if err != nil {
		return nil, err
	}
	for p.cur.kind == tokArrow {
		p.cur = p.nextToken()
		if p.cur.kind != tokString && p.cur.kind != tokIdent {
			return nil, p.errorAtCurrent("expected key after ->")
		}
		key := p.cur.text
		p.cur = p.nextToken()
		left = &jsonAccessNode{inner: left, key: key}
	}
	return left, nil
}

func (p *exprParser) parsePrimary() (exprNode, error) {
	switch p.cur.kind {
	case tokIdent:
		ident := p.cur.text
		upperIdent := strings.ToUpper(ident)
		p.cur = p.nextToken()
		if upperIdent == "TRUE" {
			return &literalNode{v: true}, nil
		}
		if upperIdent == "FALSE" {
			return &literalNode{v: false}, nil
		}
		if upperIdent == "NULL" {
			return &literalNode{v: nil}, nil
		}
		// Check for function call
		if p.cur.kind == tokLParen {
			p.cur = p.nextToken()
			var args []exprNode
			if p.cur.kind != tokRParen {
				for {
					arg, err := p.parseExpr()
					if err != nil {
						return nil, err
					}
					args = append(args, arg)
					if p.cur.kind == tokComma {
						p.cur = p.nextToken()
						continue
					}
					break
				}
			}
			if p.cur.kind != tokRParen {
				return nil, p.errorAtCurrent("expected ) after function args")
			}
			p.cur = p.nextToken()
			return &funcCallNode{name: strings.ToUpper(ident), args: args}, nil
		}
		if strings.ToUpper(ident) == "INTERVAL" {
			// Handle INTERVAL '5' MINUTE
			if p.cur.kind != tokString && p.cur.kind != tokNumber {
				return nil, p.errorAtCurrent("expected value after INTERVAL")
			}
			val := p.cur.text
			p.cur = p.nextToken()
			unit := ""
			if p.cur.kind == tokIdent {
				unit = p.cur.text
				p.cur = p.nextToken()
			}
			return &intervalNode{val: val, unit: unit}, nil
		}
		return &identNode{name: ident}, nil
	case tokNumber:
		text := p.cur.text
		p.cur = p.nextToken()
		if strings.Contains(text, ".") {
			f, err := strconv.ParseFloat(text, 64)
			if err != nil {
				return nil, err
			}
			return &literalNode{v: f}, nil
		}
		i, err := strconv.ParseInt(text, 10, 64)
		if err != nil {
			return nil, err
		}
		return &literalNode{v: i}, nil
	case tokString:
		v := p.cur.text
		p.cur = p.nextToken()
		return &literalNode{v: v}, nil
	case tokLParen:
		p.cur = p.nextToken()
		inner, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		if p.cur.kind != tokRParen {
			return nil, p.errorAtCurrent("expected )")
		}
		p.cur = p.nextToken()
		return inner, nil
	default:
		return nil, p.errorAtCurrent(fmt.Sprintf("unexpected token %q", p.cur.text))
	}
}

func (p *exprParser) nextToken() token {
	// skip whitespace
	for p.pos < len(p.src) {
		switch p.src[p.pos] {
		case ' ', '\t', '\n', '\r':
			p.pos++
			continue
		}
		break
	}
	if p.pos >= len(p.src) {
		return token{kind: tokEOF, pos: len(p.src)}
	}

	start := p.pos
	c := p.src[p.pos]
	switch c {
	case '+':
		p.pos++
		return token{kind: tokPlus, text: "+", pos: start}
	case '-':
		if p.pos+1 < len(p.src) && p.src[p.pos+1] == '>' {
			p.pos += 2
			return token{kind: tokArrow, text: "->", pos: start}
		}
		p.pos++
		return token{kind: tokMinus, text: "-", pos: start}
	case '*':
		p.pos++
		return token{kind: tokStar, text: "*", pos: start}
	case '/':
		p.pos++
		return token{kind: tokSlash, text: "/", pos: start}
	case '(':
		p.pos++
		return token{kind: tokLParen, text: "(", pos: start}
	case ')':
		p.pos++
		return token{kind: tokRParen, text: ")", pos: start}
	case ':':
		if p.pos+1 < len(p.src) && p.src[p.pos+1] == ':' {
			p.pos += 2
			return token{kind: tokDoubleColon, text: "::", pos: start}
		}
		p.pos++
		return token{kind: tokIdent, text: ":", pos: start}
	case ',':
		p.pos++
		return token{kind: tokComma, text: ",", pos: start}
	case '\'':
		// single-quoted string
		p.pos++
		start = p.pos
		for p.pos < len(p.src) && p.src[p.pos] != '\'' {
			p.pos++
		}
		if p.pos >= len(p.src) {
			return token{kind: tokString, text: p.src[start:], pos: start - 1}
		}
		text := p.src[start:p.pos]
		p.pos++
		return token{kind: tokString, text: text, pos: start - 1}
	default:
		// ident or number
		start = p.pos
		if c >= '0' && c <= '9' {
			p.pos++
			for p.pos < len(p.src) {
				d := p.src[p.pos]
				if (d >= '0' && d <= '9') || d == '.' {
					p.pos++
					continue
				}
				break
			}
			return token{kind: tokNumber, text: p.src[start:p.pos], pos: start}
		}
		p.pos++
		for p.pos < len(p.src) {
			d := p.src[p.pos]
			if (d >= 'A' && d <= 'Z') || (d >= 'a' && d <= 'z') || (d >= '0' && d <= '9') || d == '_' || d == '.' {
				p.pos++
				continue
			}
			break
		}
		return token{kind: tokIdent, text: strings.TrimSpace(p.src[start:p.pos]), pos: start}
	}
}

func (p *exprParser) errorAtCurrent(msg string) error {
	pos := p.cur.pos
	if pos < 0 {
		pos = 0
	}
	if pos > len(p.src) {
		pos = len(p.src)
	}
	caret := strings.Repeat(" ", pos) + "^"
	return fmt.Errorf("%s\nat pos %d:\n%s\n%s", msg, pos, p.src, caret)
}

type exprNode interface {
	eval(types.Tuple) (any, error)
}

type literalNode struct{ v any }

func (n *literalNode) eval(types.Tuple) (any, error) { return n.v, nil }

type identNode struct{ name string }

func (n *identNode) eval(t types.Tuple) (any, error) {
	return t[n.name], nil
}

type unaryNode struct{ inner exprNode }

func (n *unaryNode) eval(t types.Tuple) (any, error) {
	v, err := n.inner.eval(t)
	if err != nil {
		return nil, err
	}
	return -toFloat64(v), nil
}

type binOpNode struct {
	op          tokenKind
	left, right exprNode
}

func (n *binOpNode) eval(t types.Tuple) (any, error) {
	lv, err := n.left.eval(t)
	if err != nil {
		return nil, err
	}
	rv, err := n.right.eval(t)
	if err != nil {
		return nil, err
	}

	// SQL-like NULL propagation for arithmetic operators.
	if lv == nil || rv == nil {
		switch n.op {
		case tokPlus, tokMinus, tokStar, tokSlash:
			return nil, nil
		}
	}

	// Handle special cases like timestamp + interval
	if isTimestamp(lv) || isTimestamp(rv) {
		return evalTimeArithmetic(n.op, lv, rv)
	}

	lf := toFloat64(lv)
	rf := toFloat64(rv)
	switch n.op {
	case tokPlus:
		return lf + rf, nil
	case tokMinus:
		return lf - rf, nil
	case tokStar:
		return lf * rf, nil
	case tokSlash:
		if rf == 0 {
			return nil, fmt.Errorf("division by zero")
		}
		return lf / rf, nil
	default:
		return nil, fmt.Errorf("unsupported operator")
	}
}

func isTimestamp(v any) bool {
	switch v.(type) {
	case time.Time:
		return true
	case string:
		// Simple heuristic: if it looks like a date
		s := v.(string)
		return len(s) >= 10 && (s[4] == '-' || s[4] == '/')
	}
	return false
}

func evalTimeArithmetic(op tokenKind, lv, rv any) (any, error) {
	lt, le := toTime(lv)
	rt, re := toTime(rv)

	// timestamp + interval
	if le == nil && re != nil {
		dur, err := parseInterval(rv)
		if err != nil {
			return nil, err
		}
		if op == tokPlus {
			return lt.Add(dur), nil
		}
		if op == tokMinus {
			return lt.Add(-dur), nil
		}
	}
	// interval + timestamp
	if le != nil && re == nil {
		dur, err := parseInterval(lv)
		if err != nil {
			return nil, err
		}
		if op == tokPlus {
			return rt.Add(dur), nil
		}
	}
	// timestamp - timestamp = interval
	if le == nil && re == nil {
		if op == tokMinus {
			return lt.Sub(rt).Seconds(), nil
		}
	}

	return lv, nil
}

type jsonAccessNode struct {
	inner exprNode
	key   string
}

func (n *jsonAccessNode) eval(t types.Tuple) (any, error) {
	v, err := n.inner.eval(t)
	if err != nil {
		return nil, err
	}
	if v == nil {
		return nil, nil
	}

	var m map[string]any
	switch x := v.(type) {
	case map[string]any:
		m = x
	case string:
		if err := json.Unmarshal([]byte(x), &m); err != nil {
			return nil, nil
		}
	case []byte:
		if err := json.Unmarshal(x, &m); err != nil {
			return nil, nil
		}
	default:
		return nil, nil
	}

	// strip quotes from key if present
	key := strings.Trim(n.key, "'\"")
	return m[key], nil
}

type castNode struct {
	inner      exprNode
	targetType string
}

func (n *castNode) eval(t types.Tuple) (any, error) {
	v, err := n.inner.eval(t)
	if err != nil {
		return nil, err
	}
	switch n.targetType {
	case "BIGINT", "INT", "INTEGER":
		return toInt64(v), nil
	case "DOUBLE", "FLOAT":
		return toFloat64(v), nil
	case "VARCHAR", "TEXT", "STRING":
		return fmt.Sprintf("%v", v), nil
	case "TIMESTAMP", "TIME", "DATE":
		return toTime(v)
	default:
		return v, nil
	}
}

type funcCallNode struct {
	name string
	args []exprNode
}

func (n *funcCallNode) eval(t types.Tuple) (any, error) {
	var evaluatedArgs []any
	for _, arg := range n.args {
		v, err := arg.eval(t)
		if err != nil {
			return nil, err
		}
		evaluatedArgs = append(evaluatedArgs, v)
	}

	switch n.name {
	case "TIME_BUCKET":
		if len(evaluatedArgs) < 2 {
			return nil, fmt.Errorf("TIME_BUCKET requires 2 arguments")
		}
		return evalTimeBucket(evaluatedArgs[0], evaluatedArgs[1])
	case "EPOCH":
		if len(evaluatedArgs) < 1 {
			return nil, fmt.Errorf("EPOCH requires 1 argument")
		}
		return evalEpoch(evaluatedArgs[0])
	case "STRFTIME":
		if len(evaluatedArgs) < 2 {
			return nil, fmt.Errorf("STRFTIME requires 2 arguments")
		}
		return evalStrftime(evaluatedArgs[0], evaluatedArgs[1])
	case "ROUND":
		if len(evaluatedArgs) < 1 || len(evaluatedArgs) > 2 {
			return nil, fmt.Errorf("ROUND requires 1 or 2 arguments")
		}
		if len(evaluatedArgs) == 1 {
			return evalRound(evaluatedArgs[0], 0)
		}
		return evalRound(evaluatedArgs[0], toInt64(evaluatedArgs[1]))
	default:
		return nil, fmt.Errorf("unsupported function: %s", n.name)
	}
}

type intervalNode struct {
	val  string
	unit string
}

func (n *intervalNode) eval(types.Tuple) (any, error) {
	return n.val + " " + n.unit, nil
}

// --- time helpers ---

func evalTimeBucket(interval any, ts any) (any, error) {
	dur, err := parseInterval(interval)
	if err != nil {
		return nil, err
	}
	t, err := toTime(ts)
	if err != nil {
		return nil, err
	}
	// Go's Truncate works on Durations since Epoch, effectively bucketizing.
	return t.Truncate(dur), nil
}

func evalEpoch(ts any) (any, error) {
	t, err := toTime(ts)
	if err != nil {
		// try literal
		if f, err := strconv.ParseFloat(fmt.Sprintf("%v", ts), 64); err == nil {
			return int64(f), nil
		}
		return nil, err
	}
	return t.Unix(), nil
}

func evalStrftime(ts any, format any) (any, error) {
	if ts == nil {
		return nil, nil
	}
	t, err := toTime(ts)
	if err != nil {
		return nil, err
	}
	fmtStr := fmt.Sprintf("%v", format)
	// Minimal duckdb-like format conversion
	fmtStr = strings.ReplaceAll(fmtStr, "%Y", "2006")
	fmtStr = strings.ReplaceAll(fmtStr, "%m", "01")
	fmtStr = strings.ReplaceAll(fmtStr, "%d", "02")
	fmtStr = strings.ReplaceAll(fmtStr, "%H", "15")
	fmtStr = strings.ReplaceAll(fmtStr, "%M", "04")
	fmtStr = strings.ReplaceAll(fmtStr, "%S", "05")
	return t.Format(fmtStr), nil
}

func evalRound(value any, precision int64) (any, error) {
	if value == nil {
		return nil, nil
	}
	f, ok := types.ToFloat64Safe(value)
	if !ok {
		return nil, fmt.Errorf("ROUND unsupported value type: %T", value)
	}
	if precision == 0 {
		return math.Round(f), nil
	}
	pow := math.Pow10(int(precision))
	return math.Round(f*pow) / pow, nil
}

func toTime(v any) (time.Time, error) {
	if v == nil {
		return time.Time{}, nil
	}
	switch x := v.(type) {
	case time.Time:
		return x, nil
	case string:
		layouts := []string{
			"2006-01-02 15:04:05",
			"2006-01-02T15:04:05Z07:00",
			"2006-01-02",
			time.RFC3339,
		}
		for _, l := range layouts {
			if t, err := time.Parse(l, x); err == nil {
				return t, nil
			}
		}
		return time.Time{}, fmt.Errorf("cannot parse time: %s", x)
	case int64:
		switch {
		case x > 1e16:
			return time.Unix(0, x), nil
		case x > 1e14:
			return time.Unix(0, x*1e3), nil
		case x > 1e11:
			return time.Unix(0, x*1e6), nil
		default:
			return time.Unix(x, 0), nil
		}
	case int:
		xi := int64(x)
		switch {
		case xi > 1e16:
			return time.Unix(0, xi), nil
		case xi > 1e14:
			return time.Unix(0, xi*1e3), nil
		case xi > 1e11:
			return time.Unix(0, xi*1e6), nil
		default:
			return time.Unix(xi, 0), nil
		}
	case float64:
		xi := int64(x)
		switch {
		case xi > 1e16:
			return time.Unix(0, xi), nil
		case xi > 1e14:
			return time.Unix(0, xi*1e3), nil
		case xi > 1e11:
			return time.Unix(0, xi*1e6), nil
		default:
			return time.Unix(xi, 0), nil
		}
	case json.Number:
		if iv, err := x.Int64(); err == nil {
			switch {
			case iv > 1e16:
				return time.Unix(0, iv), nil
			case iv > 1e14:
				return time.Unix(0, iv*1e3), nil
			case iv > 1e11:
				return time.Unix(0, iv*1e6), nil
			default:
				return time.Unix(iv, 0), nil
			}
		}
		fv, err := x.Float64()
		if err != nil {
			return time.Time{}, fmt.Errorf("unsupported time conversion: %T", v)
		}
		iv := int64(fv)
		switch {
		case iv > 1e16:
			return time.Unix(0, iv), nil
		case iv > 1e14:
			return time.Unix(0, iv*1e3), nil
		case iv > 1e11:
			return time.Unix(0, iv*1e6), nil
		default:
			return time.Unix(iv, 0), nil
		}
	default:
		return time.Time{}, fmt.Errorf("unsupported time conversion: %T", v)
	}
}

func parseInterval(s any) (time.Duration, error) {
	str := strings.ToUpper(fmt.Sprintf("%v", s))
	parts := strings.Fields(str)
	if len(parts) < 1 {
		return 0, fmt.Errorf("invalid interval")
	}
	val, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid interval value: %s", parts[0])
	}
	unit := "SECOND"
	if len(parts) > 1 {
		unit = parts[1]
	}
	switch unit {
	case "SECOND", "SECONDS":
		return time.Duration(val) * time.Second, nil
	case "MIN", "MINS", "MINUTE", "MINUTES":
		return time.Duration(val) * time.Minute, nil
	case "HOUR", "HOURS":
		return time.Duration(val) * time.Hour, nil
	case "DAY", "DAYS":
		return time.Duration(val) * 24 * time.Hour, nil
	default:
		return 0, fmt.Errorf("unsupported interval unit: %s", unit)
	}
}
