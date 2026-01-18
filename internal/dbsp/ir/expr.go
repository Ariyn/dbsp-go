package ir

import (
	"fmt"
	"strconv"
	"strings"

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

func indexKeywordOutsideParens(upperSQL string, kw string) int {
	depth := 0
	for i := 0; i < len(upperSQL); i++ {
		switch upperSQL[i] {
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
		if i+len(kw) <= len(upperSQL) && upperSQL[i:i+len(kw)] == kw {
			// word boundary
			leftOK := i == 0 || !isIdentChar(upperSQL[i-1])
			rightOK := i+len(kw) == len(upperSQL) || !isIdentChar(upperSQL[i+len(kw)])
			if leftOK && rightOK {
				return i
			}
		}
	}
	return -1
}

func isIdentChar(c byte) bool {
	return (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_' || c == '.'
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
	switch x := v.(type) {
	case float64:
		return x
	case float32:
		return float64(x)
	case int:
		return float64(x)
	case int64:
		return float64(x)
	case string:
		f, _ := strconv.ParseFloat(x, 64)
		return f
	default:
		return 0
	}
}

func toInt64(v any) int64 {
	switch x := v.(type) {
	case int64:
		return x
	case int:
		return int64(x)
	case float64:
		return int64(x)
	case string:
		i, _ := strconv.ParseInt(x, 10, 64)
		return i
	default:
		return 0
	}
}

// --- arithmetic parser ---

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
)

type token struct {
	kind tokenKind
	text string
}

type exprParser struct {
	src   string
	pos   int
	cur   token
	peeks bool
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
		return nil, fmt.Errorf("unexpected token %q", p.cur.text)
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
	return p.parsePrimary()
}

func (p *exprParser) parsePrimary() (exprNode, error) {
	switch p.cur.kind {
	case tokIdent:
		id := p.cur.text
		p.cur = p.nextToken()
		return &identNode{name: id}, nil
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
			return nil, fmt.Errorf("expected )")
		}
		p.cur = p.nextToken()
		return inner, nil
	default:
		return nil, fmt.Errorf("unexpected token %q", p.cur.text)
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
		return token{kind: tokEOF}
	}

	c := p.src[p.pos]
	switch c {
	case '+':
		p.pos++
		return token{kind: tokPlus, text: "+"}
	case '-':
		p.pos++
		return token{kind: tokMinus, text: "-"}
	case '*':
		p.pos++
		return token{kind: tokStar, text: "*"}
	case '/':
		p.pos++
		return token{kind: tokSlash, text: "/"}
	case '(':
		p.pos++
		return token{kind: tokLParen, text: "("}
	case ')':
		p.pos++
		return token{kind: tokRParen, text: ")"}
	case '\'':
		// single-quoted string
		p.pos++
		start := p.pos
		for p.pos < len(p.src) && p.src[p.pos] != '\'' {
			p.pos++
		}
		if p.pos >= len(p.src) {
			return token{kind: tokString, text: p.src[start:]}
		}
		text := p.src[start:p.pos]
		p.pos++
		return token{kind: tokString, text: text}
	default:
		// ident or number
		start := p.pos
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
			return token{kind: tokNumber, text: p.src[start:p.pos]}
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
		return token{kind: tokIdent, text: strings.TrimSpace(p.src[start:p.pos])}
	}
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
