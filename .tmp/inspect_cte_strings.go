package main

import (
	"fmt"
	"os"
	"regexp"
	"strings"

	"github.com/Ariyn/tree-sitter-duckdb/bindings/go/ast"
	"github.com/Ariyn/tree-sitter-duckdb/bindings/go/parser"
	"gopkg.in/yaml.v3"
)

type cfg struct {
	Pipeline struct {
		Transform struct {
			Query string `yaml:"query"`
		} `yaml:"transform"`
	} `yaml:"pipeline"`
}

func normalizeQueryForParser(query string) string {
	q := query
	reInterval := regexp.MustCompile(`(?i)INTERVAL\s*'([0-9]+)'\s*([A-Z]+)`)
	q = reInterval.ReplaceAllString(q, "INTERVAL $1 $2")
	if inlined, ok := inlineSingleCTEForParser(q); ok {
		q = inlined
	}
	return q
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

func main() {
	b, _ := os.ReadFile("experiments/config.yaml")
	var c cfg
	_ = yaml.Unmarshal(b, &c)
	q := strings.TrimSpace(c.Pipeline.Transform.Query)
	fmt.Println("orig first 120:", q[:120])

	cand := normalizeQueryForParser(q)
	fmt.Println("cand first 120:", cand[:120])

	p := parser.NewParser()
	stmt, err := p.Parse(cand)
	fmt.Println("parse cand err:", err)
	if err != nil {
		return
	}

	sel, ok := stmt.(*ast.Select)
	fmt.Println("is select:", ok, "with count:", len(sel.With), "from count:", len(sel.From))
	if !ok {
		return
	}
	for i, cte := range sel.With {
		s := cte.Select.String()
		fmt.Printf("cte[%d] name=%s len=%d hasFROM=%v\n", i, cte.Name, len(s), strings.Contains(strings.ToUpper(s), "FROM"))
		if len(s) > 140 {
			fmt.Println(s[:140])
		} else {
			fmt.Println(s)
		}
	}
	mainStr := sel.String()
	fmt.Println("main select has FROM:", strings.Contains(strings.ToUpper(mainStr), "FROM"))
	fmt.Println(mainStr)
}
