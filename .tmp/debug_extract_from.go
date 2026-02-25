package main

import (
	"fmt"
	"os"
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

func hasKeywordAtWordBoundary(upper string, i int, kw string) bool {
	if i < 0 || i+len(kw) > len(upper) {
		return false
	}
	if upper[i:i+len(kw)] != kw {
		return false
	}
	if i > 0 {
		c := upper[i-1]
		if (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_' {
			return false
		}
	}
	j := i + len(kw)
	if j < len(upper) {
		c := upper[j]
		if (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_' {
			return false
		}
	}
	return true
}

func extractSelectClause(query string) (string, error) {
	upper := strings.ToUpper(query)
	selectIdx := strings.Index(upper, "SELECT")
	if selectIdx == -1 {
		return "", fmt.Errorf("query must contain SELECT")
	}

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
		return "", fmt.Errorf("query must contain FROM")
	}
	return strings.TrimSpace(query[selectIdx+len("SELECT") : fromIdx]), nil
}

func main() {
	b, _ := os.ReadFile("experiments/config.yaml")
	var c cfg
	_ = yaml.Unmarshal(b, &c)
	p := parser.NewParser()
	stmt, _ := p.Parse(strings.TrimSpace(c.Pipeline.Transform.Query))
	sel := stmt.(*ast.Select)
	s := sel.With[2].Select.String()
	fmt.Println("CTE2 SQL:\n", s)
	cl, err := extractSelectClause(s)
	fmt.Println("extract err:", err)
	fmt.Println("clause:\n", cl)
}
