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

func main() {
	b, _ := os.ReadFile("experiments/config.yaml")
	var c cfg
	_ = yaml.Unmarshal(b, &c)
	q := strings.TrimSpace(c.Pipeline.Transform.Query)

	p := parser.NewParser()
	stmt, err := p.Parse(q)
	fmt.Println("parse err:", err)
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
		fmt.Printf("cte[%d] %s hasFROM=%v hasGROUPBY=%v len=%d\n", i, cte.Name, strings.Contains(strings.ToUpper(s), "FROM"), strings.Contains(strings.ToUpper(s), "GROUP BY"), len(s))
		if len(s) > 200 {
			fmt.Println(s[:200])
		} else {
			fmt.Println(s)
		}
	}
}
