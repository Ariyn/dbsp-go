package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/Ariyn/tree-sitter-duckdb/bindings/go/ast"
	"github.com/Ariyn/tree-sitter-duckdb/bindings/go/parser"
	sqlconv "github.com/ariyn/dbsp/internal/dbsp/sql"
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

	_, err := sqlconv.ParseQueryToLogicalPlan(q)
	fmt.Println("full parse logical =>", err)

	p := parser.NewParser()
	stmt, err := p.Parse(q)
	fmt.Println("parser err =>", err)
	if err != nil {
		return
	}
	sel := stmt.(*ast.Select)
	for i, cte := range sel.With {
		s := cte.Select.String()
		_, cerr := sqlconv.ParseQueryToLogicalPlan(s)
		fmt.Printf("cte[%d] %s => %v\n", i, cte.Name, cerr)
	}
}
