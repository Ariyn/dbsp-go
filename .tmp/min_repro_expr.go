package main

import (
	"fmt"

	"github.com/Ariyn/tree-sitter-duckdb/bindings/go/ast"
	"github.com/Ariyn/tree-sitter-duckdb/bindings/go/parser"
)

func main() {
	q := `SELECT
  panel_position AS id,
  TIME_BUCKET(INTERVAL 5 MINUTE, timestamp::TIMESTAMP) AS binned_date,
  SUM((p_out + p_out_last) * timedelta_second / 2.0 / 3600.0) AS energy
FROM power_calc
GROUP BY id, binned_date`
	p := parser.NewParser()
	stmt, err := p.Parse(q)
	fmt.Println("parse err:", err)
	if err != nil {
		return
	}
	sel := stmt.(*ast.Select)
	fmt.Println("sel.String():")
	fmt.Println(sel.String())
}
