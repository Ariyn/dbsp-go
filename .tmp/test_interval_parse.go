package main

import (
	"fmt"

	"github.com/Ariyn/tree-sitter-duckdb/bindings/go/parser"
)

func main() {
	cases := []string{
		"SELECT TIME_BUCKET(INTERVAL '5' MINUTE, ts::TIMESTAMP) FROM sales",
		"SELECT TIME_BUCKET(INTERVAL 5 MINUTE, ts::TIMESTAMP) FROM sales",
		"SELECT TIME_BUCKET(INTERVAL 5, ts::TIMESTAMP) FROM sales",
	}
	for _, q := range cases {
		p := parser.NewParser()
		_, err := p.Parse(q)
		fmt.Printf("%q => %v\n", q, err)
	}
}
