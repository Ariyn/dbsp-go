package main

import (
	"fmt"
	"os"
	"regexp"

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

func try(name, q string) {
	p := parser.NewParser()
	_, err := p.Parse(q)
	fmt.Printf("%s => %v\n", name, err)
}

func main() {
	b, _ := os.ReadFile("experiments/config.yaml")
	var c cfg
	_ = yaml.Unmarshal(b, &c)
	q := c.Pipeline.Transform.Query
	try("original", q)
	reInterval := regexp.MustCompile(`(?i)INTERVAL\s*'([0-9]+)'\s*([A-Z]+)`)
	q2 := reInterval.ReplaceAllString(q, "INTERVAL $1 $2")
	try("normalized_interval", q2)
}
