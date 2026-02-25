package main

import (
	"fmt"
	"os"

	tree_sitter_duckdb "github.com/Ariyn/tree-sitter-duckdb/bindings/go"
	sitter "github.com/smacker/go-tree-sitter"
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
	b, err := os.ReadFile("experiments/config.yaml")
	if err != nil {
		panic(err)
	}
	var c cfg
	if err := yaml.Unmarshal(b, &c); err != nil {
		panic(err)
	}
	sql := c.Pipeline.Transform.Query

	p := sitter.NewParser()
	lang := sitter.NewLanguage(tree_sitter_duckdb.GetLanguage())
	p.SetLanguage(lang)
	tree := p.Parse(nil, []byte(sql))
	root := tree.RootNode()

	fmt.Printf("root type=%s hasError=%v range=%d-%d\n", root.Type(), root.HasError(), root.StartByte(), root.EndByte())
	if !root.HasError() {
		fmt.Println("no parse errors")
		return
	}

	var walk func(*sitter.Node)
	walk = func(n *sitter.Node) {
		if n == nil {
			return
		}
		if n.HasError() || n.IsMissing() || n.IsNamed() && n.Type() == "ERROR" {
			start := int(n.StartByte())
			end := int(n.EndByte())
			if start < 0 {
				start = 0
			}
			if end > len(sql) {
				end = len(sql)
			}
			snippet := sql[start:end]
			if len(snippet) > 120 {
				snippet = snippet[:120]
			}
			fmt.Printf("node=%s err=%v missing=%v range=%d-%d snippet=%q\n", n.Type(), n.HasError(), n.IsMissing(), n.StartByte(), n.EndByte(), snippet)
		}
		for i := 0; i < int(n.ChildCount()); i++ {
			walk(n.Child(i))
		}
	}

	walk(root)
}
