package main

import (
	"fmt"
	"os"

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
	q := c.Pipeline.Transform.Query

	_, err := sqlconv.ParseQueryToLogicalPlan(q)
	fmt.Printf("ParseQueryToLogicalPlan => %v\n", err)

	_, err = sqlconv.ParseQueryToDBSP(q)
	fmt.Printf("ParseQueryToDBSP => %v\n", err)

	_, err = sqlconv.ParseQueryToIncrementalDBSP(q)
	fmt.Printf("ParseQueryToIncrementalDBSP => %v\n", err)
}
