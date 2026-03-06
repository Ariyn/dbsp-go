package main

import (
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/ariyn/dbsp/internal/dbsp/graph"
	sqlconv "github.com/ariyn/dbsp/internal/dbsp/sql"
)

func runGraphCommand(args []string) error {
	fs := flag.NewFlagSet("graph", flag.ContinueOnError)
	configPath := fs.String("config", "config.yaml", "Path to configuration file")
	stage := fs.String("stage", "both", "Graph stage: logical|operator|both")
	out := fs.String("out", "", "Output file path for a single stage")
	outPrefix := fs.String("out-prefix", "", "Output file prefix for stage=both")
	verbose := fs.Bool("verbose", false, "Include detailed labels")
	if err := fs.Parse(args); err != nil {
		return err
	}

	stageValue := strings.ToLower(strings.TrimSpace(*stage))
	switch stageValue {
	case "logical", "operator", "both":
	default:
		return fmt.Errorf("invalid -stage %q: use logical|operator|both", *stage)
	}

	if stageValue == "both" && strings.TrimSpace(*out) != "" {
		return fmt.Errorf("-out is only valid when -stage is logical or operator")
	}
	if stageValue == "both" && strings.TrimSpace(*outPrefix) == "" {
		return fmt.Errorf("-out-prefix is required when -stage=both")
	}
	if stageValue != "both" && strings.TrimSpace(*outPrefix) != "" {
		return fmt.Errorf("-out-prefix is only valid when -stage=both")
	}

	cfgBytes, err := os.ReadFile(*configPath)
	if err != nil {
		return fmt.Errorf("read config: %w", err)
	}
	cfg, err := parsePipelineConfig(cfgBytes)
	if err != nil {
		return fmt.Errorf("parse config: %w", err)
	}

	if cfg.Pipeline.Transform.Type != "sql" {
		return fmt.Errorf("unsupported transform type: %s", cfg.Pipeline.Transform.Type)
	}
	query := strings.TrimSpace(cfg.Pipeline.Transform.Query)
	if query == "" {
		return fmt.Errorf("transform query is empty")
	}

	var options []sqlconv.ComplianceOption
	if cfg.Pipeline.Transform.SQLCompliance.StrictValidation {
		options = append(options, sqlconv.WithStrictValidation(true))
	}
	if cfg.Pipeline.Transform.SQLCompliance.FallbackWarn {
		options = append(options, sqlconv.WithFallbackWarn(true))
	}

	logicalPlan, err := sqlconv.ParseQueryToLogicalPlan(query, options...)
	if err != nil {
		return fmt.Errorf("compile logical plan: %w", err)
	}
	operatorRoot, err := sqlconv.ParseQueryToIncrementalDBSP(query, options...)
	if err != nil {
		return fmt.Errorf("compile operator graph: %w", err)
	}

	opts := graph.Options{Verbose: *verbose}

	switch stageValue {
	case "logical":
		dot := graph.LogicalPlanDOT(logicalPlan, opts)
		return writeDotOutput(dot, *out)
	case "operator":
		dot := graph.OperatorGraphDOT(operatorRoot, opts)
		return writeDotOutput(dot, *out)
	case "both":
		logicalDot := graph.LogicalPlanDOT(logicalPlan, opts)
		operatorDot := graph.OperatorGraphDOT(operatorRoot, opts)
		logicalPath := strings.TrimSpace(*outPrefix) + ".logical.dot"
		operatorPath := strings.TrimSpace(*outPrefix) + ".operator.dot"
		if err := os.WriteFile(logicalPath, []byte(logicalDot), 0644); err != nil {
			return fmt.Errorf("write %s: %w", logicalPath, err)
		}
		if err := os.WriteFile(operatorPath, []byte(operatorDot), 0644); err != nil {
			return fmt.Errorf("write %s: %w", operatorPath, err)
		}
		fmt.Printf("Wrote %s and %s\n", logicalPath, operatorPath)
		return nil
	}
	return nil
}

func writeDotOutput(dot string, outPath string) error {
	outPath = strings.TrimSpace(outPath)
	if outPath == "" {
		fmt.Print(dot)
		return nil
	}
	if err := os.WriteFile(outPath, []byte(dot), 0644); err != nil {
		return fmt.Errorf("write %s: %w", outPath, err)
	}
	fmt.Printf("Wrote %s\n", outPath)
	return nil
}
