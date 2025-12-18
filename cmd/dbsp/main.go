package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/ariyn/dbsp/internal/dbsp/op"
	sqlconv "github.com/ariyn/dbsp/internal/dbsp/sql"
	"github.com/ariyn/dbsp/internal/dbsp/types"
	"gopkg.in/yaml.v3"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	configPath := flag.String("config", "config.yaml", "Path to configuration file")
	flag.Parse()

	// 1. Load Config
	configFile, err := os.ReadFile(*configPath)
	if err != nil {
		fmt.Printf("Error reading config file: %v\n", err)
		os.Exit(1)
	}

	var config PipelineConfig
	if err := yaml.Unmarshal(configFile, &config); err != nil {
		fmt.Printf("Error parsing config file: %v\n", err)
		os.Exit(1)
	}

	// 2. Initialize Source
	var source Source
	switch config.Pipeline.Source.Type {
	case "csv":
		source, err = NewCSVSource(config.Pipeline.Source.Config)
	case "http":
		source, err = NewHTTPSource(config.Pipeline.Source.Config)
	case "chain":
		source, err = NewChainSource(config.Pipeline.Source.Config)
	default:
		err = fmt.Errorf("unsupported source type: %s", config.Pipeline.Source.Type)
	}
	if err != nil {
		fmt.Printf("Error initializing source: %v\n", err)
		os.Exit(1)
	}
	defer source.Close()

	// 3. Initialize Transform (SQL)
	if config.Pipeline.Transform.Type != "sql" {
		fmt.Printf("Unsupported transform type: %s\n", config.Pipeline.Transform.Type)
		os.Exit(1)
	}

	query := config.Pipeline.Transform.Query
	fmt.Printf("Compiling Query: %s\n", query)

	rootNode, err := sqlconv.ParseQueryToIncrementalDBSP(query)
	if err != nil {
		fmt.Printf("Error compiling SQL query: %v\n", err)
		os.Exit(1)
	}

	// 4. Initialize Sink
	var sink Sink
	switch config.Pipeline.Sink.Type {
	case "console":
		sink, err = NewConsoleSink(config.Pipeline.Sink.Config)
	case "file":
		sink, err = NewFileSink(config.Pipeline.Sink.Config)
	default:
		err = fmt.Errorf("unsupported sink type: %s", config.Pipeline.Sink.Type)
	}
	if err != nil {
		fmt.Printf("Error initializing sink: %v\n", err)
		os.Exit(1)
	}
	defer sink.Close()

	// 5. Run Pipeline
	fmt.Println("Starting pipeline...")
	err = runPipeline(ctx, source, sink, func(batch types.Batch) (types.Batch, error) {
		return op.Execute(rootNode, batch)
	})
	if err != nil {
		if ctx.Err() != nil {
			fmt.Println("Shutdown requested. Exiting...")
			return
		}
		fmt.Printf("Pipeline error: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("Pipeline finished.")
}

