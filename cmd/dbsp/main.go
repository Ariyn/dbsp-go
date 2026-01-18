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
	"github.com/ariyn/dbsp/internal/dbsp/wal"
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

	// If Parquet sink is selected, infer/load and cache output schema at SQL-analysis time.
	var parquetSchema *ParquetSchema
	if config.Pipeline.Sink.Type == "parquet" {
		parquetSchema, err = inferOrLoadParquetSchema(query, config.Pipeline.Source, config.Pipeline.Sink.Config)
		if err != nil {
			fmt.Printf("Error inferring parquet schema: %v\n", err)
			os.Exit(1)
		}
	}

	if config.Pipeline.Transform.Watermark.Enabled {
		wmCfg, err := buildWatermarkConfig(config.Pipeline.Transform.Watermark)
		if err != nil {
			fmt.Printf("Error parsing watermark config: %v\n", err)
			os.Exit(1)
		}
		applyWatermarkConfig(rootNode, wmCfg)
		fmt.Printf("Applied watermark enabled=%v policy=%v\n", wmCfg.Enabled, wmCfg.Policy)
	}

	if config.Pipeline.Transform.JoinTTL != "" {
		ttl, err := parseJoinTTL(config.Pipeline.Transform.JoinTTL)
		if err != nil {
			fmt.Printf("Error parsing join_ttl: %v\n", err)
			os.Exit(1)
		}
		if ttl > 0 {
			applyJoinTTL(rootNode, ttl)
			fmt.Printf("Applied join_ttl=%s\n", ttl)
		}
	}

	// 4. Initialize Sink
	var sink Sink
	switch config.Pipeline.Sink.Type {
	case "console":
		sink, err = NewConsoleSink(config.Pipeline.Sink.Config)
	case "file":
		sink, err = NewFileSink(config.Pipeline.Sink.Config)
	case "parquet":
		sink, err = NewParquetSink(config.Pipeline.Sink.Config, parquetSchema)
	default:
		err = fmt.Errorf("unsupported sink type: %s", config.Pipeline.Sink.Type)
	}
	if err != nil {
		fmt.Printf("Error initializing sink: %v\n", err)
		os.Exit(1)
	}
	sink, err = wrapSinkWithBatchingIfConfigured(config.Pipeline.Sink.Config, sink)
	if err != nil {
		fmt.Printf("Error initializing sink batching: %v\n", err)
		os.Exit(1)
	}
	defer sink.Close()

	// 4.5 Initialize WAL (optional)
	var writeAheadLog *wal.SQLiteWAL
	if config.Pipeline.WAL.Enabled {
		writeAheadLog, err = wal.NewSQLiteWAL(config.Pipeline.WAL.Path)
		if err != nil {
			fmt.Printf("Error initializing WAL: %v\n", err)
			os.Exit(1)
		}
		defer writeAheadLog.Close()
		fmt.Printf("WAL enabled: sqlite=%s\n", config.Pipeline.WAL.Path)
	}

	// 5. Run Pipeline
	fmt.Println("Starting pipeline...")
	err = runPipeline(ctx, source, sink, func(batch types.Batch) (types.Batch, error) {
		return op.Execute(rootNode, batch)
	}, writeAheadLog,
		pipelineSnapshotterFunc{
			snap:    func() ([]byte, error) { return op.SnapshotGraph(rootNode) },
			restore: func(b []byte) error { return op.RestoreGraph(rootNode, b) },
		},
		config.Pipeline.WAL.CheckpointEveryBatches,
	)
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
