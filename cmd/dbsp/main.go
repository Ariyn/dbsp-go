package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/ariyn/dbsp/cmd/dbsp/config"
	"github.com/ariyn/dbsp/cmd/dbsp/pipeline"
	"github.com/ariyn/dbsp/cmd/dbsp/provider"
	"github.com/ariyn/dbsp/cmd/dbsp/sink"
	"github.com/ariyn/dbsp/cmd/dbsp/source"
	"github.com/ariyn/dbsp/cmd/dbsp/watermark"
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

	var cfg config.PipelineConfig
	if err := yaml.Unmarshal(configFile, &cfg); err != nil {
		fmt.Printf("Error parsing config file: %v\n", err)
		os.Exit(1)
	}

	// 2. Initialize Source
	var src provider.Source
	switch cfg.Pipeline.Source.Type {
	case "csv":
		src, err = source.NewCSVSource(cfg.Pipeline.Source.Config)
	case "http":
		src, err = source.NewHTTPSource(cfg.Pipeline.Source.Config)
	case "chain":
		src, err = source.NewChainSource(cfg.Pipeline.Source.Config)
	default:
		err = fmt.Errorf("unsupported source type: %s", cfg.Pipeline.Source.Type)
	}
	if err != nil {
		fmt.Printf("Error initializing source: %v\n", err)
		os.Exit(1)
	}
	defer src.Close()

	// 3. Initialize Transform (SQL)
	if cfg.Pipeline.Transform.Type != "sql" {
		fmt.Printf("Unsupported transform type: %s\n", cfg.Pipeline.Transform.Type)
		os.Exit(1)
	}

	query := cfg.Pipeline.Transform.Query
	fmt.Printf("Compiling Query: %s\n", query)

	rootNode, err := sqlconv.ParseQueryToIncrementalDBSP(query)
	if err != nil {
		fmt.Printf("Error compiling SQL query: %v\n", err)
		os.Exit(1)
	}

	// If Parquet sink is selected, infer/load and cache output schema at SQL-analysis time.
	var parquetSchema *config.ParquetSchema
	if cfg.Pipeline.Sink.Type == "parquet" {
		parquetSchema, err = config.InferOrLoadParquetSchema(query, cfg.Pipeline.Source, cfg.Pipeline.Sink.Config)
		if err != nil {
			fmt.Printf("Error inferring parquet schema: %v\n", err)
			os.Exit(1)
		}
	}

	if cfg.Pipeline.Transform.Watermark.Enabled {
		wmCfg, err := watermark.BuildWatermarkConfig(cfg.Pipeline.Transform.Watermark)
		if err != nil {
			fmt.Printf("Error parsing watermark config: %v\n", err)
			os.Exit(1)
		}
		watermark.ApplyWatermarkConfig(rootNode, wmCfg)
		fmt.Printf("Applied watermark enabled=%v policy=%v\n", wmCfg.Enabled, wmCfg.Policy)
	}

	if cfg.Pipeline.Transform.JoinTTL != "" {
		ttl, err := pipeline.ParseJoinTTL(cfg.Pipeline.Transform.JoinTTL)
		if err != nil {
			fmt.Printf("Error parsing join_ttl: %v\n", err)
			os.Exit(1)
		}
		if ttl > 0 {
			pipeline.ApplyJoinTTL(rootNode, ttl)
			fmt.Printf("Applied join_ttl=%s\n", ttl)
		}
	}

	// 4. Initialize Sink
	var snk provider.Sink
	switch cfg.Pipeline.Sink.Type {
	case "console":
		snk, err = sink.NewConsoleSink(cfg.Pipeline.Sink.Config)
	case "file":
		snk, err = sink.NewFileSink(cfg.Pipeline.Sink.Config)
	case "parquet":
		snk, err = sink.NewParquetSink(cfg.Pipeline.Sink.Config, parquetSchema)
	default:
		err = fmt.Errorf("unsupported sink type: %s", cfg.Pipeline.Sink.Type)
	}
	if err != nil {
		fmt.Printf("Error initializing sink: %v\n", err)
		os.Exit(1)
	}
	snk, err = sink.WrapSinkWithBatchingIfConfigured(cfg.Pipeline.Sink.Config, snk)
	if err != nil {
		fmt.Printf("Error initializing sink batching: %v\n", err)
		os.Exit(1)
	}
	defer snk.Close()

	// 4.5 Initialize WAL (optional)
	var writeAheadLog *wal.SQLiteWAL
	if cfg.Pipeline.WAL.Enabled {
		writeAheadLog, err = wal.NewSQLiteWAL(cfg.Pipeline.WAL.Path)
		if err != nil {
			fmt.Printf("Error initializing WAL: %v\n", err)
			os.Exit(1)
		}
		defer writeAheadLog.Close()
		fmt.Printf("WAL enabled: sqlite=%s\n", cfg.Pipeline.WAL.Path)
	}

	// 5. Run Pipeline
	fmt.Println("Starting pipeline...")
	err = pipeline.RunPipeline(ctx, src, snk, func(batch types.Batch) (types.Batch, error) {
		return op.Execute(rootNode, batch)
	}, writeAheadLog,
		pipeline.PipelineSnapshotterFunc{
			SnapFunc:    func() ([]byte, error) { return op.SnapshotGraph(rootNode) },
			RestoreFunc: func(b []byte) error { return op.RestoreGraph(rootNode, b) },
		},
		cfg.Pipeline.WAL.CheckpointEveryBatches,
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
