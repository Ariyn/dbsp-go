package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/ariyn/dbsp/cmd/dbsp/config"
	"github.com/ariyn/dbsp/cmd/dbsp/pipeline"
	"github.com/ariyn/dbsp/cmd/dbsp/provider"
	"github.com/ariyn/dbsp/cmd/dbsp/sink"
	"github.com/ariyn/dbsp/cmd/dbsp/source"
	"github.com/ariyn/dbsp/internal/dbsp/op"
	sqlconv "github.com/ariyn/dbsp/internal/dbsp/sql"
	"github.com/ariyn/dbsp/internal/dbsp/types"
)

var compileIncrementalQuery = sqlconv.ParseQueryToIncrementalDBSP

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	configPath := flag.String("config", "config.yaml", "Path to configuration file")
	flag.Parse()

	configFile, err := os.ReadFile(*configPath)
	if err != nil {
		fmt.Printf("Error reading config file: %v\n", err)
		os.Exit(1)
	}

	cfg, err := parsePipelineConfig(configFile)
	if err != nil {
		fmt.Printf("Error parsing config file: %v\n", err)
		os.Exit(1)
	}

	if err := config.ApplyMemoryLimit(cfg.Pipeline.State.MemoryLimit); err != nil {
		fmt.Printf("Error applying memory limit: %v\n", err)
		os.Exit(1)
	}

	if err := validateMinimalContract(&cfg); err != nil {
		fmt.Printf("Unsupported config for minimal runtime: %v\n", err)
		os.Exit(1)
	}

	if err := runSinglePipeline(ctx, &cfg); err != nil {
		if ctx.Err() != nil {
			fmt.Println("Shutdown requested. Exiting...")
			return
		}
		fmt.Printf("Pipeline error: %v\n", err)
		os.Exit(1)
	}
}

func validateMinimalContract(cfg *config.PipelineConfig) error {
	if cfg == nil {
		return fmt.Errorf("pipeline config is nil")
	}
	if cfg.Pipeline.Source.Type != "http" {
		return fmt.Errorf("only source.type=http is supported")
	}
	if cfg.Pipeline.Transform.Type != "sql" {
		return fmt.Errorf("only transform.type=sql is supported")
	}
	if strings.TrimSpace(cfg.Pipeline.Transform.Query) == "" {
		return fmt.Errorf("transform.query is required")
	}
	if cfg.Pipeline.Sink.Type != "http_pull" {
		return fmt.Errorf("only sink.type=http_pull is supported")
	}
	return nil
}

func runSinglePipeline(ctx context.Context, cfg *config.PipelineConfig) error {
	query, rootNode, err := compileTransform(cfg)
	if err != nil {
		return err
	}

	src, err := newSource(cfg)
	if err != nil {
		return fmt.Errorf("initializing source: %w", err)
	}
	defer src.Close()

	parquetSchema, err := config.InferOrLoadParquetSchema(query, cfg.Pipeline.Source, cfg.Pipeline.Sink.Config)
	if err != nil {
		return fmt.Errorf("inferring parquet schema: %w", err)
	}
	if err := validatePartitionColumns(parquetSchema, rootNode.PartitionBy); err != nil {
		return err
	}

	snk, err := newSink(cfg, rootNode, parquetSchema)
	if err != nil {
		return fmt.Errorf("initializing sink: %w", err)
	}
	defer snk.Close()

	fmt.Println("Starting minimal pipeline (http -> sql -> http_pull)...")
	if err := pipeline.RunPipeline(ctx, src, snk, func(batch types.Batch) (types.Batch, error) {
		return op.Execute(rootNode, batch)
	}); err != nil {
		return err
	}
	fmt.Println("Pipeline finished.")
	return nil
}

func newSource(cfg *config.PipelineConfig) (provider.Source, error) {
	var httpCfg config.HTTPSourceConfig
	if err := config.DecodeTo(cfg.Pipeline.Source.Config, &httpCfg); err != nil {
		return nil, fmt.Errorf("failed to decode http source config: %w", err)
	}
	return source.NewHTTPSource(httpCfg)
}

func newSink(cfg *config.PipelineConfig, rootNode *op.Node, parquetSchema *config.ParquetSchema) (provider.Sink, error) {
	var httpPullCfg config.HTTPPullSinkConfig
	if err := config.DecodeTo(cfg.Pipeline.Sink.Config, &httpPullCfg); err != nil {
		return nil, fmt.Errorf("failed to decode http_pull sink config: %w", err)
	}
	baseSink, err := sink.NewHTTPPullSink(httpPullCfg, rootNode.PartitionBy, parquetSchema)
	if err != nil {
		return nil, err
	}
	return baseSink, nil
}

func compileTransform(cfg *config.PipelineConfig) (string, *op.Node, error) {
	if cfg.Pipeline.Transform.Type != "sql" {
		return "", nil, fmt.Errorf("unsupported transform type: %s", cfg.Pipeline.Transform.Type)
	}
	query := strings.TrimSpace(cfg.Pipeline.Transform.Query)
	if query == "" {
		return "", nil, fmt.Errorf("transform query is empty")
	}
	rootNode, err := compileIncrementalQuery(query)
	if err != nil {
		return "", nil, fmt.Errorf("compiling SQL query: %w", err)
	}
	return query, rootNode, nil
}

func validatePartitionColumns(schema *config.ParquetSchema, partitionBy []string) error {
	if len(partitionBy) == 0 || schema == nil {
		return nil
	}
	available := make(map[string]struct{}, len(schema.Columns))
	for _, col := range schema.Columns {
		available[col.Name] = struct{}{}
	}
	missing := make([]string, 0, len(partitionBy))
	for _, key := range partitionBy {
		if _, ok := available[key]; !ok {
			missing = append(missing, key)
		}
	}
	if len(missing) == 0 {
		return nil
	}
	return fmt.Errorf("http_pull partition keys must be present in query output; missing=%s", strings.Join(missing, ","))
}

func parseHumanBytes(raw string) (int64, error) {
	return config.ParseHumanBytes(raw)
}

func parsePipelineConfig(configFile []byte) (config.PipelineConfig, error) {
	return config.ParsePipelineConfig(configFile)
}
