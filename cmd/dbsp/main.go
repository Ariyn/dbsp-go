package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/ariyn/dbsp/cmd/dbsp/config"
	"github.com/ariyn/dbsp/cmd/dbsp/pipeline"
	"github.com/ariyn/dbsp/cmd/dbsp/provider"
	"github.com/ariyn/dbsp/cmd/dbsp/sink"
	"github.com/ariyn/dbsp/cmd/dbsp/source"
	"github.com/ariyn/dbsp/internal/dbsp/ir"
	"github.com/ariyn/dbsp/internal/dbsp/op"
	sqlconv "github.com/ariyn/dbsp/internal/dbsp/sql"
	"github.com/ariyn/dbsp/internal/dbsp/types"
	"github.com/ariyn/dbsp/internal/metrics"
)

var compileIncrementalQuery = sqlconv.ParseQueryToIncrementalDBSP

func main() {
	if len(os.Args) > 1 && os.Args[1] == "graph" {
		if err := runGraphCommand(os.Args[2:]); err != nil {
			fmt.Printf("graph error: %v\n", err)
			os.Exit(1)
		}
		return
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	startObservabilityServers()

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

func startObservabilityServers() {
	pprofAddr := strings.TrimSpace(os.Getenv("DBSP_PPROF_ADDR"))
	metricsAddr := strings.TrimSpace(os.Getenv("DBSP_METRICS_ADDR"))
	metricsPath := strings.TrimSpace(os.Getenv("DBSP_METRICS_PATH"))
	if metricsPath == "" {
		metricsPath = "/metrics"
	}

	switch {
	case pprofAddr == "" && metricsAddr == "":
		return
	case pprofAddr != "" && pprofAddr == metricsAddr:
		go serveObservability("pprof+metrics", pprofAddr, newObservabilityMux(true, metricsPath))
	default:
		if pprofAddr != "" {
			go serveObservability("pprof", pprofAddr, newObservabilityMux(true, ""))
		}
		if metricsAddr != "" {
			go serveObservability("metrics", metricsAddr, newObservabilityMux(false, metricsPath))
		}
	}
}

func serveObservability(label, addr string, handler http.Handler) {
	fmt.Printf("%s listening on %s\n", label, addr)
	if err := http.ListenAndServe(addr, handler); err != nil {
		fmt.Printf("%s server error: %v\n", label, err)
	}
}

func newObservabilityMux(includePprof bool, metricsPath string) *http.ServeMux {
	mux := http.NewServeMux()
	if metricsPath != "" {
		mux.Handle(metricsPath, metrics.Handler())
	}
	if includePprof {
		registerPprofHandlers(mux)
	}
	return mux
}

func registerPprofHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
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
	query, rootNode, requiredFields, requiredFieldHints, err := compileTransform(cfg)
	if err != nil {
		return err
	}
	stateBackend, err := op.NewStateBackendFromConfig(cfg.Pipeline.State.Enabled, cfg.Pipeline.State.Type, cfg.Pipeline.State.Path)
	if err != nil {
		return fmt.Errorf("initializing state backend: %w", err)
	}
	if stateBackend != nil {
		defer stateBackend.Close()
		op.ApplyStateBackend(rootNode, stateBackend, "pipeline")
	}
	if ttl, err := config.ParseDuration(cfg.Pipeline.State.StateTTL); err != nil {
		return fmt.Errorf("invalid state_ttl: %w", err)
	} else if ttl > 0 {
		op.ApplyStateTTL(rootNode, ttl)
	}
	if cfg.Pipeline.State.OnlyLastLag {
		op.ApplyOnlyLastLag(rootNode, true)
	}

	src, err := newSource(cfg, requiredFields, requiredFieldHints)
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

	// WAL Recovery
	if hsrc, ok := src.(*source.HTTPSource); ok {
		var httpCfg config.HTTPSourceConfig
		_ = config.DecodeTo(cfg.Pipeline.Source.Config, &httpCfg)
		if httpCfg.WALDir != "" {
			fmt.Printf("Replaying WAL from %s...\n", httpCfg.WALDir)
			if err := hsrc.ReplayWAL(httpCfg.WALDir); err != nil {
				fmt.Printf("Warning: WAL replay error: %v\n", err)
			}
		}

		// Periodic Checkpoint (Phase 03)
		if httpCfg.CheckpointDir != "" {
			interval, _ := config.ParseDuration(httpCfg.CheckpointInterval)
			if interval <= 0 {
				interval = 5 * time.Minute
			}
			go func() {
				ticker := time.NewTicker(interval)
				defer ticker.Stop()
				for {
					select {
					case <-ctx.Done():
						return
					case <-ticker.C:
						if psink, ok := snk.(*sink.HTTPPullSink); ok {
							fmt.Printf("Creating periodic checkpoint in %s...\n", httpCfg.CheckpointDir)
							if err := psink.Checkpoint(httpCfg.CheckpointDir); err != nil {
								fmt.Printf("Checkpoint error: %v\n", err)
							}
						}
					}
				}
			}()
		}
	}

	if err := pipeline.RunPipeline(ctx, src, snk, func(batch types.Batch) (types.Batch, error) {
		return op.Execute(rootNode, batch)
	}); err != nil {
		return err
	}
	fmt.Println("Pipeline finished.")
	return nil
}

func newSource(cfg *config.PipelineConfig, requiredFields map[string]struct{}, requiredFieldHints map[string]string) (provider.Source, error) {
	var httpCfg config.HTTPSourceConfig
	if err := config.DecodeTo(cfg.Pipeline.Source.Config, &httpCfg); err != nil {
		return nil, fmt.Errorf("failed to decode http source config: %w", err)
	}
	httpCfg.Schema = mergeSourceSchemaHints(httpCfg.Schema, requiredFieldHints)
	return source.NewHTTPSource(httpCfg, requiredFields)
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

func compileTransform(cfg *config.PipelineConfig) (string, *op.Node, map[string]struct{}, map[string]string, error) {
	if cfg.Pipeline.Transform.Type != "sql" {
		return "", nil, nil, nil, fmt.Errorf("unsupported transform type: %s", cfg.Pipeline.Transform.Type)
	}
	query := strings.TrimSpace(cfg.Pipeline.Transform.Query)
	if query == "" {
		return "", nil, nil, nil, fmt.Errorf("transform query is empty")
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
		return "", nil, nil, nil, fmt.Errorf("compiling SQL query: %w", err)
	}

	rootNode, err := compileIncrementalQuery(query, options...)
	if err != nil {
		return "", nil, nil, nil, fmt.Errorf("compiling SQL query: %w", err)
	}
	requiredFields := ir.CollectRequiredInputColumns(logicalPlan)
	requiredFieldHints := ir.CollectRequiredInputTypeHints(logicalPlan)
	if strings.TrimSpace(os.Getenv("DBSP_DEBUG_FIELDS")) != "" {
		if requiredFields == nil {
			fmt.Println("DEBUG requiredFields: <all>")
		} else {
			fmt.Printf("DEBUG requiredFields (%d): %v\n", len(requiredFields), requiredFields)
		}
		if len(requiredFieldHints) == 0 {
			fmt.Println("DEBUG requiredFieldHints: <empty>")
		} else {
			fmt.Printf("DEBUG requiredFieldHints (%d): %v\n", len(requiredFieldHints), requiredFieldHints)
		}
	}
	return query, rootNode, requiredFields, requiredFieldHints, nil
}

func mergeSourceSchemaHints(schema map[string]string, hints map[string]string) map[string]string {
	if len(hints) == 0 {
		return schema
	}
	merged := make(map[string]string, len(schema)+len(hints))
	for name, typ := range schema {
		merged[name] = typ
	}
	for name, typ := range hints {
		if _, exists := merged[name]; !exists {
			merged[name] = typ
		}
	}
	return merged
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
