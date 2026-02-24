package main

import (
	"context"
	"errors"
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
	"github.com/ariyn/dbsp/cmd/dbsp/watermark"
	"github.com/ariyn/dbsp/internal/dbsp/op"
	sqlconv "github.com/ariyn/dbsp/internal/dbsp/sql"
	"github.com/ariyn/dbsp/internal/dbsp/types"
	"github.com/ariyn/dbsp/internal/dbsp/wal"
	walpkg "github.com/ariyn/dbsp/internal/dbsp/wal"
	"gopkg.in/yaml.v3"
)

type partitionRuntime struct {
	values      map[string]string
	rootNode    *op.Node
	sink        provider.Sink
	wal         *wal.SQLiteWAL
	snapshotter pipeline.PipelineSnapshotter
	batchCount  int
}

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
	if err := config.ValidatePartitionConfig(cfg.Pipeline.Partition, cfg.Pipeline.Transform.Query); err != nil {
		fmt.Printf("Invalid partition config: %v\n", err)
		os.Exit(1)
	}
	if cfg.Pipeline.Partition.Enabled && config.QueryContainsPartitionPredicate(cfg.Pipeline.Transform.Query, cfg.Pipeline.Partition.Keys) {
		fmt.Printf("Warning: transform.query contains partition predicate; runtime partition demux works best with unfiltered transform.query\n")
	}

	if cfg.Pipeline.Partition.Enabled {
		if err := runPartitionFanout(ctx, &cfg); err != nil {
			fmt.Printf("Partition fan-out failed: %v\n", err)
			os.Exit(1)
		}
		return
	}

	if err := runSinglePipeline(ctx, &cfg, nil); err != nil {
		if ctx.Err() != nil {
			fmt.Println("Shutdown requested. Exiting...")
			return
		}
		fmt.Printf("Pipeline error: %v\n", err)
		os.Exit(1)
	}
}

func runPartitionFanout(ctx context.Context, cfg *config.PipelineConfig) error {
	partCfg := cfg.Pipeline.Partition
	fmt.Printf("Partition runtime demux enabled: keys=%v\n", partCfg.Keys)

	src, err := newSource(cfg)
	if err != nil {
		return fmt.Errorf("initializing source: %w", err)
	}
	defer src.Close()

	runtimes := map[string]*partitionRuntime{}
	defer closePartitionRuntimes(runtimes)

	droppedRecords := 0
	for {
		batch, err := src.NextBatch()
		if err != nil {
			return err
		}
		if batch == nil {
			if droppedRecords > 0 {
				fmt.Printf("Partition demux dropped %d records without matching partition job values\n", droppedRecords)
			}
			return ctx.Err()
		}

		routed := map[string]types.Batch{}
		for _, td := range batch {
			partitionValues, ok := extractPartitionValues(td.Tuple, partCfg.Keys)
			if !ok {
				droppedRecords++
				continue
			}
			key := makePartitionKey(partCfg.Keys, partitionValues)
			if _, exists := runtimes[key]; !exists {
				rt, err := buildPartitionRuntime(cfg, partitionValues)
				if err != nil {
					return fmt.Errorf("initializing runtime for partition %s: %w", config.PartitionSummary(partitionValues, partCfg.Keys), err)
				}
				runtimes[key] = rt
				fmt.Printf("Created partition runtime (%s)\n", config.PartitionSummary(partitionValues, partCfg.Keys))
			}
			routed[key] = append(routed[key], td)
		}

		for key, partBatch := range routed {
			if len(partBatch) == 0 {
				continue
			}
			rt := runtimes[key]
			if err := runPartitionBatch(ctx, cfg, rt, partBatch); err != nil {
				return fmt.Errorf("partition %s: %w", config.PartitionSummary(rt.values, partCfg.Keys), err)
			}
		}

		if ctx.Err() != nil {
			return ctx.Err()
		}
	}
}

// 3. Initialize Transform (SQL)

func runSinglePipeline(ctx context.Context, cfg *config.PipelineConfig, partitionValues map[string]string) error {
	if cfg.Pipeline.Transform.Type != "sql" {
		return fmt.Errorf("unsupported transform type: %s", cfg.Pipeline.Transform.Type)
	}
	query := strings.TrimSpace(cfg.Pipeline.Transform.Query)
	if query == "" {
		return fmt.Errorf("transform query is empty")
	}

	src, err := newSource(cfg)
	if err != nil {
		return fmt.Errorf("initializing source: %w", err)
	}
	defer src.Close()

	fmt.Printf("Compiling Query: %s\n", query)

	rootNode, err := sqlconv.ParseQueryToIncrementalDBSP(query)
	if err != nil {
		return fmt.Errorf("compiling SQL query: %w", err)
	}

	sinkCfg := cloneConfigMap(cfg.Pipeline.Sink.Config)
	if cfg.Pipeline.Partition.Enabled && len(partitionValues) > 0 {
		applyHivePathToSink(cfg.Pipeline.Sink.Type, sinkCfg, cfg.Pipeline.Partition.Keys, partitionValues)
	}

	// If Parquet/HTTPPull sink is selected, infer/load and cache output schema at SQL-analysis time.
	var parquetSchema *config.ParquetSchema
	if cfg.Pipeline.Sink.Type == "parquet" || cfg.Pipeline.Sink.Type == "http_pull" {
		parquetSchema, err = config.InferOrLoadParquetSchema(query, cfg.Pipeline.Source, sinkCfg)
		if err != nil {
			return fmt.Errorf("inferring parquet schema: %w", err)
		}
	}

	if cfg.Pipeline.Transform.Watermark.Enabled {
		wmCfg, err := watermark.BuildWatermarkConfig(cfg.Pipeline.Transform.Watermark)
		if err != nil {
			return fmt.Errorf("parsing watermark config: %w", err)
		}
		watermark.ApplyWatermarkConfig(rootNode, wmCfg)
		fmt.Printf("Applied watermark enabled=%v policy=%v\n", wmCfg.Enabled, wmCfg.Policy)
	}

	if cfg.Pipeline.Transform.JoinTTL != "" {
		ttl, err := pipeline.ParseJoinTTL(cfg.Pipeline.Transform.JoinTTL)
		if err != nil {
			return fmt.Errorf("parsing join_ttl: %w", err)
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
		snk, err = sink.NewConsoleSink(sinkCfg)
	case "file":
		snk, err = sink.NewFileSink(sinkCfg)
	case "parquet":
		snk, err = sink.NewParquetSink(sinkCfg, parquetSchema)
	case "http_pull":
		snk, err = sink.NewHTTPPullSink(sinkCfg, rootNode.PartitionBy, parquetSchema)
	default:
		err = fmt.Errorf("unsupported sink type: %s", cfg.Pipeline.Sink.Type)
	}
	if err != nil {
		return fmt.Errorf("initializing sink: %w", err)
	}
	snk, err = sink.WrapSinkWithBatchingIfConfigured(sinkCfg, snk)
	if err != nil {
		return fmt.Errorf("initializing sink batching: %w", err)
	}
	defer snk.Close()

	// 4.5 Initialize WAL (optional)
	var writeAheadLog *wal.SQLiteWAL
	if cfg.Pipeline.WAL.Enabled {
		walPath := cfg.Pipeline.WAL.Path
		if cfg.Pipeline.Partition.Enabled && len(partitionValues) > 0 && strings.TrimSpace(walPath) != "" {
			walPath = config.BuildHivePartitionPath(walPath, cfg.Pipeline.Partition.Keys, partitionValues)
		}
		writeAheadLog, err = wal.NewSQLiteWAL(walPath)
		if err != nil {
			return fmt.Errorf("initializing WAL: %w", err)
		}
		defer writeAheadLog.Close()
		fmt.Printf("WAL enabled: sqlite=%s\n", walPath)
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
		return err
	}
	fmt.Println("Pipeline finished.")
	return nil
}

func newSource(cfg *config.PipelineConfig) (provider.Source, error) {
	switch cfg.Pipeline.Source.Type {
	case "csv":
		return source.NewCSVSource(cfg.Pipeline.Source.Config)
	case "http":
		return source.NewHTTPSource(cfg.Pipeline.Source.Config)
	case "chain":
		return source.NewChainSource(cfg.Pipeline.Source.Config)
	default:
		return nil, fmt.Errorf("unsupported source type: %s", cfg.Pipeline.Source.Type)
	}
}

func buildPartitionRuntime(cfg *config.PipelineConfig, partitionValues map[string]string) (*partitionRuntime, error) {
	if cfg.Pipeline.Transform.Type != "sql" {
		return nil, fmt.Errorf("unsupported transform type: %s", cfg.Pipeline.Transform.Type)
	}
	query := strings.TrimSpace(cfg.Pipeline.Transform.Query)
	rootNode, err := sqlconv.ParseQueryToIncrementalDBSP(query)
	if err != nil {
		return nil, fmt.Errorf("compiling SQL query: %w", err)
	}

	if cfg.Pipeline.Transform.Watermark.Enabled {
		wmCfg, err := watermark.BuildWatermarkConfig(cfg.Pipeline.Transform.Watermark)
		if err != nil {
			return nil, fmt.Errorf("parsing watermark config: %w", err)
		}
		watermark.ApplyWatermarkConfig(rootNode, wmCfg)
	}
	if cfg.Pipeline.Transform.JoinTTL != "" {
		ttl, err := pipeline.ParseJoinTTL(cfg.Pipeline.Transform.JoinTTL)
		if err != nil {
			return nil, fmt.Errorf("parsing join_ttl: %w", err)
		}
		if ttl > 0 {
			pipeline.ApplyJoinTTL(rootNode, ttl)
		}
	}

	sinkCfg := cloneConfigMap(cfg.Pipeline.Sink.Config)
	applyHivePathToSink(cfg.Pipeline.Sink.Type, sinkCfg, cfg.Pipeline.Partition.Keys, partitionValues)

	var parquetSchema *config.ParquetSchema
	if cfg.Pipeline.Sink.Type == "parquet" || cfg.Pipeline.Sink.Type == "http_pull" {
		parquetSchema, err = config.InferOrLoadParquetSchema(query, cfg.Pipeline.Source, sinkCfg)
		if err != nil {
			return nil, fmt.Errorf("inferring parquet schema: %w", err)
		}
	}

	var snk provider.Sink
	switch cfg.Pipeline.Sink.Type {
	case "console":
		snk, err = sink.NewConsoleSink(sinkCfg)
	case "file":
		snk, err = sink.NewFileSink(sinkCfg)
	case "parquet":
		snk, err = sink.NewParquetSink(sinkCfg, parquetSchema)
	case "http_pull":
		snk, err = sink.NewHTTPPullSink(sinkCfg, rootNode.PartitionBy, parquetSchema)
	default:
		err = fmt.Errorf("unsupported sink type: %s", cfg.Pipeline.Sink.Type)
	}
	if err != nil {
		return nil, fmt.Errorf("initializing sink: %w", err)
	}
	snk, err = sink.WrapSinkWithBatchingIfConfigured(sinkCfg, snk)
	if err != nil {
		_ = snk.Close()
		return nil, fmt.Errorf("initializing sink batching: %w", err)
	}

	var writeAheadLog *wal.SQLiteWAL
	if cfg.Pipeline.WAL.Enabled {
		walPath := cfg.Pipeline.WAL.Path
		if strings.TrimSpace(walPath) != "" {
			walPath = config.BuildHivePartitionPath(walPath, cfg.Pipeline.Partition.Keys, partitionValues)
		}
		writeAheadLog, err = wal.NewSQLiteWAL(walPath)
		if err != nil {
			_ = snk.Close()
			return nil, fmt.Errorf("initializing WAL: %w", err)
		}
	}

	rt := &partitionRuntime{
		values:   partitionValues,
		rootNode: rootNode,
		sink:     snk,
		wal:      writeAheadLog,
		snapshotter: pipeline.PipelineSnapshotterFunc{
			SnapFunc:    func() ([]byte, error) { return op.SnapshotGraph(rootNode) },
			RestoreFunc: func(b []byte) error { return op.RestoreGraph(rootNode, b) },
		},
	}
	if err := replayRuntime(context.Background(), rt); err != nil {
		_ = rt.sink.Close()
		if rt.wal != nil {
			_ = rt.wal.Close()
		}
		return nil, err
	}

	return rt, nil
}

func runPartitionBatch(ctx context.Context, cfg *config.PipelineConfig, rt *partitionRuntime, batch types.Batch) error {
	rt.batchCount++
	if rt.wal != nil {
		if err := rt.wal.Append(ctx, batch); err != nil {
			return err
		}
	}

	if rt.wal != nil && rt.snapshotter != nil && cfg.Pipeline.WAL.CheckpointEveryBatches > 0 && (rt.batchCount%cfg.Pipeline.WAL.CheckpointEveryBatches) == 0 {
		if cwal, ok := any(rt.wal).(pipeline.CheckpointWAL); ok {
			snap, err := rt.snapshotter.Snapshot()
			if err != nil {
				return err
			}
			maxSeq, err := cwal.MaxSeq(ctx)
			if err != nil {
				return err
			}
			if err := cwal.SaveCheckpoint(ctx, walpkg.Checkpoint{LastSeq: maxSeq, Snapshot: snap}); err != nil {
				return err
			}
		}
	}

	resultBatch, err := op.Execute(rt.rootNode, batch)
	if err != nil {
		return err
	}
	return rt.sink.WriteBatch(resultBatch)
}

func replayRuntime(ctx context.Context, rt *partitionRuntime) error {
	if rt.wal == nil {
		return nil
	}
	if cwal, ok := any(rt.wal).(pipeline.CheckpointWAL); ok && rt.snapshotter != nil {
		cp, err := cwal.LoadLatestCheckpoint(ctx)
		if err != nil {
			return err
		}
		afterSeq := int64(0)
		if cp != nil && len(cp.Snapshot) > 0 {
			if err := rt.snapshotter.Restore(cp.Snapshot); err != nil {
				return err
			}
			afterSeq = cp.LastSeq
		}
		return cwal.ReplayFrom(ctx, afterSeq, func(b types.Batch) error {
			_, err := op.Execute(rt.rootNode, b)
			return err
		})
	}
	return rt.wal.Replay(ctx, func(b types.Batch) error {
		_, err := op.Execute(rt.rootNode, b)
		return err
	})
}

func extractPartitionValues(tuple types.Tuple, keys []string) (map[string]string, bool) {
	vals := make(map[string]string, len(keys))
	for _, key := range keys {
		v, ok := tuple[key]
		if !ok || v == nil {
			return nil, false
		}
		vals[key] = fmt.Sprintf("%v", v)
	}
	return vals, true
}

func makePartitionKey(keys []string, values map[string]string) string {
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		parts = append(parts, key+"="+values[key])
	}
	return strings.Join(parts, "|")
}

func closePartitionRuntimes(runtimes map[string]*partitionRuntime) error {
	var errs []error
	for _, rt := range runtimes {
		if rt.sink != nil {
			if err := rt.sink.Close(); err != nil {
				errs = append(errs, err)
			}
		}
		if rt.wal != nil {
			if err := rt.wal.Close(); err != nil {
				errs = append(errs, err)
			}
		}
	}
	return errors.Join(errs...)
}

func cloneConfigMap(in map[string]interface{}) map[string]interface{} {
	if in == nil {
		return map[string]interface{}{}
	}
	b, err := yaml.Marshal(in)
	if err != nil {
		out := make(map[string]interface{}, len(in))
		for k, v := range in {
			out[k] = v
		}
		return out
	}
	var out map[string]interface{}
	if err := yaml.Unmarshal(b, &out); err != nil {
		out = make(map[string]interface{}, len(in))
		for k, v := range in {
			out[k] = v
		}
	}
	return out
}

func applyHivePathToSink(sinkType string, sinkCfg map[string]interface{}, keys []string, values map[string]string) {
	switch sinkType {
	case "file", "parquet":
		path, _ := sinkCfg["path"].(string)
		if strings.TrimSpace(path) == "" {
			return
		}
		sinkCfg["path"] = config.BuildHivePartitionPath(path, keys, values)
	case "http_pull":
		diskSpillPath, _ := sinkCfg["disk_spill_path"].(string)
		if strings.TrimSpace(diskSpillPath) == "" {
			return
		}
		sinkCfg["disk_spill_path"] = config.BuildHivePartitionPath(diskSpillPath, keys, values)
	}
}
