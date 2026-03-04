package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/http"
	_ "net/http/pprof"
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
	"github.com/ariyn/dbsp/cmd/dbsp/watermark"
	"github.com/ariyn/dbsp/internal/dbsp/op"
	sqlconv "github.com/ariyn/dbsp/internal/dbsp/sql"
	"github.com/ariyn/dbsp/internal/dbsp/types"
	"github.com/ariyn/dbsp/internal/dbsp/wal"
	walpkg "github.com/ariyn/dbsp/internal/dbsp/wal"
)

type partitionRuntime struct {
	values      map[string]string
	rootNode    *op.Node
	sink        provider.Sink
	wal         *wal.SQLiteWAL
	state       op.StateBackend
	snapshotter pipeline.PipelineSnapshotter
	checkpoint  pipeline.CheckpointState
}

var compileIncrementalQuery = sqlconv.ParseQueryToIncrementalDBSP

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	configPath := flag.String("config", "config.yaml", "Path to configuration file")
	pprofAddr := flag.String("pprof-addr", "", "Start pprof HTTP server at this address (e.g. 127.0.0.1:6060)")
	flag.Parse()

	if strings.TrimSpace(*pprofAddr) != "" {
		go func() {
			fmt.Printf("pprof enabled: http://%s/debug/pprof/\n", *pprofAddr)
			if err := http.ListenAndServe(*pprofAddr, nil); err != nil {
				fmt.Printf("pprof server error: %v\n", err)
			}
		}()
	}

	// 1. Load Config
	configFile, err := os.ReadFile(*configPath)
	if err != nil {
		fmt.Printf("Error reading config file: %v\n", err)
		os.Exit(1)
	}

	cfg, err := config.ParsePipelineConfig(configFile)
	if err != nil {
		fmt.Printf("Error parsing config file: %v\n", err)
		os.Exit(1)
	}
	if err := config.ValidatePartitionConfig(cfg.Pipeline.Partition, cfg.Pipeline.Transform.Query); err != nil {
		fmt.Printf("Invalid partition config: %v\n", err)
		os.Exit(1)
	}

	if err := config.ApplyMemoryLimit(cfg.Pipeline.State.MemoryLimit); err != nil {
		fmt.Printf("Error applying memory limit: %v\n", err)
		os.Exit(1)
	}
	if cfg.Pipeline.Partition.Enabled && config.QueryContainsPartitionPredicate(cfg.Pipeline.Transform.Query, cfg.Pipeline.Partition.Keys) {
		fmt.Printf("Warning: transform.query contains partition predicate; runtime partition demux works best with unfiltered transform.query\n")
	}

	if cfg.Pipeline.Partition.Enabled {
		if err := preflightPartitionQueryBuild(&cfg); err != nil {
			fmt.Printf("Partition startup preflight failed: %v\n", err)
			os.Exit(1)
		}
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

func preflightPartitionQueryBuild(cfg *config.PipelineConfig) error {
	if cfg == nil {
		return fmt.Errorf("pipeline config is nil")
	}
	if !cfg.Pipeline.Partition.Enabled {
		return nil
	}
	if cfg.Pipeline.Transform.Type != "sql" {
		return fmt.Errorf("unsupported transform type: %s", cfg.Pipeline.Transform.Type)
	}
	query := strings.TrimSpace(cfg.Pipeline.Transform.Query)
	if query == "" {
		return fmt.Errorf("transform query is empty")
	}

	if _, err := compileIncrementalQuery(query); err != nil {
		return fmt.Errorf("compiling SQL query during startup preflight: %w", err)
	}

	return nil
}

func runPartitionFanout(ctx context.Context, cfg *config.PipelineConfig) error {
	partCfg := cfg.Pipeline.Partition
	fmt.Printf("Partition runtime demux enabled: keys=%v\n", partCfg.Keys)

	if cfg.Pipeline.Source.Type == "http" {
		var httpSourceCfg config.HTTPSourceConfig
		if err := config.DecodeTo(cfg.Pipeline.Source.Config, &httpSourceCfg); err == nil {
			if len(httpSourceCfg.Schema) == 0 {
				fmt.Printf("Warning: Source type 'http' used with partitioning but no schema is defined. Records might be dropped if keys cannot be extracted from raw JSON.\n")
			}
		}
	}

	src, err := newSource(cfg)
	if err != nil {
		return fmt.Errorf("initializing source: %w", err)
	}
	defer src.Close()

	runtimes := map[string]*partitionRuntime{}
	defer closePartitionRuntimes(runtimes)
	var sharedPartitionSink provider.Sink

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
				if droppedRecords%100 == 1 {
					fmt.Printf("Warning: Dropping record %d due to missing partition keys: tuple=%v keys=%v\n", droppedRecords, td.Tuple, partCfg.Keys)
				}
				continue
			}
			key := makePartitionKey(partCfg.Keys, partitionValues)
			if _, exists := runtimes[key]; !exists {
				rt, sharedSink, err := buildPartitionRuntime(cfg, partitionValues, sharedPartitionSink)
				if err != nil {
					return fmt.Errorf("initializing runtime for partition %s: %w", config.PartitionSummary(partitionValues, partCfg.Keys), err)
				}
				if cfg.Pipeline.Sink.Type == "http_pull" && sharedPartitionSink == nil && sharedSink != nil {
					sharedPartitionSink = sharedSink
					defer sharedPartitionSink.Close()
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
	query, rootNode, ttl, err := compileTransform(cfg, true)
	if err != nil {
		return err
	}

	src, err := newSource(cfg)
	if err != nil {
		return fmt.Errorf("initializing source: %w", err)
	}
	defer src.Close()

	fmt.Printf("Compiling Query: %s\n", query)

	sinkCfg := buildSinkConfig(cfg, partitionValues, false)

	// If Parquet/HTTPPull sink is selected, infer/load and cache output schema at SQL-analysis time.
	parquetSchema, err := inferParquetSchemaIfNeeded(cfg, query, sinkCfg, nil)
	if err != nil {
		return fmt.Errorf("inferring parquet schema: %w", err)
	}
	if cfg.Pipeline.Sink.Type == "http_pull" && !cfg.Pipeline.Partition.Enabled {
		if err := validatePartitionColumns(parquetSchema, rootNode.PartitionBy); err != nil {
			return err
		}
	}

	// 4. Initialize Sink
	snk, _, err := createSink(cfg, sinkCfg, rootNode, parquetSchema, nil)
	if err != nil {
		return err
	}
	defer snk.Close()

	// 4.5 Initialize WAL (optional)
	writeAheadLog, walPath, err := buildWAL(cfg, ttl, partitionValues, false, true)
	if err != nil {
		return err
	}
	if writeAheadLog != nil {
		defer writeAheadLog.Close()
		fmt.Printf("WAL enabled: sqlite=%s\n", walPath)
	}

	stateBackend, mutationTracker, attachCounts, err := buildStateBackend(cfg, rootNode, partitionValues, false)
	if err != nil {
		return err
	}
	if stateBackend != nil {
		defer stateBackend.Close()
		fmt.Printf("State backend enabled: type=%s path=%s join_ops=%d groupagg_ops=%d windowagg_ops=%d\n", cfg.Pipeline.State.Type, attachCounts.path, attachCounts.joinOps, attachCounts.groupOps, attachCounts.windowOps)
	}

	// 5. Run Pipeline
	fmt.Println("Starting pipeline...")
	snapshotter := pipeline.NewPipelineSnapshotter(cfg,
		func() ([]byte, error) { return op.SnapshotGraph(rootNode) },
		func(b []byte) error { return op.RestoreGraph(rootNode, b) },
		func() []walpkg.CheckpointMutation {
			if mutationTracker == nil {
				return nil
			}
			drained := mutationTracker.DrainMutations()
			mutations := checkpointMutationsFromStateOps(drained)
			if len(mutations) > 0 {
				fmt.Printf("State mutation changelog drained for checkpoint: ops=%d\n", len(mutations))
			}
			return mutations
		},
		func(mutations []walpkg.CheckpointMutation) error {
			return applyCheckpointMutationsToStateBackend(stateBackend, mutations)
		},
		func(mutations []walpkg.CheckpointMutation) {
			restoreCheckpointMutationsToTracker(mutationTracker, mutations)
		},
	)
	err = pipeline.RunPipeline(ctx, src, snk, func(batch types.Batch) (types.Batch, error) {
		return op.Execute(rootNode, batch)
	}, writeAheadLog, snapshotter, cfg.Pipeline.WAL.CheckpointEveryBatches)
	if err != nil {
		return err
	}
	fmt.Println("Pipeline finished.")
	return nil
}

func newSource(cfg *config.PipelineConfig) (provider.Source, error) {
	switch cfg.Pipeline.Source.Type {
	case "http":
		var httpCfg config.HTTPSourceConfig
		if err := config.DecodeTo(cfg.Pipeline.Source.Config, &httpCfg); err != nil {
			return nil, fmt.Errorf("failed to decode http source config: %w", err)
		}
		return source.NewHTTPSource(httpCfg)
	case "chain":
		var chainCfg config.ChainSourceConfig
		if err := config.DecodeTo(cfg.Pipeline.Source.Config, &chainCfg); err != nil {
			return nil, fmt.Errorf("failed to decode chain source config: %w", err)
		}
		return source.NewChainSource(chainCfg)
	default:
		return nil, fmt.Errorf("unsupported source type: %s", cfg.Pipeline.Source.Type)
	}
}

type stateAttachStats struct {
	path      string
	joinOps   int
	groupOps  int
	windowOps int
}

func compileTransform(cfg *config.PipelineConfig, logWatermark bool) (string, *op.Node, time.Duration, error) {
	if cfg.Pipeline.Transform.Type != "sql" {
		return "", nil, 0, fmt.Errorf("unsupported transform type: %s", cfg.Pipeline.Transform.Type)
	}
	query := strings.TrimSpace(cfg.Pipeline.Transform.Query)
	if query == "" {
		return "", nil, 0, fmt.Errorf("transform query is empty")
	}

	rootNode, err := compileIncrementalQuery(query)
	if err != nil {
		return "", nil, 0, fmt.Errorf("compiling SQL query: %w", err)
	}

	if cfg.Pipeline.Transform.Watermark.Enabled {
		wmCfg, err := watermark.BuildWatermarkConfig(cfg.Pipeline.Transform.Watermark)
		if err != nil {
			return "", nil, 0, fmt.Errorf("parsing watermark config: %w", err)
		}
		watermark.ApplyWatermarkConfig(rootNode, wmCfg)
		if logWatermark {
			fmt.Printf("Applied watermark enabled=%v policy=%v\n", wmCfg.Enabled, wmCfg.Policy)
		}
	}

	ttl, err := validateTransformTTL(cfg.Pipeline.Transform.TTL, cfg.Pipeline.WAL.Enabled)
	if err != nil {
		return "", nil, 0, err
	}

	return query, rootNode, ttl, nil
}

func buildSinkConfig(cfg *config.PipelineConfig, partitionValues map[string]string, partitionRuntime bool) map[string]interface{} {
	sinkCfg := types.CloneConfigMap(cfg.Pipeline.Sink.Config)
	if partitionRuntime {
		if cfg.Pipeline.Sink.Type != "http_pull" {
			applyHivePathToSink(cfg.Pipeline.Sink.Type, sinkCfg, cfg.Pipeline.Partition.Keys, partitionValues)
		}
		return sinkCfg
	}
	if cfg.Pipeline.Partition.Enabled && len(partitionValues) > 0 {
		applyHivePathToSink(cfg.Pipeline.Sink.Type, sinkCfg, cfg.Pipeline.Partition.Keys, partitionValues)
	}
	return sinkCfg
}

func inferParquetSchemaIfNeeded(cfg *config.PipelineConfig, query string, sinkCfg map[string]interface{}, sharedSink provider.Sink) (*config.ParquetSchema, error) {
	if cfg.Pipeline.Sink.Type != "parquet" && cfg.Pipeline.Sink.Type != "http_pull" {
		return nil, nil
	}
	if sharedSink != nil {
		return nil, nil
	}
	return config.InferOrLoadParquetSchema(query, cfg.Pipeline.Source, sinkCfg)
}

func createSink(cfg *config.PipelineConfig, sinkCfg map[string]interface{}, rootNode *op.Node, parquetSchema *config.ParquetSchema, sharedSink provider.Sink) (provider.Sink, provider.Sink, error) {
	var snk provider.Sink
	var sharedHTTPPull provider.Sink
	var err error

	if cfg.Pipeline.Sink.Type == "http_pull" {
		if sharedSink != nil {
			snk = sink.NewNoopCloseSink(sharedSink)
		} else {
			partitionBy := rootNode.PartitionBy
			if cfg.Pipeline.Partition.Enabled {
				partitionBy = cfg.Pipeline.Partition.Keys
			}
			var httpPullCfg config.HTTPPullSinkConfig
			if err := config.DecodeTo(sinkCfg, &httpPullCfg); err != nil {
				return nil, nil, fmt.Errorf("failed to decode http_pull sink config: %w", err)
			}
			snk, err = sink.NewHTTPPullSink(httpPullCfg, partitionBy, parquetSchema)
			if err != nil {
				return nil, nil, fmt.Errorf("initializing sink: %w", err)
			}
			sharedHTTPPull = snk
			snk = sink.NewNoopCloseSink(snk)
		}
	} else if sharedSink != nil {
		snk = sharedSink
	} else {
		switch cfg.Pipeline.Sink.Type {
		case "console":
			var consoleCfg config.ConsoleSinkConfig
			if err := config.DecodeTo(sinkCfg, &consoleCfg); err != nil {
				return nil, nil, fmt.Errorf("failed to decode console sink config: %w", err)
			}
			snk, err = sink.NewConsoleSink(consoleCfg)
		case "file":
			var fileCfg config.FileSinkConfig
			if err := config.DecodeTo(sinkCfg, &fileCfg); err != nil {
				return nil, nil, fmt.Errorf("failed to decode file sink config: %w", err)
			}
			snk, err = sink.NewFileSink(fileCfg)
		case "parquet":
			var parquetSinkCfg config.ParquetSinkConfig
			if err := config.DecodeTo(sinkCfg, &parquetSinkCfg); err != nil {
				return nil, nil, fmt.Errorf("failed to decode parquet sink config: %w", err)
			}
			snk, err = sink.NewParquetSink(parquetSinkCfg, parquetSchema)
		default:
			err = fmt.Errorf("unsupported sink type: %s", cfg.Pipeline.Sink.Type)
		}
		if err != nil {
			return nil, nil, fmt.Errorf("initializing sink: %w", err)
		}
	}

	snk, err = sink.WrapSinkWithBatchingIfConfigured(sinkCfg, snk)
	if err != nil {
		_ = snk.Close()
		return nil, nil, fmt.Errorf("initializing sink batching: %w", err)
	}

	return snk, sharedHTTPPull, nil
}

func buildWAL(cfg *config.PipelineConfig, ttl time.Duration, partitionValues map[string]string, partitionRuntime bool, logTTL bool) (*wal.SQLiteWAL, string, error) {
	if !cfg.Pipeline.WAL.Enabled {
		return nil, "", nil
	}

	walPath := cfg.Pipeline.WAL.Path
	applyPartitionPath := partitionRuntime || (cfg.Pipeline.Partition.Enabled && len(partitionValues) > 0)
	if applyPartitionPath && strings.TrimSpace(walPath) != "" {
		walPath = config.BuildHivePartitionPath(walPath, cfg.Pipeline.Partition.Keys, partitionValues)
	}
	writeAheadLog, err := wal.NewSQLiteWALWithConfig(walPath, buildSQLiteWALConfig(cfg.Pipeline.WAL))
	if err != nil {
		return nil, "", fmt.Errorf("initializing WAL: %w", err)
	}
	if ttl > 0 {
		writeAheadLog.SetRetentionTTL(ttl)
		if logTTL {
			fmt.Printf("Applied ttl=%s (WAL retention)\n", ttl)
		}
	}
	return writeAheadLog, walPath, nil
}

func buildStateBackend(cfg *config.PipelineConfig, rootNode *op.Node, partitionValues map[string]string, partitionRuntime bool) (op.StateBackend, op.StateMutationTracker, stateAttachStats, error) {
	if !cfg.Pipeline.State.Enabled {
		return nil, nil, stateAttachStats{}, nil
	}

	statePath := cfg.Pipeline.State.Path
	applyPartitionPath := partitionRuntime || (cfg.Pipeline.Partition.Enabled && len(partitionValues) > 0)
	if applyPartitionPath && strings.TrimSpace(statePath) != "" {
		statePath = config.BuildHivePartitionPath(statePath, cfg.Pipeline.Partition.Keys, partitionValues)
	}
	baseStateBackend, err := op.NewStateBackendFromConfig(true, cfg.Pipeline.State.Type, statePath)
	if err != nil {
		return nil, nil, stateAttachStats{}, fmt.Errorf("initializing state backend: %w", err)
	}
	tracked := op.NewMutationTrackingStateBackend(baseStateBackend)
	stateBackend := tracked
	mutationTracker := tracked

	stats := stateAttachStats{
		path:      statePath,
		joinOps:   op.AttachJoinStateBackend(rootNode, stateBackend),
		groupOps:  op.AttachGroupAggStateBackend(rootNode, stateBackend),
		windowOps: op.AttachWindowAggStateBackend(rootNode, stateBackend),
	}
	return stateBackend, mutationTracker, stats, nil
}

func buildPartitionRuntime(cfg *config.PipelineConfig, partitionValues map[string]string, sharedSink provider.Sink) (*partitionRuntime, provider.Sink, error) {
	query, rootNode, ttl, err := compileTransform(cfg, false)
	if err != nil {
		return nil, nil, err
	}

	sinkCfg := buildSinkConfig(cfg, partitionValues, true)
	parquetSchema, err := inferParquetSchemaIfNeeded(cfg, query, sinkCfg, sharedSink)
	if err != nil {
		return nil, nil, fmt.Errorf("inferring parquet schema: %w", err)
	}

	snk, sharedHTTPPull, err := createSink(cfg, sinkCfg, rootNode, parquetSchema, sharedSink)
	if err != nil {
		return nil, nil, err
	}

	writeAheadLog, _, err := buildWAL(cfg, ttl, partitionValues, true, false)
	if err != nil {
		_ = snk.Close()
		return nil, nil, err
	}

	stateBackend, mutationTracker, _, err := buildStateBackend(cfg, rootNode, partitionValues, true)
	if err != nil {
		if writeAheadLog != nil {
			_ = writeAheadLog.Close()
		}
		_ = snk.Close()
		return nil, nil, err
	}

	rt := &partitionRuntime{
		values:   partitionValues,
		rootNode: rootNode,
		sink:     snk,
		wal:      writeAheadLog,
		state:    stateBackend,
		snapshotter: pipeline.NewPipelineSnapshotter(cfg,
			func() ([]byte, error) { return op.SnapshotGraph(rootNode) },
			func(b []byte) error { return op.RestoreGraph(rootNode, b) },
			func() []walpkg.CheckpointMutation {
				if mutationTracker == nil {
					return nil
				}
				drained := mutationTracker.DrainMutations()
				mutations := checkpointMutationsFromStateOps(drained)
				if len(mutations) > 0 {
					fmt.Printf("State mutation changelog drained for checkpoint: ops=%d\n", len(mutations))
				}
				return mutations
			},
			func(mutations []walpkg.CheckpointMutation) error {
				return applyCheckpointMutationsToStateBackend(stateBackend, mutations)
			},
			func(mutations []walpkg.CheckpointMutation) {
				restoreCheckpointMutationsToTracker(mutationTracker, mutations)
			},
		),
	}
	replayWrite := func(batch types.Batch) error {
		if len(batch) == 0 {
			return nil
		}
		if ps, ok := rt.sink.(sink.PartitionedSink); ok {
			return ps.WriteBatchWithPartition(batch, rt.values)
		}
		if rs, ok := rt.sink.(provider.ReplaySink); ok {
			return rs.ReplayWriteBatch(batch)
		}
		return nil
	}
	if err := pipeline.ReplayWithCheckpoint(context.Background(), rt.wal, rt.snapshotter, func(b types.Batch) error {
		result, err := op.Execute(rt.rootNode, b)
		if err != nil {
			return err
		}
		return replayWrite(result)
	}); err != nil {
		_ = rt.sink.Close()
		if rt.wal != nil {
			_ = rt.wal.Close()
		}
		return nil, nil, err
	}
	checkpointState, err := pipeline.NewCheckpointState(context.Background(), rt.wal)
	if err != nil {
		_ = rt.sink.Close()
		if rt.wal != nil {
			_ = rt.wal.Close()
		}
		return nil, nil, err
	}
	rt.checkpoint = checkpointState

	return rt, sharedHTTPPull, nil
}

func buildSQLiteWALConfig(cfg config.WALConfig) wal.SQLiteWALConfig {
	pragmas := cfg.SQLitePragmas
	return wal.SQLiteWALConfig{
		TempStore:     pragmas.TempStore,
		CacheSize:     pragmas.CacheSize,
		MmapSize:      pragmas.MmapSize,
		BusyTimeoutMS: pragmas.BusyTimeoutMS,
		ExtraPragmas:  pragmas.ExtraPragmas,
	}
}

// These are used by tests in main_memory_test.go and main_ttl_test.go
func parseHumanBytes(raw string) (int64, error) {
	return config.ParseHumanBytes(raw)
}

func parsePipelineConfig(configFile []byte) (config.PipelineConfig, error) {
	return config.ParsePipelineConfig(configFile)
}

func validateTransformTTL(ttl string, walEnabled bool) (time.Duration, error) {
	if strings.TrimSpace(ttl) == "" {
		return 0, nil
	}
	parsed, err := pipeline.ParseTTL(ttl)
	if err != nil {
		return 0, fmt.Errorf("parsing ttl: %w", err)
	}
	if parsed <= 0 {
		return 0, fmt.Errorf("ttl must be greater than 0")
	}
	if !walEnabled {
		return 0, fmt.Errorf("transform.ttl requires pipeline.wal.enabled=true (ttl currently applies to WAL retention)")
	}
	return parsed, nil
}

func validatePartitionColumns(schema *config.ParquetSchema, partitionBy []string) error {
	if len(partitionBy) == 0 || schema == nil {
		return nil
	}
	available := make(map[string]struct{}, len(schema.Columns))
	colNames := make([]string, 0, len(schema.Columns))
	for _, col := range schema.Columns {
		available[col.Name] = struct{}{}
		colNames = append(colNames, col.Name)
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

	return fmt.Errorf("http_pull partition keys must be present in the query output; missing=%s output_columns=%s", strings.Join(missing, ","), strings.Join(colNames, ","))
}

func runPartitionBatch(ctx context.Context, cfg *config.PipelineConfig, rt *partitionRuntime, batch types.Batch) error {
	writeBatch := rt.sink.WriteBatch
	if cfg.Pipeline.Sink.Type == "http_pull" {
		if ps, ok := rt.sink.(sink.PartitionedSink); ok {
			writeBatch = func(result types.Batch) error {
				return ps.WriteBatchWithPartition(result, rt.values)
			}
		}
	}
	return pipeline.RunBatchWithCheckpoint(ctx, batch, func(input types.Batch) (types.Batch, error) {
		return op.Execute(rt.rootNode, input)
	}, rt.wal, rt.snapshotter, cfg.Pipeline.WAL.CheckpointEveryBatches, &rt.checkpoint, writeBatch)
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
	return config.PartitionSummary(values, keys)
}

func closePartitionRuntimes(runtimes map[string]*partitionRuntime) error {
	var errs []error
	closedSinks := map[provider.Sink]struct{}{}
	for _, rt := range runtimes {
		if rt.sink != nil {
			if _, exists := closedSinks[rt.sink]; !exists {
				closedSinks[rt.sink] = struct{}{}
				if err := rt.sink.Close(); err != nil {
					errs = append(errs, err)
				}
			}
		}
		if rt.wal != nil {
			if err := rt.wal.Close(); err != nil {
				errs = append(errs, err)
			}
		}
		if rt.state != nil {
			if err := rt.state.Close(); err != nil {
				errs = append(errs, err)
			}
		}
	}
	return errors.Join(errs...)
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

func checkpointMutationsFromStateOps(ops []op.StateBatchOp) []walpkg.CheckpointMutation {
	if len(ops) == 0 {
		return nil
	}
	out := make([]walpkg.CheckpointMutation, 0, len(ops))
	for _, stateOp := range ops {
		out = append(out, walpkg.CheckpointMutation{
			Type:  stateOp.MutationType(),
			Key:   append([]byte(nil), stateOp.Key...),
			Value: append([]byte(nil), stateOp.Value...),
		})
	}
	return out
}

func applyCheckpointMutationsToStateBackend(backend op.StateBackend, mutations []walpkg.CheckpointMutation) error {
	if backend == nil || len(mutations) == 0 {
		return nil
	}
	ops := make([]op.StateBatchOp, 0, len(mutations))
	for _, mutation := range mutations {
		ops = append(ops, op.StateBatchOpFromMutation(mutation.Type, mutation.Key, mutation.Value))
	}
	return backend.BatchWrite(ops)
}

func restoreCheckpointMutationsToTracker(tracker op.StateMutationTracker, mutations []walpkg.CheckpointMutation) {
	if tracker == nil || len(mutations) == 0 {
		return
	}
	ops := make([]op.StateBatchOp, 0, len(mutations))
	for _, mutation := range mutations {
		ops = append(ops, op.StateBatchOpFromMutation(mutation.Type, mutation.Key, mutation.Value))
	}
	tracker.RestoreMutations(ops)
}
