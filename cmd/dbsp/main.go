package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
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
	"gopkg.in/yaml.v3"
)

type partitionRuntime struct {
	values      map[string]string
	rootNode    *op.Node
	sink        provider.Sink
	wal         *wal.SQLiteWAL
	state       op.StateBackend
	snapshotter pipeline.PipelineSnapshotter
	batchCount  int
}

var compileIncrementalQuery = sqlconv.ParseQueryToIncrementalDBSP

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

	cfg, err := parsePipelineConfig(configFile)
	if err != nil {
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

	rootNode, err := compileIncrementalQuery(query)
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
	if cfg.Pipeline.Sink.Type == "http_pull" && !cfg.Pipeline.Partition.Enabled {
		if err := validatePartitionColumns(parquetSchema, rootNode.PartitionBy); err != nil {
			return err
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

	ttl, err := validateTransformTTL(cfg.Pipeline.Transform.TTL, cfg.Pipeline.WAL.Enabled)
	if err != nil {
		return err
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
		writeAheadLog, err = wal.NewSQLiteWALWithConfig(walPath, buildSQLiteWALConfig(cfg.Pipeline.WAL))
		if err != nil {
			return fmt.Errorf("initializing WAL: %w", err)
		}
		if ttl > 0 {
			writeAheadLog.SetRetentionTTL(ttl)
			fmt.Printf("Applied ttl=%s (WAL retention)\n", ttl)
		}
		defer writeAheadLog.Close()
		fmt.Printf("WAL enabled: sqlite=%s\n", walPath)
	}

	var stateBackend op.StateBackend
	var mutationTracker op.StateMutationTracker
	if cfg.Pipeline.State.Enabled {
		statePath := cfg.Pipeline.State.Path
		if cfg.Pipeline.Partition.Enabled && len(partitionValues) > 0 && strings.TrimSpace(statePath) != "" {
			statePath = config.BuildHivePartitionPath(statePath, cfg.Pipeline.Partition.Keys, partitionValues)
		}
		baseStateBackend, stateErr := op.NewStateBackendFromConfig(true, cfg.Pipeline.State.Type, statePath)
		if stateErr != nil {
			err = stateErr
		} else {
			tracked := op.NewMutationTrackingStateBackend(baseStateBackend)
			stateBackend = tracked
			mutationTracker = tracked
		}
		if err != nil {
			return fmt.Errorf("initializing state backend: %w", err)
		}
		joinAttached := op.AttachJoinStateBackend(rootNode, stateBackend)
		groupAttached := op.AttachGroupAggStateBackend(rootNode, stateBackend)
		windowAttached := op.AttachWindowAggStateBackend(rootNode, stateBackend)
		defer stateBackend.Close()
		fmt.Printf("State backend enabled: type=%s path=%s join_ops=%d groupagg_ops=%d windowagg_ops=%d\n", cfg.Pipeline.State.Type, statePath, joinAttached, groupAttached, windowAttached)
	}

	// 5. Run Pipeline
	fmt.Println("Starting pipeline...")
	err = pipeline.RunPipeline(ctx, src, snk, func(batch types.Batch) (types.Batch, error) {
		return op.Execute(rootNode, batch)
	}, writeAheadLog,
		pipeline.PipelineSnapshotterFunc{
			SnapFunc:    func() ([]byte, error) { return op.SnapshotGraph(rootNode) },
			RestoreFunc: func(b []byte) error { return op.RestoreGraph(rootNode, b) },
			DrainCheckpointMutationsFunc: func() []walpkg.CheckpointMutation {
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
			ApplyCheckpointMutationsFunc: func(mutations []walpkg.CheckpointMutation) error {
				return applyCheckpointMutationsToStateBackend(stateBackend, mutations)
			},
			RollbackCheckpointMutationsFunc: func(mutations []walpkg.CheckpointMutation) {
				restoreCheckpointMutationsToTracker(mutationTracker, mutations)
			},
			Mode:                           strings.ToLower(strings.TrimSpace(cfg.Pipeline.State.CheckpointMode)),
			FullSnapshotEveryBatches:       cfg.Pipeline.State.CheckpointEveryBatches,
			MaxIncrementalMutationBytesVal: cfg.Pipeline.State.MaxIncrementalMutationBytes,
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

func buildPartitionRuntime(cfg *config.PipelineConfig, partitionValues map[string]string, sharedSink provider.Sink) (*partitionRuntime, provider.Sink, error) {
	if cfg.Pipeline.Transform.Type != "sql" {
		return nil, nil, fmt.Errorf("unsupported transform type: %s", cfg.Pipeline.Transform.Type)
	}
	query := strings.TrimSpace(cfg.Pipeline.Transform.Query)
	rootNode, err := compileIncrementalQuery(query)
	if err != nil {
		return nil, nil, fmt.Errorf("compiling SQL query: %w", err)
	}

	if cfg.Pipeline.Transform.Watermark.Enabled {
		wmCfg, err := watermark.BuildWatermarkConfig(cfg.Pipeline.Transform.Watermark)
		if err != nil {
			return nil, nil, fmt.Errorf("parsing watermark config: %w", err)
		}
		watermark.ApplyWatermarkConfig(rootNode, wmCfg)
	}
	ttl, err := validateTransformTTL(cfg.Pipeline.Transform.TTL, cfg.Pipeline.WAL.Enabled)
	if err != nil {
		return nil, nil, err
	}

	sinkCfg := cloneConfigMap(cfg.Pipeline.Sink.Config)
	if cfg.Pipeline.Sink.Type != "http_pull" {
		applyHivePathToSink(cfg.Pipeline.Sink.Type, sinkCfg, cfg.Pipeline.Partition.Keys, partitionValues)
	}

	var parquetSchema *config.ParquetSchema
	if sharedSink == nil && (cfg.Pipeline.Sink.Type == "parquet" || cfg.Pipeline.Sink.Type == "http_pull") {
		parquetSchema, err = config.InferOrLoadParquetSchema(query, cfg.Pipeline.Source, sinkCfg)
		if err != nil {
			return nil, nil, fmt.Errorf("inferring parquet schema: %w", err)
		}
	}
	if cfg.Pipeline.Sink.Type == "http_pull" && !cfg.Pipeline.Partition.Enabled {
		if err := validatePartitionColumns(parquetSchema, rootNode.PartitionBy); err != nil {
			return nil, nil, err
		}
	}

	var snk provider.Sink
	var sharedHTTPPull provider.Sink
	if cfg.Pipeline.Sink.Type == "http_pull" {
		if sharedSink != nil {
			snk = sink.NewNoopCloseSink(sharedSink)
		} else {
			partitionBy := rootNode.PartitionBy
			if cfg.Pipeline.Partition.Enabled {
				partitionBy = cfg.Pipeline.Partition.Keys
			}
			snk, err = sink.NewHTTPPullSink(sinkCfg, partitionBy, parquetSchema)
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
			snk, err = sink.NewConsoleSink(sinkCfg)
		case "file":
			snk, err = sink.NewFileSink(sinkCfg)
		case "parquet":
			snk, err = sink.NewParquetSink(sinkCfg, parquetSchema)
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

	var writeAheadLog *wal.SQLiteWAL
	if cfg.Pipeline.WAL.Enabled {
		walPath := cfg.Pipeline.WAL.Path
		if strings.TrimSpace(walPath) != "" {
			walPath = config.BuildHivePartitionPath(walPath, cfg.Pipeline.Partition.Keys, partitionValues)
		}
		writeAheadLog, err = wal.NewSQLiteWALWithConfig(walPath, buildSQLiteWALConfig(cfg.Pipeline.WAL))
		if err != nil {
			_ = snk.Close()
			return nil, nil, fmt.Errorf("initializing WAL: %w", err)
		}
		if ttl > 0 {
			writeAheadLog.SetRetentionTTL(ttl)
		}
	}

	var stateBackend op.StateBackend
	var mutationTracker op.StateMutationTracker
	if cfg.Pipeline.State.Enabled {
		statePath := cfg.Pipeline.State.Path
		if strings.TrimSpace(statePath) != "" {
			statePath = config.BuildHivePartitionPath(statePath, cfg.Pipeline.Partition.Keys, partitionValues)
		}
		baseStateBackend, stateErr := op.NewStateBackendFromConfig(true, cfg.Pipeline.State.Type, statePath)
		if stateErr != nil {
			err = stateErr
		} else {
			tracked := op.NewMutationTrackingStateBackend(baseStateBackend)
			stateBackend = tracked
			mutationTracker = tracked
		}
		if err != nil {
			if writeAheadLog != nil {
				_ = writeAheadLog.Close()
			}
			_ = snk.Close()
			return nil, nil, fmt.Errorf("initializing state backend: %w", err)
		}
		op.AttachJoinStateBackend(rootNode, stateBackend)
		op.AttachGroupAggStateBackend(rootNode, stateBackend)
		op.AttachWindowAggStateBackend(rootNode, stateBackend)
	}

	rt := &partitionRuntime{
		values:   partitionValues,
		rootNode: rootNode,
		sink:     snk,
		wal:      writeAheadLog,
		state:    stateBackend,
		snapshotter: pipeline.PipelineSnapshotterFunc{
			SnapFunc:    func() ([]byte, error) { return op.SnapshotGraph(rootNode) },
			RestoreFunc: func(b []byte) error { return op.RestoreGraph(rootNode, b) },
			DrainCheckpointMutationsFunc: func() []walpkg.CheckpointMutation {
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
			ApplyCheckpointMutationsFunc: func(mutations []walpkg.CheckpointMutation) error {
				return applyCheckpointMutationsToStateBackend(stateBackend, mutations)
			},
			RollbackCheckpointMutationsFunc: func(mutations []walpkg.CheckpointMutation) {
				restoreCheckpointMutationsToTracker(mutationTracker, mutations)
			},
			Mode:                           strings.ToLower(strings.TrimSpace(cfg.Pipeline.State.CheckpointMode)),
			FullSnapshotEveryBatches:       cfg.Pipeline.State.CheckpointEveryBatches,
			MaxIncrementalMutationBytesVal: cfg.Pipeline.State.MaxIncrementalMutationBytes,
		},
	}
	if err := replayRuntime(context.Background(), rt); err != nil {
		_ = rt.sink.Close()
		if rt.wal != nil {
			_ = rt.wal.Close()
		}
		return nil, nil, err
	}

	return rt, sharedHTTPPull, nil
}

func parsePipelineConfig(configFile []byte) (config.PipelineConfig, error) {
	var cfg config.PipelineConfig
	dec := yaml.NewDecoder(bytes.NewReader(configFile))
	dec.KnownFields(true)
	if err := dec.Decode(&cfg); err != nil {
		return config.PipelineConfig{}, err
	}
	return cfg, nil
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
	rt.batchCount++
	if rt.wal != nil {
		if err := rt.wal.Append(ctx, batch); err != nil {
			return err
		}
	}

	resultBatch, err := op.Execute(rt.rootNode, batch)
	if err != nil {
		return err
	}
	if cfg.Pipeline.Partition.Enabled && cfg.Pipeline.Sink.Type == "http_pull" {
		if ps, ok := rt.sink.(sink.PartitionedSink); ok {
			if err := ps.WriteBatchWithPartition(resultBatch, rt.values); err != nil {
				return err
			}
		} else if err := rt.sink.WriteBatch(resultBatch); err != nil {
			return err
		}
	} else if err := rt.sink.WriteBatch(resultBatch); err != nil {
		return err
	}

	if rt.wal != nil && rt.snapshotter != nil && cfg.Pipeline.WAL.CheckpointEveryBatches > 0 && (rt.batchCount%cfg.Pipeline.WAL.CheckpointEveryBatches) == 0 {
		if cwal, ok := any(rt.wal).(pipeline.CheckpointWAL); ok {
			hook, hasHook := any(rt.snapshotter).(pipeline.CheckpointHookProvider)
			mutationProvider, hasMutationProvider := any(rt.snapshotter).(pipeline.CheckpointMutationProvider)
			mutationRollbackProvider, hasMutationRollbackProvider := any(rt.snapshotter).(pipeline.CheckpointMutationRollbackProvider)
			maxSeq, err := cwal.MaxSeq(ctx)
			if err != nil {
				return err
			}
			mode := strings.ToLower(strings.TrimSpace(cfg.Pipeline.State.CheckpointMode))
			fullEvery := cfg.Pipeline.State.CheckpointEveryBatches
			maxMutationBytes := cfg.Pipeline.State.MaxIncrementalMutationBytes
			if maxMutationBytes <= 0 {
				maxMutationBytes = 1 << 20
			}
			if fullEvery <= 0 {
				fullEvery = cfg.Pipeline.WAL.CheckpointEveryBatches
			}

			if mode == "incremental" {
				checkpointMutations := []walpkg.CheckpointMutation(nil)
				if hasMutationProvider {
					checkpointMutations = mutationProvider.DrainCheckpointMutations()
				}
				cp, err := cwal.LoadLatestCheckpoint(ctx)
				if err != nil {
					return err
				}
				lastFullSeq := int64(0)
				lastCheckpointSeq := int64(0)
				if cp != nil {
					lastCheckpointSeq = cp.LastSeq
					if cp.Mode == "full" {
						lastFullSeq = cp.LastSeq
					} else {
						lastFullSeq = cp.BaseSeq
					}
				}

				forceFull := false
				if depthProvider, ok := cwal.(pipeline.CheckpointChainDepthProvider); ok {
					depth, err := depthProvider.IncrementalChainDepth(ctx, maxSeq)
					if err != nil {
						return err
					}
					if depth >= 8 {
						forceFull = true
					}
				}
				if checkpointMutationSize(checkpointMutations) >= maxMutationBytes {
					forceFull = true
				}

				if forceFull || lastFullSeq == 0 || (fullEvery > 0 && (rt.batchCount%fullEvery) == 0) {
					snap, err := rt.snapshotter.Snapshot()
					if err != nil {
						if hasMutationRollbackProvider && len(checkpointMutations) > 0 {
							mutationRollbackProvider.RollbackCheckpointMutations(checkpointMutations)
						}
						return err
					}
					if err := cwal.SaveCheckpoint(ctx, walpkg.Checkpoint{Mode: "full", LastSeq: maxSeq, BaseSeq: maxSeq, Snapshot: snap, Mutations: checkpointMutations}); err != nil {
						if hasMutationRollbackProvider && len(checkpointMutations) > 0 {
							mutationRollbackProvider.RollbackCheckpointMutations(checkpointMutations)
						}
						return err
					}
					if hasHook {
						hook.AfterCheckpoint("full", maxSeq)
					}
				} else {
					snap, err := rt.snapshotter.Snapshot()
					if err != nil {
						if hasMutationRollbackProvider && len(checkpointMutations) > 0 {
							mutationRollbackProvider.RollbackCheckpointMutations(checkpointMutations)
						}
						return err
					}
					baseSeq := lastCheckpointSeq
					if baseSeq <= 0 {
						baseSeq = lastFullSeq
					}
					if err := cwal.SaveCheckpoint(ctx, walpkg.Checkpoint{Mode: "incremental", LastSeq: maxSeq, BaseSeq: baseSeq, Snapshot: snap, Mutations: checkpointMutations}); err != nil {
						if hasMutationRollbackProvider && len(checkpointMutations) > 0 {
							mutationRollbackProvider.RollbackCheckpointMutations(checkpointMutations)
						}
						return err
					}
					if hasHook {
						hook.AfterCheckpoint("incremental", maxSeq)
					}
				}
			} else {
				snap, err := rt.snapshotter.Snapshot()
				if err != nil {
					return err
				}
				if err := cwal.SaveCheckpoint(ctx, walpkg.Checkpoint{Mode: "full", LastSeq: maxSeq, BaseSeq: maxSeq, Snapshot: snap}); err != nil {
					return err
				}
				if hasHook {
					hook.AfterCheckpoint("full", maxSeq)
				}
			}
		}
	}

	return nil
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
		restoreSnapshot := []byte(nil)
		restoreMutations := []walpkg.CheckpointMutation(nil)
		if cp != nil {
			if resolver, ok := cwal.(pipeline.CheckpointSnapshotResolverWithMutations); ok {
				snap, seq, mutations, err := resolver.ResolveCheckpointSnapshotWithMutations(ctx, cp)
				if err != nil {
					return err
				}
				restoreSnapshot = snap
				afterSeq = seq
				restoreMutations = mutations
			} else if resolver, ok := cwal.(pipeline.CheckpointSnapshotResolver); ok {
				snap, seq, err := resolver.ResolveCheckpointSnapshot(ctx, cp)
				if err != nil {
					return err
				}
				restoreSnapshot = snap
				afterSeq = seq
			} else {
				restoreCP := cp
				if cp.Mode == "incremental" {
					if lookup, ok := cwal.(pipeline.CheckpointWALWithFullLookup); ok {
						fullCP, err := lookup.LoadLatestFullCheckpointBefore(ctx, cp.BaseSeq)
						if err != nil {
							return err
						}
						restoreCP = fullCP
					} else {
						restoreCP = nil
					}
				}
				if restoreCP != nil {
					restoreSnapshot = restoreCP.Snapshot
					afterSeq = restoreCP.LastSeq
				}
			}
		}
		if len(restoreSnapshot) > 0 {
			if err := rt.snapshotter.Restore(restoreSnapshot); err != nil {
				return err
			}
		}
		if len(restoreMutations) > 0 {
			if applier, ok := any(rt.snapshotter).(pipeline.CheckpointMutationApplier); ok {
				if err := applier.ApplyCheckpointMutations(restoreMutations); err != nil {
					return err
				}
			}
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

func checkpointMutationsFromStateOps(ops []op.StateBatchOp) []walpkg.CheckpointMutation {
	if len(ops) == 0 {
		return nil
	}
	out := make([]walpkg.CheckpointMutation, 0, len(ops))
	for _, stateOp := range ops {
		mutationType := "put"
		if stateOp.Type == op.StateBatchDelete {
			mutationType = "delete"
		}
		out = append(out, walpkg.CheckpointMutation{
			Type:  mutationType,
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
		typ := op.StateBatchPut
		if strings.EqualFold(strings.TrimSpace(mutation.Type), "delete") {
			typ = op.StateBatchDelete
		}
		ops = append(ops, op.StateBatchOp{
			Type:  typ,
			Key:   append([]byte(nil), mutation.Key...),
			Value: append([]byte(nil), mutation.Value...),
		})
	}
	if len(ops) == 0 {
		return nil
	}
	return backend.BatchWrite(ops)
}

func restoreCheckpointMutationsToTracker(tracker op.StateMutationTracker, mutations []walpkg.CheckpointMutation) {
	if tracker == nil || len(mutations) == 0 {
		return
	}
	ops := make([]op.StateBatchOp, 0, len(mutations))
	for _, mutation := range mutations {
		typ := op.StateBatchPut
		if strings.EqualFold(strings.TrimSpace(mutation.Type), "delete") {
			typ = op.StateBatchDelete
		}
		ops = append(ops, op.StateBatchOp{
			Type:  typ,
			Key:   append([]byte(nil), mutation.Key...),
			Value: append([]byte(nil), mutation.Value...),
		})
	}
	tracker.RestoreMutations(ops)
}

func checkpointMutationSize(mutations []walpkg.CheckpointMutation) int {
	total := 0
	for _, mutation := range mutations {
		total += len(mutation.Type)
		total += len(mutation.Key)
		total += len(mutation.Value)
	}
	return total
}
