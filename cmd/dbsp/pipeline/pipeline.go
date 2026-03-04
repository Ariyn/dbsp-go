package pipeline

import (
	"context"
	"fmt"

	"github.com/ariyn/dbsp/cmd/dbsp/provider"
	"github.com/ariyn/dbsp/internal/dbsp/types"
	walpkg "github.com/ariyn/dbsp/internal/dbsp/wal"
)

type executeFn func(types.Batch) (types.Batch, error)

type PipelineSnapshotter interface {
	Snapshot() ([]byte, error)
	Restore([]byte) error
}

type PipelineWAL interface {
	Append(ctx context.Context, batch types.Batch) error
	Replay(ctx context.Context, apply func(types.Batch) error) error
}

type CheckpointWAL interface {
	LoadLatestCheckpoint(ctx context.Context) (*walpkg.Checkpoint, error)
	SaveCheckpoint(ctx context.Context, cp walpkg.Checkpoint) error
	ReplayFrom(ctx context.Context, afterSeq int64, apply func(types.Batch) error) error
	MaxSeq(ctx context.Context) (int64, error)
	LoadLatestFullCheckpointBefore(ctx context.Context, upToSeq int64) (*walpkg.Checkpoint, error)
	ResolveCheckpointSnapshotWithMutations(ctx context.Context, cp *walpkg.Checkpoint) ([]byte, int64, []walpkg.CheckpointMutation, error)
}

type CheckpointPolicyProvider interface {
	CheckpointMode() string
	FullSnapshotEvery() int
	MaxIncrementalMutationBytes() int
}

type CheckpointHookProvider interface {
	AfterCheckpoint(mode string, lastSeq int64)
}

type CheckpointMutationProvider interface {
	DrainCheckpointMutations() []walpkg.CheckpointMutation
}

type CheckpointMutationApplier interface {
	ApplyCheckpointMutations([]walpkg.CheckpointMutation) error
}

type CheckpointMutationRollbackProvider interface {
	RollbackCheckpointMutations([]walpkg.CheckpointMutation)
}

type CheckpointChainDepthProvider interface {
	IncrementalChainDepth(ctx context.Context, upToSeq int64) (int, error)
}

const defaultMaxIncrementalCheckpointDepth = 8
const defaultMaxIncrementalCheckpointMutationBytes = 1 << 20

type CheckpointState struct {
	BatchCount            int
	LastFullCheckpointSeq int64
	LastCheckpointSeq     int64
}

func estimateCheckpointMutationBytes(mutations []walpkg.CheckpointMutation) int {
	total := 0
	for _, mutation := range mutations {
		total += len(mutation.Type)
		total += len(mutation.Key)
		total += len(mutation.Value)
	}
	return total
}

func NewCheckpointState(ctx context.Context, writeAheadLog PipelineWAL) (CheckpointState, error) {
	state := CheckpointState{}
	if writeAheadLog == nil {
		return state, nil
	}
	if cwal, ok := writeAheadLog.(CheckpointWAL); ok {
		cp, err := cwal.LoadLatestCheckpoint(ctx)
		if err != nil {
			return state, err
		}
		if cp != nil {
			state.LastCheckpointSeq = cp.LastSeq
			if cp.Mode == "full" {
				state.LastFullCheckpointSeq = cp.LastSeq
			} else {
				state.LastFullCheckpointSeq = cp.BaseSeq
			}
		}
	}
	return state, nil
}

func ReplayWithCheckpoint(ctx context.Context, writeAheadLog PipelineWAL, snapshotter PipelineSnapshotter, apply func(types.Batch) error) error {
	if writeAheadLog == nil {
		return nil
	}
	if cwal, ok := writeAheadLog.(CheckpointWAL); ok && snapshotter != nil {
		cp, err := cwal.LoadLatestCheckpoint(ctx)
		if err != nil {
			return err
		}
		afterSeq := int64(0)
		restoreSnapshot := []byte(nil)
		restoreMutations := []walpkg.CheckpointMutation(nil)
		if cp != nil {
			snap, seq, mutations, err := cwal.ResolveCheckpointSnapshotWithMutations(ctx, cp)
			if err != nil {
				return err
			}
			restoreSnapshot = snap
			afterSeq = seq
			restoreMutations = mutations

			if restoreSnapshot == nil {
				// Fallback to manual resolution if something went wrong or for full-only logic.
				restoreCP := cp
				if cp.Mode == "incremental" {
					fullCP, err := cwal.LoadLatestFullCheckpointBefore(ctx, cp.BaseSeq)
					if err != nil {
						return err
					}
					restoreCP = fullCP
				}
				if restoreCP != nil {
					restoreSnapshot = restoreCP.Snapshot
					afterSeq = restoreCP.LastSeq
				}
			}
		}
		if len(restoreSnapshot) > 0 {
			if err := snapshotter.Restore(restoreSnapshot); err != nil {
				return err
			}
		}
		if len(restoreMutations) > 0 {
			if applier, ok := snapshotter.(CheckpointMutationApplier); ok {
				if err := applier.ApplyCheckpointMutations(restoreMutations); err != nil {
					return err
				}
			}
		}
		return cwal.ReplayFrom(ctx, afterSeq, apply)
	}
	return writeAheadLog.Replay(ctx, apply)
}

func RunBatchWithCheckpoint(ctx context.Context, batch types.Batch, execute executeFn, writeAheadLog PipelineWAL, snapshotter PipelineSnapshotter, checkpointEveryBatches int, state *CheckpointState, sinkWrite func(types.Batch) error) error {
	if state == nil {
		return fmt.Errorf("checkpoint state is nil")
	}
	state.BatchCount++
	if writeAheadLog != nil {
		if err := writeAheadLog.Append(ctx, batch); err != nil {
			return err
		}
	}

	resultBatch, err := execute(batch)
	if err != nil {
		return err
	}
	if err := sinkWrite(resultBatch); err != nil {
		return err
	}

	if writeAheadLog != nil && snapshotter != nil && checkpointEveryBatches > 0 && (state.BatchCount%checkpointEveryBatches) == 0 {
		if cwal, ok := writeAheadLog.(CheckpointWAL); ok {
			hook, hasHook := snapshotter.(CheckpointHookProvider)
			mutationProvider, hasMutationProvider := snapshotter.(CheckpointMutationProvider)
			mutationRollbackProvider, hasMutationRollbackProvider := snapshotter.(CheckpointMutationRollbackProvider)
			maxSeq, err := cwal.MaxSeq(ctx)
			if err != nil {
				return err
			}

			mode := "full"
			fullEvery := checkpointEveryBatches
			maxMutationBytes := defaultMaxIncrementalCheckpointMutationBytes
			if policy, ok := snapshotter.(CheckpointPolicyProvider); ok {
				if m := policy.CheckpointMode(); m != "" {
					mode = m
				}
				if v := policy.FullSnapshotEvery(); v > 0 {
					fullEvery = v
				}
				if v := policy.MaxIncrementalMutationBytes(); v > 0 {
					maxMutationBytes = v
				}
			}

			if mode == "incremental" {
				checkpointMutations := []walpkg.CheckpointMutation(nil)
				if hasMutationProvider {
					checkpointMutations = mutationProvider.DrainCheckpointMutations()
				}

				forceFull := false
				if depthProvider, ok := cwal.(CheckpointChainDepthProvider); ok {
					depth, err := depthProvider.IncrementalChainDepth(ctx, maxSeq)
					if err != nil {
						return err
					}
					if depth >= defaultMaxIncrementalCheckpointDepth {
						forceFull = true
					}
				}
				if estimateCheckpointMutationBytes(checkpointMutations) >= maxMutationBytes {
					forceFull = true
				}

				if forceFull || state.LastFullCheckpointSeq == 0 || (fullEvery > 0 && (state.BatchCount%fullEvery) == 0) {
					snap, err := snapshotter.Snapshot()
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
					state.LastFullCheckpointSeq = maxSeq
					state.LastCheckpointSeq = maxSeq
				} else {
					snap, err := snapshotter.Snapshot()
					if err != nil {
						if hasMutationRollbackProvider && len(checkpointMutations) > 0 {
							mutationRollbackProvider.RollbackCheckpointMutations(checkpointMutations)
						}
						return err
					}
					baseSeq := state.LastCheckpointSeq
					if baseSeq <= 0 {
						baseSeq = state.LastFullCheckpointSeq
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
					state.LastCheckpointSeq = maxSeq
				}
			} else {
				snap, err := snapshotter.Snapshot()
				if err != nil {
					return err
				}
				if err := cwal.SaveCheckpoint(ctx, walpkg.Checkpoint{Mode: "full", LastSeq: maxSeq, BaseSeq: maxSeq, Snapshot: snap}); err != nil {
					return err
				}
				if hasHook {
					hook.AfterCheckpoint("full", maxSeq)
				}
				state.LastFullCheckpointSeq = maxSeq
				state.LastCheckpointSeq = maxSeq
			}
		}
	}

	return nil
}

func RunPipeline(ctx context.Context, source provider.Source, sink provider.Sink, execute executeFn, writeAheadLog PipelineWAL, snapshotter PipelineSnapshotter, checkpointEveryBatches int) error {
	if ctx == nil {
		return fmt.Errorf("context is nil")
	}
	if source == nil {
		return fmt.Errorf("source is nil")
	}
	if sink == nil {
		return fmt.Errorf("sink is nil")
	}
	if execute == nil {
		return fmt.Errorf("execute function is nil")
	}

	stopCloser := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			_ = source.Close()
		case <-stopCloser:
		}
	}()
	defer close(stopCloser)

	// Recovery path: replay previously logged batches to rebuild in-memory operator state.
	// Only sinks that opt in via ReplaySink will receive replay outputs.
	replaySink, hasReplaySink := sink.(provider.ReplaySink)
	if err := ReplayWithCheckpoint(ctx, writeAheadLog, snapshotter, func(b types.Batch) error {
		result, err := execute(b)
		if err != nil {
			return err
		}
		if hasReplaySink {
			return replaySink.ReplayWriteBatch(result)
		}
		return nil
	}); err != nil {
		return err
	}

	state, err := NewCheckpointState(ctx, writeAheadLog)
	if err != nil {
		return err
	}

	for {
		batch, err := source.NextBatch()
		if err != nil {
			return err
		}
		if batch == nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return nil
		}

		fmt.Printf("Processing batch %d with %d records...\n", state.BatchCount+1, len(batch))
		if err := RunBatchWithCheckpoint(ctx, batch, execute, writeAheadLog, snapshotter, checkpointEveryBatches, &state, sink.WriteBatch); err != nil {
			return err
		}

		if ctx.Err() != nil {
			return ctx.Err()
		}
	}
}
