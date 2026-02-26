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
}

type CheckpointWALWithFullLookup interface {
	CheckpointWAL
	LoadLatestFullCheckpointBefore(ctx context.Context, upToSeq int64) (*walpkg.Checkpoint, error)
}

type CheckpointSnapshotResolver interface {
	ResolveCheckpointSnapshot(ctx context.Context, cp *walpkg.Checkpoint) ([]byte, int64, error)
}

type CheckpointSnapshotResolverWithMutations interface {
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

func estimateCheckpointMutationBytes(mutations []walpkg.CheckpointMutation) int {
	total := 0
	for _, mutation := range mutations {
		total += len(mutation.Type)
		total += len(mutation.Key)
		total += len(mutation.Value)
	}
	return total
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
	// We intentionally do not forward replay outputs to the sink to avoid duplicates.
	if writeAheadLog != nil {
		if cwal, ok := writeAheadLog.(CheckpointWAL); ok && snapshotter != nil {
			cp, err := cwal.LoadLatestCheckpoint(ctx)
			if err != nil {
				return err
			}
			afterSeq := int64(0)
			restoreSnapshot := []byte(nil)
			restoreMutations := []walpkg.CheckpointMutation(nil)
			if cp != nil {
				if resolver, ok := cwal.(CheckpointSnapshotResolverWithMutations); ok {
					snap, seq, mutations, err := resolver.ResolveCheckpointSnapshotWithMutations(ctx, cp)
					if err != nil {
						return err
					}
					restoreSnapshot = snap
					afterSeq = seq
					restoreMutations = mutations
				} else if resolver, ok := cwal.(CheckpointSnapshotResolver); ok {
					snap, seq, err := resolver.ResolveCheckpointSnapshot(ctx, cp)
					if err != nil {
						return err
					}
					restoreSnapshot = snap
					afterSeq = seq
				} else {
					restoreCP := cp
					if cp.Mode == "incremental" {
						if lookup, ok := cwal.(CheckpointWALWithFullLookup); ok {
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
			if err := cwal.ReplayFrom(ctx, afterSeq, func(b types.Batch) error {
				_, err := execute(b)
				return err
			}); err != nil {
				return err
			}
		} else {
			if err := writeAheadLog.Replay(ctx, func(b types.Batch) error {
				_, err := execute(b)
				return err
			}); err != nil {
				return err
			}
		}
	}

	batchCount := 0
	lastFullCheckpointSeq := int64(0)
	lastCheckpointSeq := int64(0)
	if writeAheadLog != nil {
		if cwal, ok := writeAheadLog.(CheckpointWAL); ok {
			if cp, err := cwal.LoadLatestCheckpoint(ctx); err == nil && cp != nil {
				lastCheckpointSeq = cp.LastSeq
				if cp.Mode == "full" {
					lastFullCheckpointSeq = cp.LastSeq
				} else {
					lastFullCheckpointSeq = cp.BaseSeq
				}
			}
		}
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

		batchCount++
		fmt.Printf("Processing batch %d with %d records...\n", batchCount, len(batch))

		if writeAheadLog != nil {
			if err := writeAheadLog.Append(ctx, batch); err != nil {
				return err
			}
		}

		resultBatch, err := execute(batch)
		if err != nil {
			return err
		}
		if err := sink.WriteBatch(resultBatch); err != nil {
			return err
		}

		if writeAheadLog != nil && snapshotter != nil && checkpointEveryBatches > 0 && (batchCount%checkpointEveryBatches) == 0 {
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

					if forceFull || lastFullCheckpointSeq == 0 || (fullEvery > 0 && (batchCount%fullEvery) == 0) {
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
						lastFullCheckpointSeq = maxSeq
						lastCheckpointSeq = maxSeq
					} else {
						snap, err := snapshotter.Snapshot()
						if err != nil {
							if hasMutationRollbackProvider && len(checkpointMutations) > 0 {
								mutationRollbackProvider.RollbackCheckpointMutations(checkpointMutations)
							}
							return err
						}
						baseSeq := lastCheckpointSeq
						if baseSeq <= 0 {
							baseSeq = lastFullCheckpointSeq
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
						lastCheckpointSeq = maxSeq
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
					lastFullCheckpointSeq = maxSeq
					lastCheckpointSeq = maxSeq
				}
			}
		}

		if ctx.Err() != nil {
			return ctx.Err()
		}
	}
}
