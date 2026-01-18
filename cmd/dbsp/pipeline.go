package main

import (
	"context"
	"fmt"

	"github.com/ariyn/dbsp/internal/dbsp/types"
	walpkg "github.com/ariyn/dbsp/internal/dbsp/wal"
)

type executeFn func(types.Batch) (types.Batch, error)

type pipelineSnapshotter interface {
	Snapshot() ([]byte, error)
	Restore([]byte) error
}

type pipelineWAL interface {
	Append(ctx context.Context, batch types.Batch) error
	Replay(ctx context.Context, apply func(types.Batch) error) error
}

type checkpointWAL interface {
	LoadLatestCheckpoint(ctx context.Context) (*walpkg.Checkpoint, error)
	SaveCheckpoint(ctx context.Context, cp walpkg.Checkpoint) error
	ReplayFrom(ctx context.Context, afterSeq int64, apply func(types.Batch) error) error
	MaxSeq(ctx context.Context) (int64, error)
}

func runPipeline(ctx context.Context, source Source, sink Sink, execute executeFn, writeAheadLog pipelineWAL, snapshotter pipelineSnapshotter, checkpointEveryBatches int) error {
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
		if cwal, ok := writeAheadLog.(checkpointWAL); ok && snapshotter != nil {
			cp, err := cwal.LoadLatestCheckpoint(ctx)
			if err != nil {
				return err
			}
			afterSeq := int64(0)
			if cp != nil && len(cp.Snapshot) > 0 {
				if err := snapshotter.Restore(cp.Snapshot); err != nil {
					return err
				}
				afterSeq = cp.LastSeq
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

		if writeAheadLog != nil && snapshotter != nil && checkpointEveryBatches > 0 && (batchCount%checkpointEveryBatches) == 0 {
			if cwal, ok := writeAheadLog.(checkpointWAL); ok {
				snap, err := snapshotter.Snapshot()
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

		resultBatch, err := execute(batch)
		if err != nil {
			return err
		}
		if err := sink.WriteBatch(resultBatch); err != nil {
			return err
		}

		if ctx.Err() != nil {
			return ctx.Err()
		}
	}
}
