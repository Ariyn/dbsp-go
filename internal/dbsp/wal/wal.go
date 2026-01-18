package wal

import (
	"context"

	"github.com/ariyn/dbsp/internal/dbsp/types"
)

// WAL is an append-only log of input batches for crash recovery.
//
// In WAL-only mode, the system restores state by replaying all logged batches
// into the same execution path used during normal processing.
//
// Note: WAL replay typically SHOULD NOT re-emit sink outputs to avoid duplicates.
// The pipeline is responsible for choosing whether to forward replay outputs.
type WAL interface {
	Append(ctx context.Context, batch types.Batch) error
	Replay(ctx context.Context, apply func(types.Batch) error) error
	Close() error
}
