package main

import "github.com/ariyn/dbsp/internal/dbsp/types"

// Source is the interface for data sources
type Source interface {
	// NextBatch returns the next batch of data. Returns nil, nil when exhausted.
	NextBatch() (types.Batch, error)
	Close() error
}

// Sink is the interface for data sinks
type Sink interface {
	// WriteBatch writes a batch of results.
	WriteBatch(types.Batch) error
	Close() error
}
