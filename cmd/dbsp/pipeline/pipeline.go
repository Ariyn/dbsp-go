package pipeline

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/ariyn/dbsp/cmd/dbsp/provider"
	"github.com/ariyn/dbsp/internal/dbsp/types"
)

type executeFn func(types.Batch) (types.Batch, error)

func RunPipeline(ctx context.Context, source provider.Source, sink provider.Sink, execute executeFn) error {
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
		resultBatch, err := execute(batch)
		if err != nil {
			return err
		}
		if strings.TrimSpace(os.Getenv("DBSP_DEBUG_PIPELINE")) != "" {
			fmt.Printf("DEBUG pipeline: output batch size=%d\n", len(resultBatch))
		}
		if err := sink.WriteBatch(resultBatch); err != nil {
			return err
		}

		if ctx.Err() != nil {
			return ctx.Err()
		}
	}
}
