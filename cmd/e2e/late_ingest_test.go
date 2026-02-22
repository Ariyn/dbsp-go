package e2e

import (
	"context"
	"testing"

	"github.com/ariyn/dbsp/cmd/dbsp/config"
	"github.com/ariyn/dbsp/cmd/dbsp/pipeline"
	"github.com/ariyn/dbsp/cmd/dbsp/watermark"
	"github.com/ariyn/dbsp/internal/dbsp/op"
	sqlconv "github.com/ariyn/dbsp/internal/dbsp/sql"
	"github.com/ariyn/dbsp/internal/dbsp/testutil"
	"github.com/ariyn/dbsp/internal/dbsp/types"
)

func TestLateIngest(t *testing.T) {
	// Query: Tumbling window of 10 seconds
	query := "SELECT k, SUM(v) FROM events GROUP BY k, TUMBLE(ts, INTERVAL '10' SECOND)"
	root, _ := sqlconv.ParseQueryToIncrementalDBSP(query)

	// Watermark: 0 out-of-orderness, 5s allowed lateness.
	// Policy: EmitLateEvents to allow "late but within allowed lateness" updates.
	wmCfg, _ := watermark.BuildWatermarkConfig(config.WatermarkYAMLConfig{
		Enabled:           true,
		Policy:            "emit",
		MaxOutOfOrderness: "0s",
		AllowedLateness:   "5s",
	})
	watermark.ApplyWatermarkConfig(root, wmCfg)

	sink := testutil.NewRecordingSink()

	// 1. Advance watermark to 10s
	batch1 := types.Batch{
		{Tuple: types.Tuple{"ts": int64(10000), "v": 10.0, "k": "A"}, Count: 1},
	}

	// 2. Late event T=7s. Watermark=10s.
	// 7s < 10s (Late) but 7s >= (10s - 5s) = 5s (Allowed).
	// Should be processed and marked as __late.
	batch2 := types.Batch{
		{Tuple: types.Tuple{"ts": int64(7000), "v": 20.0, "k": "A"}, Count: 1},
	}

	// 3. Too late event T=4s.
	// 4s < 5s (Too late). Should be dropped completely.
	batch3 := types.Batch{
		{Tuple: types.Tuple{"ts": int64(4000), "v": 30.0, "k": "A"}, Count: 1},
	}

	ctx := context.Background()
	execute := func(b types.Batch) (types.Batch, error) { return op.Execute(root, b) }
	pipeline.RunPipeline(ctx, testutil.NewSliceSource([]types.Batch{batch1, batch2, batch3}), sink, execute, nil, nil, 0)

	window0Sum := 0.0
	foundLate := false
	processedBatch3 := false

	for _, b := range sink.Batches {
		for _, td := range b {
			sd, _ := td.Tuple["agg_delta"].(float64)
			if td.Tuple["__late"] == true {
				foundLate = true
			}
			// Check if T=4s (v=30.0) was processed.
			if sd == 30.0 {
				processedBatch3 = true
			}

			if start, ok := td.Tuple["__window_start"].(int64); ok && start == 0 {
				window0Sum += sd
			}
		}
	}

	if !foundLate {
		t.Errorf("Expected T=7s to be processed as late event")
	}
	if processedBatch3 {
		t.Errorf("Expected T=4s to be dropped as it exceeded allowed lateness")
	}
	// Window 0-10 (T=10s belongs to W10-20, T=7s belongs to W0-10, T=4s dropped)
	// So Window 0 sum should ONLY be 20.0 (from T=7s).
	// Wait! Batch 1 had T=10000 which is window_start=10000.
	if window0Sum != 20.0 {
		t.Errorf("Expected window 0 cumulative sum to be 20.0, got %f", window0Sum)
	}
}
