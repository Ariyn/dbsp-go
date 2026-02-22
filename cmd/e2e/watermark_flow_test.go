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

func TestWatermarkFlow(t *testing.T) {
	// Query: Tumbling window of 10 seconds (10000 ms)
	// Single aggregate SUM because multi-agg with window is not supported yet for event-time.
	query := "SELECT k, SUM(v) FROM events GROUP BY k, TUMBLE(ts, INTERVAL '10' SECOND)"
	root, err := sqlconv.ParseQueryToIncrementalDBSP(query)
	if err != nil {
		t.Fatalf("ParseQueryToIncrementalDBSP: %v", err)
	}

	// Watermark: Allow 2 seconds of out-of-orderness
	wmCfg, _ := watermark.BuildWatermarkConfig(config.WatermarkYAMLConfig{
		Enabled:           true,
		Policy:            "emit",
		MaxOutOfOrderness: "2s",
		AllowedLateness:   "5s",
	})
	watermark.ApplyWatermarkConfig(root, wmCfg)

	sink := testutil.NewRecordingSink()

	// Batch 1: Events at T=1s, 5s. MaxTs=5s -> Watermark=3s
	batch1 := types.Batch{
		{Tuple: types.Tuple{"ts": int64(1000), "v": 10.0, "k": "A"}, Count: 1},
		{Tuple: types.Tuple{"ts": int64(5000), "v": 20.0, "k": "A"}, Count: 1},
	}

	// Batch 2: Event at T=12s. MaxTs=12s -> Watermark=10s
	batch2 := types.Batch{
		{Tuple: types.Tuple{"ts": int64(12000), "v": 5.0, "k": "A"}, Count: 1},
	}

	// Batch 3: Late event at T=6s. Watermark=10s. T=6s < Watermark=10s.
	// 6s >= (Watermark=10s - AllowedLateness=5s) = 5s. OK.
	batch3 := types.Batch{
		{Tuple: types.Tuple{"ts": int64(6000), "v": 7.0, "k": "A"}, Count: 1},
	}

	ctx := context.Background()
	execute := func(b types.Batch) (types.Batch, error) { return op.Execute(root, b) }

	pipeline.RunPipeline(ctx, testutil.NewSliceSource([]types.Batch{batch1, batch2, batch3}), sink, execute, nil, nil, 0)

	totalCount := 0
	foundLate := false
	window0Sum := 0.0
	window10Sum := 0.0

	for _, b := range sink.Batches {
		for _, td := range b {
			// t.Logf("Got tuple: %+v", td.Tuple)
			totalCount++

			// SUM(v) in WindowAggOp uses "agg_delta" (SumAgg default DeltaCol)
			sd, ok := td.Tuple["agg_delta"].(float64)
			if !ok {
				// Fallback to int if it happened to be returned that way (though SumAgg uses float64)
				if val, ok := td.Tuple["agg_delta"].(int64); ok {
					sd = float64(val)
				}
			}

			if td.Tuple["__late"] == true {
				foundLate = true
			}

			// W0 start=0, W10 start=10000
			if start, ok := td.Tuple["__window_start"].(int64); ok {
				if start == 0 {
					window0Sum += sd
				} else if start == 10000 {
					window10Sum += sd
				}
			}
		}
	}

	if !foundLate {
		t.Errorf("Expected to find a late event marked with __late=true")
	}
	if window0Sum != 37.0 {
		t.Errorf("Expected window 0 sum to be 37.0 (10+20+7), got %f. total tuples=%d", window0Sum, totalCount)
	}
	if window10Sum != 5.0 {
		t.Errorf("Expected window 10000 sum to be 5.0, got %f. total tuples=%d", window10Sum, totalCount)
	}
}
