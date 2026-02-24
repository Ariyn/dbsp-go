package e2e

import (
	"context"
	"runtime"
	"testing"
	"time"

	"github.com/ariyn/dbsp/cmd/dbsp/pipeline"
	"github.com/ariyn/dbsp/internal/dbsp/op"
	sqlconv "github.com/ariyn/dbsp/internal/dbsp/sql"
	"github.com/ariyn/dbsp/internal/dbsp/testutil"
	"github.com/ariyn/dbsp/internal/dbsp/types"
)

func TestTelemetryExprFlow_E2E(t *testing.T) {
	if runtime.GOOS == "darwin" {
		t.Skip("parser-linked e2e can be unstable on darwin in local env; covered by parser-independent unit tests")
	}

	query := `
WITH power_prev AS (
    SELECT
        device_id,
        event_time,
        state->'active_power'::DOUBLE AS power,
        LAG(state->'active_power'::DOUBLE) OVER (PARTITION BY device_id ORDER BY event_time) AS prev_power
    FROM telemetry
)
SELECT
    device_id,
    TIME_BUCKET(INTERVAL '5' MINUTE, event_time::TIMESTAMP) AS bucket,
    AVG(power) AS avg_power,
    SUM(power - prev_power) AS power_delta
FROM power_prev
GROUP BY device_id, bucket
ORDER BY bucket ASC, device_id ASC
`

	root, err := sqlconv.ParseQueryToIncrementalDBSP(query)
	if err != nil {
		t.Fatalf("ParseQueryToIncrementalDBSP: %v", err)
	}

	base := time.Date(2024, 1, 1, 10, 0, 0, 0, time.UTC)
	batch := types.Batch{
		{Tuple: types.Tuple{"device_id": "D1", "event_time": base.Format("2006-01-02 15:04:05"), "state": map[string]any{"active_power": 100.0}}, Count: 1},
		{Tuple: types.Tuple{"device_id": "D1", "event_time": base.Add(2 * time.Minute).Format("2006-01-02 15:04:05"), "state": map[string]any{"active_power": 120.0}}, Count: 1},
		{Tuple: types.Tuple{"device_id": "D1", "event_time": base.Add(6 * time.Minute).Format("2006-01-02 15:04:05"), "state": map[string]any{"active_power": 150.0}}, Count: 1},
	}

	sink := testutil.NewRecordingSink()
	ctx := context.Background()
	execute := func(b types.Batch) (types.Batch, error) { return op.Execute(root, b) }
	pipeline.RunPipeline(ctx, testutil.NewSliceSource([]types.Batch{batch}), sink, execute, nil, nil, 0)

	found1000 := false
	found1005 := false

	for _, b := range sink.Batches {
		for _, td := range b {
			bucket, ok := td.Tuple["bucket"].(time.Time)
			if !ok {
				continue
			}
			avg, _ := td.Tuple["avg_power"].(float64)
			sumDelta, _ := td.Tuple["power_delta"].(float64)

			if bucket.Equal(base) {
				if avg == 110.0 && sumDelta == 20.0 {
					found1000 = true
				}
			}
			if bucket.Equal(base.Add(5 * time.Minute)) {
				if avg == 150.0 && sumDelta == 30.0 {
					found1005 = true
				}
			}
		}
	}

	if !found1000 || !found1005 {
		t.Fatalf("expected buckets with avg/sum results not found; found1000=%v found1005=%v batches=%v", found1000, found1005, sink.Batches)
	}
}
