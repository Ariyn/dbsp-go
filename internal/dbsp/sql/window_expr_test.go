package sqlconv

import (
	"fmt"
	"testing"
	"time"

	"github.com/ariyn/dbsp/internal/dbsp/ir"
	"github.com/ariyn/dbsp/internal/dbsp/op"
	"github.com/ariyn/dbsp/internal/dbsp/types"
)

func TestComplexTelemetryQuery_E2E(t *testing.T) {
	// The user's target query (simplified for testing environment)
	query := `
WITH RawTelemetry AS (
    SELECT
        device_id,
        time_bucket(INTERVAL '5' MINUTE, (event_time::TIMESTAMP)) AS bucket,
        state->'active_power'::DOUBLE AS power,
        LAG(state->'active_power'::DOUBLE) OVER (PARTITION BY device_id ORDER BY event_time) AS prev_power
    FROM telemetry_stream
    WHERE state->'active_power' IS NOT NULL
)
SELECT
    device_id,
    bucket,
    AVG(power) AS avg_power,
    SUM(power - prev_power) AS total_power_delta
FROM RawTelemetry
GROUP BY device_id, bucket
ORDER BY bucket ASC, device_id ASC
`

	// Parse to incremental DBSP graph
	node, err := ParseQueryToIncrementalDBSP(query)
	if err != nil {
		t.Fatalf("Failed to parse query: %v", err)
	}

	// Create test data
	// Note: event_time is a string that will be cast to TIMESTAMP
	baseTime := time.Date(2024, 1, 1, 10, 0, 0, 0, time.UTC)

	// Batch 1: Initial data
	data1 := types.Batch{
		{
			Tuple: types.Tuple{
				"device_id":  "D1",
				"event_time": baseTime.Format("2006-01-02 15:04:05"),
				"state":      map[string]any{"active_power": 100.0},
			},
			Count: 1,
		},
		{
			Tuple: types.Tuple{
				"device_id":  "D1",
				"event_time": baseTime.Add(2 * time.Minute).Format("2006-01-02 15:04:05"),
				"state":      map[string]any{"active_power": 120.0},
			},
			Count: 1,
		},
		{
			Tuple: types.Tuple{
				"device_id":  "D1",
				"event_time": baseTime.Add(6 * time.Minute).Format("2006-01-02 15:04:05"),
				"state":      map[string]any{"active_power": 150.0},
			},
			Count: 1,
		},
	}

	// Execute Batch 1
	out1, err := op.Execute(node, data1)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	// Verify results of Batch 1
	// Bucket 1 (10:00 - 10:05):
	//   D1 entry 1: power=100, prev=nil
	//   D1 entry 2: power=120, prev=100 -> delta = 20
	//   Avg(100, 120) = 110, Sum(delta) = 20
	// Bucket 2 (10:05 - 10:10):
	//   D1 entry 3: power=150, prev=120 -> delta = 30
	//   Avg(150) = 150, Sum(delta) = 30

	if len(out1) < 2 {
		t.Errorf("Expected at least 2 output rows, got %d", len(out1))
		for i, v := range out1 {
			fmt.Printf("Out[%d]: %v\n", i, v.Tuple)
		}
	}

	// Let's check a specific row (e.g., Bucket 1 for D1)
	// Results might be incremental deltas, so we need to accumulate or check final state.
	// For simplicity in this test, we check if the values appear.

	foundBucket1 := false
	foundBucket2 := false
	for _, td := range out1 {
		bucket, _ := td.Tuple["bucket"].(time.Time)
		avgPower, _ := td.Tuple["avg_power"].(float64)
		sumDelta, _ := td.Tuple["total_power_delta"].(float64)

		if bucket.Equal(baseTime) {
			foundBucket1 = true
			if avgPower != 110.0 {
				t.Errorf("Bucket 1: Expected avg_power 110.0, got %v", avgPower)
			}
			if sumDelta != 20.0 {
				t.Errorf("Bucket 1: Expected total_power_delta 20.0, got %v", sumDelta)
			}
		}
		if bucket.Equal(baseTime.Add(5 * time.Minute)) {
			foundBucket2 = true
			if avgPower != 150.0 {
				t.Errorf("Bucket 2: Expected avg_power 150.0, got %v", avgPower)
			}
			if sumDelta != 30.0 {
				t.Errorf("Bucket 2: Expected total_power_delta 30.0, got %v", sumDelta)
			}
		}
	}

	if !foundBucket1 {
		t.Errorf("Bucket 1 (10:00) not found in output")
	}
	if !foundBucket2 {
		t.Errorf("Bucket 2 (10:05) not found in output")
	}
}

func TestSimplifiedExprInAgg(t *testing.T) {
	// Manually build LogicalPlan to bypass parser (and its Cgo/linking issues on macOS)
	// SELECT device_id, SUM(a - b) as diff_sum FROM t GROUP BY device_id
	lp := &ir.LogicalGroupAgg{
		Keys: []string{"device_id"},
		Aggs: []ir.AggSpec{
			{Name: "SUM", Col: "a - b"},
		},
		Input: &ir.LogicalScan{Table: "t"},
	}

	node, err := ir.LogicalToDBSP(lp)
	if err != nil {
		t.Fatalf("Plan transformation error: %v", err)
	}

	data := types.Batch{
		{Tuple: types.Tuple{"device_id": "D1", "a": 10.0, "b": 2.0}, Count: 1},
		{Tuple: types.Tuple{"device_id": "D1", "a": 20.0, "b": 5.0}, Count: 1},
	}

	out, err := op.Execute(node, data)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}
	// Expected: SUM(10-2, 20-5) = SUM(8, 15) = 23.0
	found := false
	for _, td := range out {
		if td.Tuple["agg_delta"] == 23.0 {
			found = true
		}
	}
	if !found {
		t.Errorf("Expected SUM(a-b)=23.0 not found in %v", out)
	}
}
