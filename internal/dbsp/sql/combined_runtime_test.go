package sqlconv

import (
	"testing"

	"github.com/ariyn/dbsp/internal/dbsp/op"
)

func TestPowerCalcOutput_HasCombinedDataInputs(t *testing.T) {
	query := `WITH lagged_data AS (
		SELECT
			timestamp,
			entity_id,
			group_id,
			event_date,
			metric_x,
			metric_y,
			baseline,
			signal AS normalized_signal,
			LAG(timestamp) OVER (PARTITION BY entity_id ORDER BY timestamp) AS previous_timestamp,
			LAG(metric_x) OVER (PARTITION BY entity_id ORDER BY timestamp) AS previous_metric_x,
			LAG(metric_y) OVER (PARTITION BY entity_id ORDER BY timestamp) AS previous_metric_y
		FROM events
	),
	power_calc AS (
		SELECT
			timestamp,
			entity_id,
			group_id,
			event_date,
			normalized_signal,
			metric_y,
			baseline,
			metric_x,
			metric_x * metric_y AS score,
			previous_metric_x * previous_metric_y AS previous_score,
			(timestamp::DOUBLE / 1000000000.0) - (previous_timestamp::DOUBLE / 1000000000.0) AS delta_seconds
		FROM lagged_data
		WHERE previous_timestamp IS NOT NULL AND timestamp IS NOT NULL
	)
	SELECT * FROM power_calc`

	root, err := ParseQueryToIncrementalDBSP(query)
	if err != nil {
		t.Fatalf("ParseQueryToIncrementalDBSP failed: %v", err)
	}
	out, err := op.Execute(root, sampleTelemetryBatch())
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}
	if len(out) == 0 {
		t.Fatalf("expected non-empty power_calc output")
	}
	need := []string{"metric_y", "metric_x", "baseline", "normalized_signal", "score", "previous_score", "delta_seconds"}
	tuple := out[0].Tuple
	for _, k := range need {
		if _, ok := tuple[k]; !ok {
			t.Fatalf("expected key %s in power_calc output tuple=%v", k, tuple)
		}
	}
}
