package sqlconv

import (
	"testing"

	"github.com/ariyn/dbsp/internal/dbsp/op"
	"github.com/ariyn/dbsp/internal/dbsp/types"
)

func TestTrace_FullQueryWithFloatTimestampInput(t *testing.T) {
	query := `WITH lagged_data AS (
		SELECT
			timestamp,
			panel_position,
			plant_id,
			local_date,
			v_out,
			i_out,
			v_in,
			temperature AS temp,
			LAG(timestamp) OVER (PARTITION BY panel_position ORDER BY timestamp) AS timestamp_last,
			LAG(v_out) OVER (PARTITION BY panel_position ORDER BY timestamp) AS v_out_last,
			LAG(i_out) OVER (PARTITION BY panel_position ORDER BY timestamp) AS i_out_last
		FROM events
	),
	power_calc AS (
		SELECT
			timestamp_last AS timestamp_start,
			timestamp AS timestamp_end,
			timestamp,
			panel_position,
			plant_id,
			local_date,
			v_out,
			i_out,
			v_in,
			temp,
			v_out * i_out AS p_out,
			v_out_last * i_out_last AS p_out_last,
			(timestamp::DOUBLE / 1000000000.0) - (timestamp_last::DOUBLE / 1000000000.0) AS timedelta_second
		FROM lagged_data
		WHERE timestamp_last IS NOT NULL AND timestamp IS NOT NULL
	),
	combined_data AS (
		SELECT
			panel_position AS id,
			plant_id,
			local_date,
			TIME_BUCKET(INTERVAL '5 min', timestamp::TIMESTAMP) AS binned_date,
			AVG(i_out) AS i_out_raw,
			AVG(i_out * v_out) AS p_raw,
			AVG(v_in) AS v_in_raw,
			AVG(v_out) AS v_out_raw,
			AVG(temp) AS temp_raw,
			SUM((p_out + p_out_last) * timedelta_second / 2.0 / 3600.0) AS energy
		FROM power_calc
		GROUP BY id, plant_id, local_date, binned_date
	),
	final_data AS (
		SELECT
			ROUND(i_out_raw, 2) AS i_out,
			ROUND(p_raw, 2) AS p,
			ROUND(v_in_raw, 2) AS v_in,
			ROUND(v_out_raw, 2) AS v_out,
			ROUND(temp_raw, 2) AS temp,
			energy,
			SUM(energy) OVER (PARTITION BY id ORDER BY binned_date) AS cumulative_energy,
			id,
			plant_id,
			local_date,
			STRFTIME(binned_date, '%H:%M:%S') AS date,
			binned_date AS timestamp
		FROM combined_data
	)
	SELECT *
	FROM final_data
	ORDER BY date
	PARTITION BY plant_id, local_date`

	root, err := ParseQueryToIncrementalDBSP(query)
	if err != nil {
		t.Fatalf("ParseQueryToIncrementalDBSP failed: %v", err)
	}

	batch := types.Batch{
		{Tuple: types.Tuple{
			"timestamp":      float64(1_700_000_000_000_000_000),
			"panel_position": "p1",
			"plant_id":       "gxdfsvdoellfl7t",
			"local_date":     "2026-02-27",
			"v_out":          7.0,
			"i_out":          2.0,
			"v_in":           8.0,
			"temperature":    10.0,
		}, Count: 1},
		{Tuple: types.Tuple{
			"timestamp":      float64(1_700_000_300_000_000_000),
			"panel_position": "p1",
			"plant_id":       "gxdfsvdoellfl7t",
			"local_date":     "2026-02-27",
			"v_out":          6.0,
			"i_out":          3.0,
			"v_in":           8.5,
			"temperature":    11.0,
		}, Count: 1},
	}

	out, err := op.Execute(root, batch)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}
	if len(out) == 0 {
		t.Fatalf("expected non-empty output with float timestamp input")
	}
}
