package sqlconv

import "testing"

func TestUserQueryCompile(t *testing.T) {
	query := `WITH lagged_data AS (
		SELECT
			timestamp,
			device_id,
			site_id,
			event_day,
			voltage,
			current,
			input_voltage,
			temperature_c AS temperature,
			LAG(timestamp) OVER (PARTITION BY device_id ORDER BY timestamp) AS previous_timestamp,
			LAG(voltage) OVER (PARTITION BY device_id ORDER BY timestamp) AS previous_voltage,
			LAG(current) OVER (PARTITION BY device_id ORDER BY timestamp) AS previous_current
		FROM events
	),
	power_calc AS (
		SELECT
			previous_timestamp AS timestamp_start,
			timestamp AS timestamp_end,
			timestamp,
			device_id,
			site_id,
			event_day,
			voltage,
			current,
			input_voltage,
			temperature,
			voltage * current AS power,
			previous_voltage * previous_current AS previous_power,
			(timestamp::DOUBLE / 1000000000.0) - (previous_timestamp::DOUBLE / 1000000000.0) AS delta_seconds
		FROM lagged_data
		WHERE previous_timestamp IS NOT NULL AND timestamp IS NOT NULL
	),
	combined_data AS (
		SELECT
			device_id AS entity_id,
			site_id,
			event_day,
			TIME_BUCKET(INTERVAL '5 min', timestamp::TIMESTAMP) AS bucket_time,
			AVG(current) AS avg_current,
			AVG(current * voltage) AS avg_power,
			AVG(input_voltage) AS avg_input_voltage,
			AVG(voltage) AS avg_voltage,
			AVG(temperature) AS avg_temperature,
			SUM((power + previous_power) * delta_seconds / 2.0 / 3600.0) AS energy_kwh
		FROM power_calc
		GROUP BY entity_id, site_id, event_day, bucket_time
	),
	final_data AS (
		SELECT
			ROUND(avg_current, 2) AS current,
			ROUND(avg_power, 2) AS power,
			ROUND(avg_input_voltage, 2) AS input_voltage,
			ROUND(avg_voltage, 2) AS voltage,
			ROUND(avg_temperature, 2) AS temperature,
			energy_kwh,
			SUM(energy_kwh) OVER (PARTITION BY entity_id ORDER BY bucket_time) AS cumulative_energy_kwh,
			entity_id,
			site_id,
			event_day,
			STRFTIME(bucket_time, '%H:%M:%S') AS time_of_day,
			bucket_time AS timestamp
		FROM combined_data
	)
	SELECT *
	FROM final_data
	ORDER BY time_of_day
	PARTITION BY site_id, event_day`

	if _, err := ParseQueryToIncrementalDBSP(query); err != nil {
		t.Fatalf("ParseQueryToIncrementalDBSP failed: %v", err)
	}
}

func TestUserQueryExtractSelectClauses(t *testing.T) {
	query := `WITH lagged_data AS (
		SELECT timestamp FROM events
	),
	combined_data AS (
		SELECT AVG(v_out) AS v FROM lagged_data GROUP BY timestamp
	)
	SELECT * FROM combined_data`

	clauses, err := extractSelectClauses(query)
	if err != nil {
		t.Fatalf("extractSelectClauses failed: %v", err)
	}
	if len(clauses) == 0 {
		t.Fatalf("expected select clauses, got none")
	}
}
