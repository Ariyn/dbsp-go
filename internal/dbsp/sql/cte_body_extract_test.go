package sqlconv

import "testing"

func TestExtractCTEBodyQueries_Stage2(t *testing.T) {
	query := `WITH stage1 AS (
		SELECT timestamp FROM events
	),
	stage2 AS (
		SELECT
			timestamp,
			previous_timestamp,
			(timestamp::DOUBLE / 1000000000.0) - (previous_timestamp::DOUBLE / 1000000000.0) AS delta_seconds
		FROM stage1
	)
	SELECT * FROM stage2`

	bodies := extractCTEBodyQueries(query)
	body := bodies["stage2"]
	if body == "" {
		t.Fatalf("expected non-empty stage2 body, got %#v", bodies)
	}
	if body == "('(' - ')')" {
		t.Fatalf("unexpected malformed body: %q", body)
	}
}
