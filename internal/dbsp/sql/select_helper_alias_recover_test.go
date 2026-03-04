package sqlconv

import "testing"

func TestExtractSelectExprByAlias_Timedelta(t *testing.T) {
	q := `SELECT
		timestamp,
		timestamp_last,
		(timestamp::DOUBLE / 1000000000.0) - (timestamp_last::DOUBLE / 1000000000.0) AS timedelta_second
	FROM lagged_data
	WHERE timestamp_last IS NOT NULL AND timestamp IS NOT NULL`

	expr, ok := extractSelectExprByAlias(q, "timedelta_second")
	if !ok {
		t.Fatalf("expected to recover expression for alias")
	}
	if expr == "" {
		t.Fatalf("expected non-empty expression")
	}
	if expr == "('(' - ')')" {
		t.Fatalf("unexpected malformed expression recovered: %q", expr)
	}
}
