package sqlconv

import "testing"

func TestFindAggregatesFromQuery_NestedAggregateExpressions(t *testing.T) {
	q := `SELECT
		entity_id AS id,
		ROUND(AVG(metric_y), 2) AS avg_metric_y,
		SUM(score + previous_score) * delta_seconds / 2.0 / 3600.0 AS weighted_score
	FROM stage2
	GROUP BY id`

	aggs, err := findAggregatesFromQuery(q)
	if err != nil {
		t.Fatalf("findAggregatesFromQuery: %v", err)
	}
	if len(aggs) != 2 {
		t.Fatalf("expected 2 aggregates, got %d: %+v", len(aggs), aggs)
	}
	if aggs[0].Name != "AVG" || aggs[0].Col != "metric_y" {
		t.Fatalf("unexpected agg[0]: %+v", aggs[0])
	}
	if aggs[1].Name != "SUM" || aggs[1].Col != "score + previous_score" {
		t.Fatalf("unexpected agg[1]: %+v", aggs[1])
	}
}
