package sqlconv

import "testing"

func TestFindAggregatesFromQuery_CTEInnerGroupBy(t *testing.T) {
	q := `WITH cte AS (
		SELECT entity_id AS id, SUM(metric_x + metric_y) AS total_value
		FROM events
		GROUP BY id
	)
	SELECT *
	FROM cte`

	aggs, err := findAggregatesFromQuery(q)
	if err != nil {
		t.Fatalf("findAggregatesFromQuery: %v", err)
	}
	if len(aggs) != 1 {
		t.Fatalf("expected 1 aggregate, got %d: %+v", len(aggs), aggs)
	}
	if aggs[0].Name != "SUM" {
		t.Fatalf("expected SUM aggregate, got %+v", aggs[0])
	}
	if aggs[0].Col != "metric_x + metric_y" {
		t.Fatalf("unexpected SUM arg: %q", aggs[0].Col)
	}
}
