package sqlconv

import "testing"

func TestFindAggregatesFromQuery_JoinMultiAgg(t *testing.T) {
	q := "SELECT a.k AS k, SUM(b.v), COUNT(b.id) FROM a JOIN b ON a.id = b.id GROUP BY a.k"
	aggs, err := findAggregatesFromQuery(q)
	if err != nil {
		t.Fatalf("findAggregatesFromQuery: %v", err)
	}
	if len(aggs) != 2 {
		t.Fatalf("expected 2 aggregates, got %d: %+v", len(aggs), aggs)
	}
	if aggs[0].Name != "SUM" || aggs[0].Col != "b.v" {
		t.Fatalf("unexpected agg[0]: %+v", aggs[0])
	}
	if aggs[1].Name != "COUNT" || aggs[1].Col != "b.id" {
		t.Fatalf("unexpected agg[1]: %+v", aggs[1])
	}
}
