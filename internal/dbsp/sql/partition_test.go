package sqlconv

import (
	"reflect"
	"testing"
)

func TestExtractGlobalPartitionBy(t *testing.T) {
	tests := []struct {
		name       string
		query      string
		expectRel  string
		expectPart []string
	}{
		{
			name:       "Simple select with partition",
			query:      "SELECT * FROM table PARTITION BY col1",
			expectRel:  "SELECT * FROM table",
			expectPart: []string{"col1"},
		},
		{
			name:       "Select with group by and multiple partitions",
			query:      "SELECT a, b, SUM(c) FROM t GROUP BY a, b PARTITION BY a, b",
			expectRel:  "SELECT a, b, SUM(c) FROM t GROUP BY a, b",
			expectPart: []string{"a", "b"},
		},
		{
			name:       "Partitions with spaces",
			query:      "SELECT * FROM t PARTITION BY  col1 ,  col2 ",
			expectRel:  "SELECT * FROM t",
			expectPart: []string{"col1", "col2"},
		},
		{
			name:       "Case insensitivity",
			query:      "SELECT col1 from table partition by col1",
			expectRel:  "SELECT col1 from table",
			expectPart: []string{"col1"},
		},
		{
			name:       "Window function NOT confused with global partition",
			query:      "SELECT SUM(v) OVER (PARTITION BY id) FROM t",
			expectRel:  "SELECT SUM(v) OVER (PARTITION BY id) FROM t",
			expectPart: nil,
		},
		{
			name:       "Window function AND global partition",
			query:      "SELECT SUM(v) OVER (PARTITION BY id) FROM t PARTITION BY category",
			expectRel:  "SELECT SUM(v) OVER (PARTITION BY id) FROM t",
			expectPart: []string{"category"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rel, part, err := extractGlobalPartitionBy(tt.query)
			if err != nil {
				t.Errorf("extractGlobalPartitionBy error: %v", err)
			}
			if rel != tt.expectRel {
				t.Errorf("expected relation [%s], got [%s]", tt.expectRel, rel)
			}
			if !reflect.DeepEqual(part, tt.expectPart) {
				t.Errorf("expected partition [%v], got [%v]", tt.expectPart, part)
			}
		})
	}
}

func TestParseQueryToDBSP_PartitionBy(t *testing.T) {
	query := "SELECT id, SUM(v) FROM t GROUP BY id PARTITION BY id"
	root, err := ParseQueryToDBSP(query)
	if err != nil {
		t.Fatalf("ParseQueryToDBSP failed: %v", err)
	}

	expect := []string{"id"}
	if !reflect.DeepEqual(root.PartitionBy, expect) {
		t.Errorf("expected PartitionBy %v, got %v", expect, root.PartitionBy)
	}
}
