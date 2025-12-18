package main

import (
	"encoding/csv"
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/ariyn/dbsp/internal/dbsp/types"
)

func TestFileSink_CSV(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "test_sink_*.csv")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpFile.Name())
	tmpFile.Close()

	config := map[string]interface{}{
		"path":   tmpFile.Name(),
		"format": "csv",
	}

	sink, err := NewFileSink(config)
	if err != nil {
		t.Fatalf("NewFileSink failed: %v", err)
	}
	defer sink.Close()

	batch := types.Batch{
		{
			Tuple: types.Tuple{"id": 1, "name": "Alice"},
			Count: 1,
		},
		{
			Tuple: types.Tuple{"id": 2, "name": "Bob"},
			Count: -1,
		},
	}

	if err := sink.WriteBatch(batch[:1]); err != nil {
		t.Fatalf("WriteBatch failed: %v", err)
	}
	if err := sink.WriteBatch(batch[1:]); err != nil {
		t.Fatalf("WriteBatch failed: %v", err)
	}

	content, err := os.ReadFile(tmpFile.Name())
	if err != nil {
		t.Fatal(err)
	}

	r := csv.NewReader(strings.NewReader(string(content)))
	records, err := r.ReadAll()
	if err != nil {
		t.Fatalf("failed to read csv: %v", err)
	}
	if len(records) != 3 {
		t.Fatalf("expected 3 records (header+2 rows), got %d", len(records))
	}
	if got, want := strings.Join(records[0], ","), "id,name,__count"; got != want {
		t.Fatalf("header mismatch: got %q want %q", got, want)
	}
	if got, want := strings.Join(records[1], ","), "1,Alice,1"; got != want {
		t.Fatalf("row1 mismatch: got %q want %q", got, want)
	}
	if got, want := strings.Join(records[2], ","), "2,Bob,-1"; got != want {
		t.Fatalf("row2 mismatch: got %q want %q", got, want)
	}
}

func TestFileSink_JSON(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "test_sink_*.json")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpFile.Name())
	tmpFile.Close()

	config := map[string]interface{}{
		"path":   tmpFile.Name(),
		"format": "json",
	}

	sink, err := NewFileSink(config)
	if err != nil {
		t.Fatalf("NewFileSink failed: %v", err)
	}
	defer sink.Close()

	batch := types.Batch{
		{
			Tuple: types.Tuple{"id": 1, "name": "Alice"},
			Count: 1,
		},
	}

	if err := sink.WriteBatch(batch); err != nil {
		t.Fatalf("WriteBatch failed: %v", err)
	}

	content, err := os.ReadFile(tmpFile.Name())
	if err != nil {
		t.Fatal(err)
	}

	lines := strings.Split(strings.TrimSpace(string(content)), "\n")
	if len(lines) != 1 {
		t.Fatalf("expected 1 json line, got %d", len(lines))
	}
	var td types.TupleDelta
	if err := json.Unmarshal([]byte(lines[0]), &td); err != nil {
		t.Fatalf("failed to unmarshal json line: %v", err)
	}
	if td.Tuple["name"] != "Alice" {
		t.Fatalf("expected Alice, got %v", td.Tuple["name"])
	}
}
