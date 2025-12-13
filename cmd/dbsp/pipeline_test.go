package main

import (
	"os"
	"testing"

	"github.com/ariyn/dbsp/internal/dbsp/types"
)

func TestCSVSource(t *testing.T) {
	// Create a temp CSV file
	content := `time_bucket,amount,product
10:00,1000,A
10:00,1500,B
10:05,3000,A`
	tmpfile, err := os.CreateTemp("", "test.csv")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpfile.Name()) // clean up

	if _, err := tmpfile.Write([]byte(content)); err != nil {
		t.Fatal(err)
	}
	if err := tmpfile.Close(); err != nil {
		t.Fatal(err)
	}

	config := map[string]interface{}{
		"path": tmpfile.Name(),
		"schema": map[string]string{
			"time_bucket": "string",
			"amount":      "float",
			"product":     "string",
		},
	}

	source, err := NewCSVSource(config)
	if err != nil {
		t.Fatalf("NewCSVSource failed: %v", err)
	}
	defer source.Close()

	batch, err := source.NextBatch()
	if err != nil {
		t.Fatalf("NextBatch failed: %v", err)
	}

	if len(batch) != 3 {
		t.Errorf("Expected 3 records, got %d", len(batch))
	}

	// Verify content
	checkTuple(t, batch[0], "10:00", 1000.0, "A")
	checkTuple(t, batch[1], "10:00", 1500.0, "B")
	checkTuple(t, batch[2], "10:05", 3000.0, "A")

	// Next batch should be nil
	batch, err = source.NextBatch()
	if err != nil {
		t.Fatalf("Second NextBatch failed: %v", err)
	}
	if batch != nil {
		t.Errorf("Expected nil batch, got %v", batch)
	}
}

func checkTuple(t *testing.T, td types.TupleDelta, time string, amount float64, product string) {
	if td.Tuple["time_bucket"] != time {
		t.Errorf("Expected time %s, got %v", time, td.Tuple["time_bucket"])
	}
	if td.Tuple["amount"] != amount {
		t.Errorf("Expected amount %f, got %v", amount, td.Tuple["amount"])
	}
	if td.Tuple["product"] != product {
		t.Errorf("Expected product %s, got %v", product, td.Tuple["product"])
	}
}
