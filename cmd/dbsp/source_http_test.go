package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/ariyn/dbsp/internal/dbsp/types"
)

func TestHTTPSource(t *testing.T) {
	config := map[string]interface{}{
		"port": 8081, // Use different port for testing
		"path": "/test",
		"schema": map[string]string{
			"id":    "int",
			"value": "float",
		},
	}

	source, err := NewHTTPSource(config)
	if err != nil {
		t.Fatalf("NewHTTPSource failed: %v", err)
	}
	defer source.Close()

	// Give server time to start
	time.Sleep(100 * time.Millisecond)

	// Send data
	data := []map[string]interface{}{
		{"id": 1, "value": 10.5},
		{"id": 2, "value": 20.0},
	}
	jsonData, _ := json.Marshal(data)

	resp, err := http.Post("http://localhost:8081/test", "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		t.Fatalf("HTTP Post failed: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected status 200, got %d", resp.StatusCode)
	}

	// Read batch
	batch, err := source.NextBatch()
	if err != nil {
		t.Fatalf("NextBatch failed: %v", err)
	}

	if len(batch) != 2 {
		t.Errorf("Expected 2 records, got %d", len(batch))
	}

	checkTupleGeneric(t, batch[0], "id", 1, "value", 10.5)
	checkTupleGeneric(t, batch[1], "id", 2, "value", 20.0)
}

func checkTupleGeneric(t *testing.T, td types.TupleDelta, k1 string, v1 interface{}, k2 string, v2 interface{}) {
	if td.Tuple[k1] != v1 {
		t.Errorf("Expected %s=%v, got %v", k1, v1, td.Tuple[k1])
	}
	if td.Tuple[k2] != v2 {
		t.Errorf("Expected %s=%v, got %v", k2, v2, td.Tuple[k2])
	}
}
