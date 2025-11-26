package ir

import (
	"testing"

	"github.com/ariyn/dbsp/internal/dbsp/op"
	"github.com/ariyn/dbsp/internal/dbsp/types"
)

func TestLogicalProjectToDBSP(t *testing.T) {
	// Create a projection that selects only "name" and "age" columns
	proj := &LogicalProject{
		Columns: []string{"name", "age"},
		Input:   &LogicalScan{Table: "users"},
	}

	node, err := LogicalToDBSP(proj)
	if err != nil {
		t.Fatalf("LogicalToDBSP failed: %v", err)
	}

	mapOp, ok := node.Op.(*op.MapOp)
	if !ok {
		t.Fatalf("expected MapOp, got %T", node.Op)
	}

	// Test projection with a batch
	batch := types.Batch{
		{Tuple: types.Tuple{"name": "Alice", "age": 30, "city": "NYC"}, Count: 1},
		{Tuple: types.Tuple{"name": "Bob", "age": 25, "city": "LA", "country": "USA"}, Count: 1},
	}

	out, err := mapOp.Apply(batch)
	if err != nil {
		t.Fatalf("Apply failed: %v", err)
	}

	if len(out) != 2 {
		t.Fatalf("expected 2 output tuples, got %d", len(out))
	}

	// Check first tuple - should only have "name" and "age"
	if len(out[0].Tuple) != 2 {
		t.Errorf("expected 2 fields in projected tuple, got %d: %+v", len(out[0].Tuple), out[0].Tuple)
	}
	if out[0].Tuple["name"] != "Alice" {
		t.Errorf("expected name=Alice, got %v", out[0].Tuple["name"])
	}
	if out[0].Tuple["age"] != 30 {
		t.Errorf("expected age=30, got %v", out[0].Tuple["age"])
	}
	if _, exists := out[0].Tuple["city"]; exists {
		t.Error("city should not exist in projected tuple")
	}

	// Check second tuple
	if len(out[1].Tuple) != 2 {
		t.Errorf("expected 2 fields in projected tuple, got %d: %+v", len(out[1].Tuple), out[1].Tuple)
	}
	if out[1].Tuple["name"] != "Bob" {
		t.Errorf("expected name=Bob, got %v", out[1].Tuple["name"])
	}
	if out[1].Tuple["age"] != 25 {
		t.Errorf("expected age=25, got %v", out[1].Tuple["age"])
	}
	if _, exists := out[1].Tuple["country"]; exists {
		t.Error("country should not exist in projected tuple")
	}
}

func TestLogicalProjectMissingColumn(t *testing.T) {
	// Project columns that may not exist in all tuples
	proj := &LogicalProject{
		Columns: []string{"id", "status"},
		Input:   &LogicalScan{Table: "t"},
	}

	node, err := LogicalToDBSP(proj)
	if err != nil {
		t.Fatalf("LogicalToDBSP failed: %v", err)
	}

	mapOp := node.Op.(*op.MapOp)

	// Tuple with only "id" (missing "status")
	batch := types.Batch{
		{Tuple: types.Tuple{"id": 1, "name": "test"}, Count: 1},
	}

	out, err := mapOp.Apply(batch)
	if err != nil {
		t.Fatalf("Apply failed: %v", err)
	}

	// Should only have "id" in output (status is missing)
	if len(out[0].Tuple) != 1 {
		t.Errorf("expected 1 field (only id), got %d: %+v", len(out[0].Tuple), out[0].Tuple)
	}
	if out[0].Tuple["id"] != 1 {
		t.Errorf("expected id=1, got %v", out[0].Tuple["id"])
	}
}
