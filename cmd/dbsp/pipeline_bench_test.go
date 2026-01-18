package main

import (
	"path/filepath"
	"testing"

	"github.com/ariyn/dbsp/internal/dbsp/op"
	sqlconv "github.com/ariyn/dbsp/internal/dbsp/sql"
	"github.com/ariyn/dbsp/internal/dbsp/types"
)

func BenchmarkPipeline_ExecuteOnly_GroupAgg(b *testing.B) {
	query := "SELECT k, SUM(v) FROM t GROUP BY k"
	root, err := sqlconv.ParseQueryToIncrementalDBSP(query)
	if err != nil {
		b.Fatalf("ParseQueryToIncrementalDBSP: %v", err)
	}

	batch := make(types.Batch, 0, 4096)
	for i := 0; i < cap(batch); i++ {
		batch = append(batch, types.TupleDelta{Tuple: types.Tuple{"k": i % 1024, "v": float64(i % 17)}, Count: 1})
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := op.Execute(root, batch); err != nil {
			b.Fatalf("Execute: %v", err)
		}
	}
}

func BenchmarkPipeline_ExecutePlusParquetSink_GroupAgg(b *testing.B) {
	query := "SELECT k, SUM(v) FROM t GROUP BY k"
	root, err := sqlconv.ParseQueryToIncrementalDBSP(query)
	if err != nil {
		b.Fatalf("ParseQueryToIncrementalDBSP: %v", err)
	}

	schema := &ParquetSchema{Columns: []ParquetColumn{
		{Name: "k", Type: "int64"},
		{Name: "agg_delta", Type: "float64"},
		{Name: "__count", Type: "int64"},
	}}

	cfg := map[string]interface{}{
		"path":           filepath.Join(b.TempDir(), "pipe"),
		"compression":    "uncompressed",
		"row_group_size": 65536,
	}
	ps, err := NewParquetSink(cfg, schema)
	if err != nil {
		b.Fatalf("NewParquetSink: %v", err)
	}
	defer ps.Close()

	batch := make(types.Batch, 0, 2048)
	for i := 0; i < cap(batch); i++ {
		batch = append(batch, types.TupleDelta{Tuple: types.Tuple{"k": i % 512, "v": float64(i % 17)}, Count: 1})
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out, err := op.Execute(root, batch)
		if err != nil {
			b.Fatalf("Execute: %v", err)
		}
		if err := ps.WriteBatch(out); err != nil {
			b.Fatalf("WriteBatch: %v", err)
		}
	}
}

func BenchmarkPipeline_ExecuteOnly_JoinGroupAgg(b *testing.B) {
	query := "SELECT a.k, SUM(b.v), COUNT(*) FROM a JOIN b ON a.id = b.id GROUP BY a.k"
	root, err := sqlconv.ParseQueryToIncrementalDBSP(query)
	if err != nil {
		b.Fatalf("ParseQueryToIncrementalDBSP: %v", err)
	}

	// Pre-load stable right side state (b).
	rightBatch := make(types.Batch, 0, 1024)
	for i := 0; i < cap(rightBatch); i++ {
		rightBatch = append(rightBatch, types.TupleDelta{Tuple: types.Tuple{"b.id": int64(i), "b.v": float64(i % 17)}, Count: 1})
	}
	if _, err := op.ExecuteTick(root, map[string]types.Batch{"b": rightBatch}); err != nil {
		b.Fatalf("ExecuteTick(preload b): %v", err)
	}

	// Each iteration inserts and deletes the same left rows to keep state bounded.
	leftInsert := make(types.Batch, 0, 1024)
	leftDelete := make(types.Batch, 0, 1024)
	for i := 0; i < cap(leftInsert); i++ {
		td := types.TupleDelta{Tuple: types.Tuple{"a.id": int64(i), "a.k": int64(i % 64)}, Count: 1}
		leftInsert = append(leftInsert, td)
		td.Count = -1
		leftDelete = append(leftDelete, td)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := op.ExecuteTick(root, map[string]types.Batch{"a": leftInsert}); err != nil {
			b.Fatalf("ExecuteTick(a insert): %v", err)
		}
		if _, err := op.ExecuteTick(root, map[string]types.Batch{"a": leftDelete}); err != nil {
			b.Fatalf("ExecuteTick(a delete): %v", err)
		}
	}
}
