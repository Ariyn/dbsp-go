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
