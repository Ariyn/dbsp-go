package main

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/ariyn/dbsp/internal/dbsp/types"
)

func BenchmarkParquetSink_WriteBatch(b *testing.B) {
	dir := b.TempDir()
	prefix := filepath.Join(dir, "bench")

	schema := &ParquetSchema{Columns: []ParquetColumn{
		{Name: "k", Type: "string"},
		{Name: "agg_delta", Type: "float64"},
		{Name: "__count", Type: "int64"},
	}}

	cfg := map[string]interface{}{
		"path":           prefix,
		"compression":    "uncompressed",
		"row_group_size": 65536,
	}

	sink, err := NewParquetSink(cfg, schema)
	if err != nil {
		b.Fatalf("NewParquetSink: %v", err)
	}
	defer sink.Close()

	batch := make(types.Batch, 0, 1024)
	for i := 0; i < cap(batch); i++ {
		batch = append(batch, types.TupleDelta{
			Tuple: types.Tuple{"k": fmt.Sprintf("k%d", i%128), "agg_delta": float64(i % 17)},
			Count: 1,
		})
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := sink.WriteBatch(batch); err != nil {
			b.Fatalf("WriteBatch: %v", err)
		}
	}
}
