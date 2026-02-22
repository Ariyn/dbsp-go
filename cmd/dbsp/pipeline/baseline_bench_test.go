package pipeline

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/ariyn/dbsp/cmd/dbsp/config"
	"github.com/ariyn/dbsp/cmd/dbsp/sink"
	"github.com/ariyn/dbsp/cmd/dbsp/source"
	"github.com/ariyn/dbsp/internal/dbsp/op"
	sqlconv "github.com/ariyn/dbsp/internal/dbsp/sql"
	"github.com/ariyn/dbsp/internal/dbsp/types"
)

func BenchmarkBaseline_GroupAgg(b *testing.B) {
	query := "SELECT product, SUM(amount), COUNT(*) FROM input GROUP BY product"
	root, err := sqlconv.ParseQueryToIncrementalDBSP(query)
	if err != nil {
		b.Fatalf("ParseQueryToIncrementalDBSP: %v", err)
	}

	// Working directory should be repo root for this to work with benchmarks/data
	// But in tests, it might be cmd/dbsp. Let's find project root.
	wd, _ := os.Getwd()
	for !exists(filepath.Join(wd, "go.mod")) && wd != "/" {
		wd = filepath.Dir(wd)
	}
	dataPath := filepath.Join(wd, "benchmarks", "data", "bench_t.csv")

	sourceCfg := map[string]interface{}{
		"path": dataPath,
		"schema": map[string]interface{}{
			"product": "string",
			"amount":  "float",
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		source, err := source.NewCSVSource(sourceCfg)
		if err != nil {
			b.Fatalf("source.NewCSVSource: %v", err)
		}

		batch, err := source.NextBatch()
		if err != nil {
			b.Fatalf("NextBatch: %v", err)
		}
		
		// Note: We use op.Execute for the root node, which is already differentiated.
		if _, err := op.Execute(root, batch); err != nil {
			b.Fatalf("Execute: %v", err)
		}
		source.Close()
	}
}

func BenchmarkBaseline_JoinGroupAgg(b *testing.B) {
	query := "SELECT a.category, SUM(b.amount) FROM a JOIN b ON a.id = b.a_id GROUP BY a.category"
	root, err := sqlconv.ParseQueryToIncrementalDBSP(query)
	if err != nil {
		b.Fatalf("ParseQueryToIncrementalDBSP: %v", err)
	}

	wd, _ := os.Getwd()
	for !exists(filepath.Join(wd, "go.mod")) && wd != "/" {
		wd = filepath.Dir(wd)
	}
	dataAPath := filepath.Join(wd, "benchmarks", "data", "bench_a.csv")
	dataBPath := filepath.Join(wd, "benchmarks", "data", "bench_b.csv")

	sourceACfg := map[string]interface{}{
		"path": dataAPath,
		"schema": map[string]interface{}{
			"id":       "int",
			"category": "string",
		},
	}
	sourceBCfg := map[string]interface{}{
		"path": dataBPath,
		"schema": map[string]interface{}{
			"a_id":   "int",
			"amount": "float",
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// We re-initialize the root to clear state between runs for a cleaner baseline, 
		// OR we can choose to measure incremental updates.
		// For a "baseline" of a single batch, we re-compile or re-init operators.
		root, _ = sqlconv.ParseQueryToIncrementalDBSP(query)

		sourceA, _ := source.NewCSVSource(sourceACfg)
		sourceB, _ := source.NewCSVSource(sourceBCfg)

		batchA, _ := sourceA.NextBatch()
		batchB, _ := sourceB.NextBatch()

		if _, err := op.ExecuteTick(root, map[string]types.Batch{"a": batchA}); err != nil {
			b.Fatalf("ExecuteTick(a): %v", err)
		}
		if _, err := op.ExecuteTick(root, map[string]types.Batch{"b": batchB}); err != nil {
			b.Fatalf("ExecuteTick(b): %v", err)
		}
		
		sourceA.Close()
		sourceB.Close()
	}
}

func BenchmarkBaseline_E2E_Parquet(b *testing.B) {
	query := "SELECT product, SUM(amount) FROM input GROUP BY product"
	root, err := sqlconv.ParseQueryToIncrementalDBSP(query)
	if err != nil {
		b.Fatalf("ParseQueryToIncrementalDBSP: %v", err)
	}

	wd, _ := os.Getwd()
	for !exists(filepath.Join(wd, "go.mod")) && wd != "/" {
		wd = filepath.Dir(wd)
	}
	dataPath := filepath.Join(wd, "benchmarks", "data", "bench_t.csv")

	sourceCfg := map[string]interface{}{
		"path": dataPath,
		"schema": map[string]interface{}{
			"product": "string",
			"amount":  "float",
		},
	}

	schema := &config.ParquetSchema{Columns: []config.ParquetColumn{
		{Name: "product", Type: "string"},
		{Name: "agg_delta", Type: "float64"},
	}}

	tmpDir := b.TempDir()
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sinkCfg := map[string]interface{}{
			"path": filepath.Join(tmpDir, "out"),
		}
		sink, _ := sink.NewParquetSink(sinkCfg, schema)

		source, _ := source.NewCSVSource(sourceCfg)
		batch, _ := source.NextBatch()

		out, _ := op.Execute(root, batch)
		_ = sink.WriteBatch(out)

		sink.Close()
		source.Close()
	}
}

func exists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}
