package sink

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/apache/arrow/go/v15/arrow/memory"
	"github.com/apache/arrow/go/v15/parquet/file"
	"github.com/apache/arrow/go/v15/parquet/pqarrow"

	"github.com/ariyn/dbsp/cmd/dbsp/config"
	"github.com/ariyn/dbsp/internal/dbsp/types"
)

func TestParquetSink_WritesAndRotatesByBatches(t *testing.T) {
	dir := t.TempDir()
	prefix := filepath.Join(dir, "out")

	schema := &config.ParquetSchema{Columns: []config.ParquetColumn{
		{Name: "k", Type: "string"},
		{Name: "agg_delta", Type: "float64"},
		{Name: "__count", Type: "int64"},
	}}

	cfg := config.ParquetSinkConfig{
		Path:               prefix,
		Compression:        "uncompressed",
		RowGroupSize:       2,
		RotateEveryBatches: 1,
	}

	ps, err := NewParquetSink(cfg, schema)
	if err != nil {
		t.Fatalf("NewParquetSink: %v", err)
	}

	b1 := types.Batch{
		{Tuple: types.Tuple{"k": "a", "agg_delta": 1.5}, Count: 1},
		{Tuple: types.Tuple{"k": "b", "agg_delta": 2.0}, Count: 1},
	}
	b2 := types.Batch{
		{Tuple: types.Tuple{"k": "a", "agg_delta": -0.5}, Count: 1},
		{Tuple: types.Tuple{"k": "c", "agg_delta": 3.25}, Count: 1},
	}

	if err := ps.WriteBatch(b1); err != nil {
		t.Fatalf("WriteBatch(b1): %v", err)
	}
	if err := ps.WriteBatch(b2); err != nil {
		t.Fatalf("WriteBatch(b2): %v", err)
	}
	if err := ps.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	ents, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	var files []string
	for _, e := range ents {
		if e.IsDir() {
			continue
		}
		if filepath.Ext(e.Name()) != ".parquet" {
			continue
		}
		files = append(files, filepath.Join(dir, e.Name()))
	}
	if len(files) != 2 {
		t.Fatalf("expected 2 parquet files, got %d: %v", len(files), files)
	}
	// Sort for determinism.
	sort.Strings(files)

	rows0 := readParquetRows(t, files[0])
	rows1 := readParquetRows(t, files[1])
	if rows0 != int64(len(b1)) {
		t.Fatalf("expected file0 rows=%d, got %d", len(b1), rows0)
	}
	if rows1 != int64(len(b2)) {
		t.Fatalf("expected file1 rows=%d, got %d", len(b2), rows1)
	}
}

func TestInferOrLoadParquetSchema_CachesAndReuses(t *testing.T) {
	dir := t.TempDir()
	cache := filepath.Join(dir, "schema.json")

	query := "SELECT k, SUM(v) FROM t GROUP BY k"
	src := config.SourceConfig{Type: "csv", Config: map[string]interface{}{"path": "ignored.csv", "schema": map[string]string{"k": "string", "v": "float"}}}
	sinkCfg := map[string]interface{}{"path": filepath.Join(dir, "out"), "schema_cache_path": cache}

	s1, err := config.InferOrLoadParquetSchema(query, src, sinkCfg)
	if err != nil {
		t.Fatalf("config.InferOrLoadParquetSchema (first): %v", err)
	}
	if _, err := os.Stat(cache); err != nil {
		t.Fatalf("expected schema cache file to exist: %v", err)
	}

	s2, err := config.InferOrLoadParquetSchema(query, src, sinkCfg)
	if err != nil {
		t.Fatalf("config.InferOrLoadParquetSchema (second): %v", err)
	}
	if len(s1.Columns) == 0 || len(s2.Columns) == 0 {
		t.Fatalf("expected non-empty schema")
	}

	// Must include group key + agg delta + __count.
	cols := map[string]string{}
	for _, c := range s2.Columns {
		cols[c.Name] = c.Type
	}
	if cols["k"] != "string" {
		t.Fatalf("expected k=string, got %q", cols["k"])
	}
	if cols["agg_delta"] != "float64" {
		t.Fatalf("expected agg_delta=float64, got %q", cols["agg_delta"])
	}
	if cols["__count"] != "int64" {
		t.Fatalf("expected __count=int64, got %q", cols["__count"])
	}
}

func TestInferOrLoadParquetSchema_DefaultCacheUsesTempForRootPath(t *testing.T) {
	query := "SELECT k, SUM(v) FROM t GROUP BY k"
	src := config.SourceConfig{Type: "csv", Config: map[string]interface{}{"path": "ignored.csv", "schema": map[string]string{"k": "string", "v": "float"}}}
	sinkCfg := map[string]interface{}{"path": filepath.Join(string(os.PathSeparator), "pull")}

	_, err := config.InferOrLoadParquetSchema(query, src, sinkCfg)
	if err != nil {
		t.Fatalf("config.InferOrLoadParquetSchema: %v", err)
	}

	expected := filepath.Join(os.TempDir(), "pull.schema.json")
	if _, err := os.Stat(expected); err != nil {
		t.Fatalf("expected schema cache file in temp dir: %v", err)
	}
	_ = os.Remove(expected)
}

func TestInferOrLoadParquetSchema_ProjectUsesSourceSchemaHints(t *testing.T) {
	query := "SELECT id, name FROM t"
	src := config.SourceConfig{Type: "http", Config: map[string]interface{}{"schema": map[string]string{"id": "int", "name": "string"}}}
	sinkCfg := map[string]interface{}{"path": "out"}

	s, err := config.InferOrLoadParquetSchema(query, src, sinkCfg)
	if err != nil {
		t.Fatalf("config.InferOrLoadParquetSchema: %v", err)
	}

	cols := map[string]string{}
	for _, c := range s.Columns {
		cols[c.Name] = c.Type
	}
	if cols["id"] != "int64" {
		t.Fatalf("expected id=int64, got %q", cols["id"])
	}
	if cols["name"] != "string" {
		t.Fatalf("expected name=string, got %q", cols["name"])
	}
	if cols["__count"] != "int64" {
		t.Fatalf("expected __count=int64, got %q", cols["__count"])
	}
}

func readParquetRows(t *testing.T, path string) int64 {
	t.Helper()
	rdr, err := file.OpenParquetFile(path, false)
	if err != nil {
		t.Fatalf("OpenParquetFile(%s): %v", path, err)
	}
	defer rdr.Close()

	fr, err := pqarrow.NewFileReader(rdr, pqarrow.ArrowReadProperties{BatchSize: 1024}, memory.NewGoAllocator())
	if err != nil {
		t.Fatalf("NewFileReader(%s): %v", path, err)
	}

	rr, err := fr.GetRecordReader(context.Background(), nil, nil)
	if err != nil {
		t.Fatalf("GetRecordReader(%s): %v", path, err)
	}
	defer rr.Release()

	var rows int64
	for rr.Next() {
		rec := rr.Record()
		rows += rec.NumRows()
	}
	if err := rr.Err(); err != nil && !errors.Is(err, io.EOF) {
		t.Fatalf("RecordReader.Err(%s): %v", path, err)
	}
	return rows
}
