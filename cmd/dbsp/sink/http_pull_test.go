package sink

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow/go/v15/arrow/array"
	"github.com/apache/arrow/go/v15/arrow/memory"
	"github.com/apache/arrow/go/v15/parquet/file"
	"github.com/apache/arrow/go/v15/parquet/pqarrow"

	"github.com/ariyn/dbsp/cmd/dbsp/config"
	"github.com/ariyn/dbsp/internal/dbsp/types"
)

func TestHTTPPullSink_E2E(t *testing.T) {
	// 1. Setup Sink
	port := 9091
	path := "/snapshot"
	cfg := map[string]interface{}{
		"port": port,
		"path": path,
	}
	partitionBy := []string{"category"}
	schema := &config.ParquetSchema{
		Columns: []config.ParquetColumn{
			{Name: "id", Type: "int64"},
			{Name: "category", Type: "string"},
			{Name: "value", Type: "float64"},
		},
	}

	s, err := NewHTTPPullSink(cfg, partitionBy, schema)
	if err != nil {
		t.Fatalf("failed to create sink: %v", err)
	}
	defer s.Close()

	// 2. Ingest Data
	batch1 := types.Batch{
		{Tuple: types.Tuple{"id": int64(1), "category": "A", "value": 10.5}, Count: 1},
		{Tuple: types.Tuple{"id": int64(2), "category": "B", "value": 20.0}, Count: 1},
		{Tuple: types.Tuple{"id": int64(3), "category": "A", "value": 5.0}, Count: 1},
	}
	if err := s.WriteBatch(batch1); err != nil {
		t.Fatalf("WriteBatch failed: %v", err)
	}

	// Wait for server to start (already started in goroutine)
	time.Sleep(100 * time.Millisecond)

	// 3. Request Partition A
	resp, err := http.Get(fmt.Sprintf("http://localhost:%d%s?category=A", port, path))
	if err != nil {
		t.Fatalf("GET failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected status OK, got %v", resp.Status)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("io.ReadAll failed: %v", err)
	}

	// 4. Verify Parquet Content
	pf, err := file.NewParquetReader(bytes.NewReader(body))
	if err != nil {
		t.Fatalf("failed to open parquet reader: %v", err)
	}

	reader, err := pqarrow.NewFileReader(pf, pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
	if err != nil {
		t.Fatalf("failed to create arrow reader: %v", err)
	}

	tbl, err := reader.ReadTable(context.Background())
	if err != nil {
		t.Fatalf("ReadTable failed: %v", err)
	}
	defer tbl.Release()

	if tbl.NumRows() != 2 {
		t.Errorf("expected 2 rows for partition A, got %d", tbl.NumRows())
	}

	// Check if categories are all "A"
	colIdx := -1
	for i, f := range tbl.Schema().Fields() {
		if f.Name == "category" {
			colIdx = i
			break
		}
	}
	if colIdx == -1 {
		t.Fatal("category column not found in result")
	}

	catCol := tbl.Column(colIdx)
	for i := 0; i < int(tbl.NumRows()); i++ {
		// Column may be chunked
		chunk := catCol.Data().Chunk(0) // with small rows it's likely one chunk
		strArr := chunk.(*array.String)
		if strArr.Value(i) != "A" {
			t.Errorf("row %d: expected category A, got %s", i, strArr.Value(i))
		}
	}

	// 5. Request Non-existent Partition
	resp2, err := http.Get(fmt.Sprintf("http://localhost:%d%s?category=C", port, path))
	if err != nil {
		t.Fatalf("GET failed: %v", err)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusNotFound {
		t.Errorf("expected 404 for unknown partition, got %v", resp2.Status)
	}
}

func TestHTTPPullSink_DiskSpill(t *testing.T) {
	// Setup with spill path
	tmpDir, err := os.MkdirTemp("", "dbsp-spill-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	port := 9092
	path := "/snapshot"
	cfg := map[string]interface{}{
		"port":            port,
		"path":            path,
		"disk_spill_path": tmpDir,
	}

	partitionBy := []string{"id"}
	schema := &config.ParquetSchema{
		Columns: []config.ParquetColumn{
			{Name: "id", Type: "int64"},
			{Name: "name", Type: "string"},
		},
	}

	s, err := NewHTTPPullSink(cfg, partitionBy, schema)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	// 1. Write Data
	batch := types.Batch{
		{Tuple: types.Tuple{"id": int64(10), "name": "user10"}, Count: 1},
	}
	if err := s.WriteBatch(batch); err != nil {
		t.Fatal(err)
	}

	// 2. Verify File Creation (filename is just "10.parquet" for id=10)
	expectedFile := filepath.Join(tmpDir, "10.parquet")
	if _, err := os.Stat(expectedFile); os.IsNotExist(err) {
		t.Errorf("expected file %s was not created", expectedFile)
	}

	// 3. Update (Delete and Add)
	batch2 := types.Batch{
		{Tuple: types.Tuple{"id": int64(10), "name": "user10"}, Count: -1},
		{Tuple: types.Tuple{"id": int64(10), "name": "user10-updated"}, Count: 1},
	}
	if err := s.WriteBatch(batch2); err != nil {
		t.Fatal(err)
	}

	// 4. Check if file content changed (just checking existence and size for now)
	info, err := os.Stat(expectedFile)
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() == 0 {
		t.Errorf("file %s is empty", expectedFile)
	}
}

func TestHTTPPullSink_CompositeKey(t *testing.T) {
	port := 9093
	path := "/snapshot"
	cfg := map[string]interface{}{
		"port": port,
		"path": path,
	}

	partitionBy := []string{"tenant_id", "region"}
	schema := &config.ParquetSchema{
		Columns: []config.ParquetColumn{
			{Name: "tenant_id", Type: "int64"},
			{Name: "region", Type: "string"},
			{Name: "data", Type: "string"},
		},
	}

	s, err := NewHTTPPullSink(cfg, partitionBy, schema)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	// 1. Write Data
	batch := types.Batch{
		{Tuple: types.Tuple{"tenant_id": int64(1), "region": "US", "data": "D1"}, Count: 1},
		{Tuple: types.Tuple{"tenant_id": int64(1), "region": "EU", "data": "D2"}, Count: 1},
	}
	if err := s.WriteBatch(batch); err != nil {
		t.Fatal(err)
	}

	time.Sleep(100 * time.Millisecond)

	// 2. Request Composite Partition (tenant_id=1, region=US)
	query := "?tenant_id=1&region=US"
	resp, err := http.Get(fmt.Sprintf("http://localhost:%d%s%s", port, path, query))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected status OK, got %v", resp.Status)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}

	pf, err := file.NewParquetReader(bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	reader, err := pqarrow.NewFileReader(pf, pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
	if err != nil {
		t.Fatal(err)
	}

	tbl, err := reader.ReadTable(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer tbl.Release()

	if tbl.NumRows() != 1 {
		t.Errorf("expected 1 row, got %d", tbl.NumRows())
	}
}

func TestHTTPPullSink_EmptyAfterDelete(t *testing.T) {
	port := 9094
	path := "/snapshot"
	cfg := map[string]interface{}{
		"port": port,
		"path": path,
	}

	partitionBy := []string{"category"}
	schema := &config.ParquetSchema{
		Columns: []config.ParquetColumn{{Name: "id", Type: "int64"}, {Name: "category", Type: "string"}},
	}

	s, err := NewHTTPPullSink(cfg, partitionBy, schema)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	// 1. Write and Then Delete Everything in Category A
	batch := types.Batch{
		{Tuple: types.Tuple{"id": int64(100), "category": "A"}, Count: 1},
	}
	s.WriteBatch(batch)

	batchDel := types.Batch{
		{Tuple: types.Tuple{"id": int64(100), "category": "A"}, Count: -1},
	}
	s.WriteBatch(batchDel)

	time.Sleep(100 * time.Millisecond)

	// 2. Request Partition A
	resp, err := http.Get(fmt.Sprintf("http://localhost:%d%s?category=A", port, path))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent {
		t.Errorf("expected StatusNoContent (204) after full delete, got %v", resp.Status)
	}
}

func TestHTTPPullSink_FailsFastOnPortConflict(t *testing.T) {
	port := 9095
	path := "/snapshot"
	cfg := map[string]interface{}{
		"port": port,
		"path": path,
	}

	schema := &config.ParquetSchema{
		Columns: []config.ParquetColumn{{Name: "id", Type: "int64"}},
	}

	s1, err := NewHTTPPullSink(cfg, []string{"id"}, schema)
	if err != nil {
		t.Fatalf("failed to create first sink: %v", err)
	}
	defer s1.Close()

	s2, err := NewHTTPPullSink(cfg, []string{"id"}, schema)
	if err == nil {
		if s2 != nil {
			_ = s2.Close()
		}
		t.Fatal("expected port binding error, got nil")
	}
	if !strings.Contains(err.Error(), "address already in use") {
		t.Fatalf("expected address-in-use error, got: %v", err)
	}
}
