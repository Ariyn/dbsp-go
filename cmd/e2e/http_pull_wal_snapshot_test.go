package e2e

import (
	"bytes"
	"context"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/apache/arrow/go/v15/arrow/memory"
	"github.com/apache/arrow/go/v15/parquet/file"
	"github.com/apache/arrow/go/v15/parquet/pqarrow"

	"github.com/ariyn/dbsp/cmd/dbsp/config"
	"github.com/ariyn/dbsp/cmd/dbsp/pipeline"
	"github.com/ariyn/dbsp/cmd/dbsp/sink"
	"github.com/ariyn/dbsp/internal/dbsp/testutil"
	"github.com/ariyn/dbsp/internal/dbsp/types"
	"github.com/ariyn/dbsp/internal/dbsp/wal"
)

func TestHTTPPull_WALReplay_RebuildsSnapshot(t *testing.T) {
	ctx := context.Background()
	tmp := t.TempDir()
	spillDir := filepath.Join(tmp, "spill")
	walPath := filepath.Join(tmp, "wal.db")

	port := reservePort(t)
	path := "/snapshot"

	schema := &config.ParquetSchema{Columns: []config.ParquetColumn{
		{Name: "id", Type: "int64"},
		{Name: "value", Type: "float64"},
	}}

	cfg := config.HTTPPullSinkConfig{
		Port:          port,
		Path:          path,
		DiskSpillPath: spillDir,
	}

	batch := types.Batch{
		{Tuple: types.Tuple{"id": int64(1), "value": 10.5}, Count: 1},
		{Tuple: types.Tuple{"id": int64(2), "value": 20.0}, Count: 1},
	}

	execute := func(b types.Batch) (types.Batch, error) { return b, nil }

	sink1, err := sink.NewHTTPPullSink(cfg, nil, schema)
	if err != nil {
		t.Fatalf("NewHTTPPullSink(run1): %v", err)
	}

	waitForHTTPReady(t, port, path)

	wal1, err := wal.NewSQLiteWAL(walPath)
	if err != nil {
		t.Fatalf("NewSQLiteWAL(run1): %v", err)
	}
	if err := pipeline.RunPipeline(ctx, testutil.NewSliceSource([]types.Batch{batch}), sink1, execute, wal1, nil, 0); err != nil {
		_ = wal1.Close()
		t.Fatalf("RunPipeline(run1): %v", err)
	}
	if err := wal1.Close(); err != nil {
		t.Fatalf("Close WAL(run1): %v", err)
	}

	if _, err := os.Stat(walPath); err != nil {
		t.Fatalf("expected wal file to exist: %v", err)
	}

	spillFile := filepath.Join(spillDir, "default.parquet")
	if _, err := os.Stat(spillFile); err != nil {
		t.Fatalf("expected spill parquet to exist: %v", err)
	}

	rows := readParquetRowsFromURL(t, port, path)
	if rows != int64(len(batch)) {
		t.Fatalf("expected %d rows from http pull, got %d", len(batch), rows)
	}

	if err := sink1.Close(); err != nil {
		t.Fatalf("Close sink(run1): %v", err)
	}

	if err := os.Remove(spillFile); err != nil {
		t.Fatalf("remove spill parquet: %v", err)
	}

	port2 := reservePort(t)
	cfg2 := cfg
	cfg2.Port = port2

	sink2, err := sink.NewHTTPPullSink(cfg2, nil, schema)
	if err != nil {
		t.Fatalf("NewHTTPPullSink(run2): %v", err)
	}
	defer sink2.Close()

	waitForHTTPReady(t, port2, path)

	wal2, err := wal.NewSQLiteWAL(walPath)
	if err != nil {
		t.Fatalf("NewSQLiteWAL(run2): %v", err)
	}
	if err := pipeline.RunPipeline(ctx, testutil.NewSliceSource(nil), sink2, execute, wal2, nil, 0); err != nil {
		_ = wal2.Close()
		t.Fatalf("RunPipeline(run2): %v", err)
	}
	if err := wal2.Close(); err != nil {
		t.Fatalf("Close WAL(run2): %v", err)
	}

	if _, err := os.Stat(spillFile); err != nil {
		t.Fatalf("expected spill parquet after replay: %v", err)
	}

	rows = readParquetRowsFromURL(t, port2, path)
	if rows != int64(len(batch)) {
		t.Fatalf("expected %d rows after replay, got %d", len(batch), rows)
	}
}

func reservePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve port: %v", err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	if err := l.Close(); err != nil {
		t.Fatalf("close listener: %v", err)
	}
	return port
}

func waitForHTTPReady(t *testing.T, port int, path string) {
	t.Helper()
	url := "http://127.0.0.1:" + itoa(port) + path
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := http.Get(url)
		if err == nil {
			_ = resp.Body.Close()
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("http pull server not ready: %s", url)
}

func readParquetRowsFromURL(t *testing.T, port int, path string) int64 {
	t.Helper()
	url := "http://127.0.0.1:" + itoa(port) + path
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("http get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("unexpected status %d: %s", resp.StatusCode, string(body))
	}
	payload, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}

	pf, err := file.NewParquetReader(bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("parquet reader: %v", err)
	}

	reader, err := pqarrow.NewFileReader(pf, pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
	if err != nil {
		t.Fatalf("arrow reader: %v", err)
	}

	tbl, err := reader.ReadTable(context.Background())
	if err != nil {
		t.Fatalf("read table: %v", err)
	}
	defer tbl.Release()

	return tbl.NumRows()
}

func itoa(v int) string {
	if v == 0 {
		return "0"
	}
	buf := [20]byte{}
	i := len(buf)
	for v > 0 {
		i--
		buf[i] = byte('0' + v%10)
		v /= 10
	}
	return string(buf[i:])
}
