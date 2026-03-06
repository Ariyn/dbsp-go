package sink

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ariyn/dbsp/cmd/dbsp/config"
	"github.com/ariyn/dbsp/internal/dbsp/types"
)

func TestHTTPPullSinkWriteBatchGroupsByPartition(t *testing.T) {
	schema := &config.ParquetSchema{Columns: []config.ParquetColumn{
		{Name: "plant_id", Type: "string"},
		{Name: "local_date", Type: "string"},
		{Name: "id", Type: "string"},
		{Name: "energy", Type: "float64"},
	}}

	s, err := NewHTTPPullSink(config.HTTPPullSinkConfig{Port: 0, Path: "/pull"}, []string{"plant_id", "local_date"}, schema)
	if err != nil {
		t.Fatalf("NewHTTPPullSink: %v", err)
	}
	defer s.Close()

	batch := types.Batch{
		{Tuple: types.Tuple{"plant_id": "p1", "local_date": "2026-03-06", "id": "a", "energy": 1.0}, Count: 1},
		{Tuple: types.Tuple{"plant_id": "p1", "local_date": "2026-03-06", "id": "b", "energy": 2.0}, Count: 1},
		{Tuple: types.Tuple{"plant_id": "p2", "local_date": "2026-03-06", "id": "c", "energy": 3.0}, Count: 1},
	}

	if err := s.WriteBatch(batch); err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}

	if got := len(s.partitions); got != 2 {
		t.Fatalf("expected 2 partitions, got %d", got)
	}

	pk1 := "plant_id=p1/local_date=2026-03-06"
	entry1 := s.partitions[pk1]
	if entry1 == nil {
		t.Fatalf("expected partition %s", pk1)
	}
	rows1 := entry1.store.ToBatch()
	if len(rows1) != 2 {
		t.Fatalf("expected 2 rows in first partition, got %d (%v)", len(rows1), rows1)
	}

	pk2 := "plant_id=p2/local_date=2026-03-06"
	entry2 := s.partitions[pk2]
	if entry2 == nil {
		t.Fatalf("expected partition %s", pk2)
	}
	rows2 := entry2.store.ToBatch()
	if len(rows2) != 1 {
		t.Fatalf("expected 1 row in second partition, got %d (%v)", len(rows2), rows2)
	}
}

func TestHTTPPullSinkHandlePullUsesOrderedPartitionKey(t *testing.T) {
	schema := &config.ParquetSchema{Columns: []config.ParquetColumn{{Name: "plant_id", Type: "string"}, {Name: "local_date", Type: "string"}, {Name: "id", Type: "string"}}}

	s, err := NewHTTPPullSink(config.HTTPPullSinkConfig{Port: 0, Path: "/pull"}, []string{"plant_id", "local_date"}, schema)
	if err != nil {
		t.Fatalf("NewHTTPPullSink: %v", err)
	}
	defer s.Close()

	if got := s.getPartitionKey(types.Tuple{"plant_id": "p1", "local_date": "2026-03-06"}); got != "plant_id=p1/local_date=2026-03-06" {
		t.Fatalf("unexpected partition key: %s", got)
	}
}

func TestHTTPPullSinkCachesSnapshotUntilWriteInvalidates(t *testing.T) {
	schema := &config.ParquetSchema{Columns: []config.ParquetColumn{{Name: "plant_id", Type: "string"}, {Name: "local_date", Type: "string"}, {Name: "id", Type: "string"}}}
	s, err := NewHTTPPullSink(config.HTTPPullSinkConfig{Port: 0, Path: "/pull"}, []string{"plant_id", "local_date"}, schema)
	if err != nil {
		t.Fatalf("NewHTTPPullSink: %v", err)
	}
	defer s.Close()

	if err := s.WriteBatch(types.Batch{{Tuple: types.Tuple{"plant_id": "p1", "local_date": "2026-03-06", "id": "a"}, Count: 1}}); err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}
	pk := "plant_id=p1/local_date=2026-03-06"
	entry := s.partitions[pk]
	if entry == nil || !entry.dirty {
		t.Fatalf("expected dirty cached partition entry, got %+v", entry)
	}

	req := httptest.NewRequest(http.MethodGet, "/pull?plant_id=p1&local_date=2026-03-06", nil)
	resp := httptest.NewRecorder()
	s.handlePull(resp, req)
	if resp.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.Code)
	}
	if entry.dirty {
		t.Fatalf("expected snapshot cache to become clean after pull")
	}
	first := append([]byte(nil), resp.Body.Bytes()...)
	if len(first) == 0 {
		t.Fatal("expected parquet payload")
	}

	resp2 := httptest.NewRecorder()
	s.handlePull(resp2, req)
	if resp2.Code != http.StatusOK {
		t.Fatalf("expected 200 on second pull, got %d", resp2.Code)
	}
	if string(first) != resp2.Body.String() {
		t.Fatalf("expected cached parquet payload to be reused")
	}

	if err := s.WriteBatch(types.Batch{{Tuple: types.Tuple{"plant_id": "p1", "local_date": "2026-03-06", "id": "b"}, Count: 1}}); err != nil {
		t.Fatalf("WriteBatch second: %v", err)
	}
	if !entry.dirty {
		t.Fatalf("expected write to invalidate snapshot cache")
	}
	resp3 := httptest.NewRecorder()
	s.handlePull(resp3, req)
	if resp3.Code != http.StatusOK {
		t.Fatalf("expected 200 on refreshed pull, got %d", resp3.Code)
	}
	if string(first) == resp3.Body.String() {
		t.Fatalf("expected parquet payload to change after cache invalidation")
	}
}
