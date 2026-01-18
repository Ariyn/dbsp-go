package wal

import (
	"context"
	"database/sql"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/ariyn/dbsp/internal/dbsp/types"

	_ "github.com/mattn/go-sqlite3"
)

func TestSQLiteWAL_AppendAndReplay_RoundTrip(t *testing.T) {
	tmp := t.TempDir()
	dbPath := filepath.Join(tmp, "wal.db")

	w, err := NewSQLiteWAL(dbPath)
	if err != nil {
		t.Fatalf("NewSQLiteWAL: %v", err)
	}

	b1 := types.Batch{{Tuple: types.Tuple{"id": int64(1), "v": "a"}, Count: 1}}
	b2 := types.Batch{{Tuple: types.Tuple{"id": int64(2), "v": "b"}, Count: -1}}

	ctx := context.Background()
	if err := w.Append(ctx, b1); err != nil {
		t.Fatalf("Append b1: %v", err)
	}
	if err := w.Append(ctx, b2); err != nil {
		t.Fatalf("Append b2: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	w2, err := NewSQLiteWAL(dbPath)
	if err != nil {
		t.Fatalf("NewSQLiteWAL reopen: %v", err)
	}
	defer w2.Close()

	var got []types.Batch
	if err := w2.Replay(ctx, func(b types.Batch) error {
		got = append(got, b)
		return nil
	}); err != nil {
		t.Fatalf("Replay: %v", err)
	}

	want := []types.Batch{b1, b2}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("replayed batches mismatch\nwant=%v\n got=%v", want, got)
	}
}

func TestSQLiteWAL_TableRowCount(t *testing.T) {
	tmp := t.TempDir()
	dbPath := filepath.Join(tmp, "wal.db")

	w, err := NewSQLiteWAL(dbPath)
	if err != nil {
		t.Fatalf("NewSQLiteWAL: %v", err)
	}
	defer w.Close()

	ctx := context.Background()
	if err := w.Append(ctx, types.Batch{{Tuple: types.Tuple{"k": "x"}, Count: 1}}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := w.Append(ctx, types.Batch{{Tuple: types.Tuple{"k": "y"}, Count: 1}}); err != nil {
		t.Fatalf("Append: %v", err)
	}

	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer db.Close()

	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM wal_batches`).Scan(&count); err != nil {
		t.Fatalf("count query: %v", err)
	}
	if count != 2 {
		t.Fatalf("expected 2 rows, got %d", count)
	}
}

func TestSQLiteWAL_Checkpoint_SaveLoad_AndReplayFrom(t *testing.T) {
	tmp := t.TempDir()
	dbPath := filepath.Join(tmp, "wal.db")

	w, err := NewSQLiteWAL(dbPath)
	if err != nil {
		t.Fatalf("NewSQLiteWAL: %v", err)
	}
	defer w.Close()

	ctx := context.Background()
	if err := w.Append(ctx, types.Batch{{Tuple: types.Tuple{"id": int64(1)}, Count: 1}}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := w.Append(ctx, types.Batch{{Tuple: types.Tuple{"id": int64(2)}, Count: 1}}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := w.Append(ctx, types.Batch{{Tuple: types.Tuple{"id": int64(3)}, Count: 1}}); err != nil {
		t.Fatalf("Append: %v", err)
	}

	maxSeq, err := w.MaxSeq(ctx)
	if err != nil {
		t.Fatalf("MaxSeq: %v", err)
	}
	if maxSeq != 3 {
		t.Fatalf("expected maxSeq=3, got %d", maxSeq)
	}

	if err := w.SaveCheckpoint(ctx, Checkpoint{LastSeq: 2, Snapshot: []byte("snapshot")}); err != nil {
		t.Fatalf("SaveCheckpoint: %v", err)
	}
	cp, err := w.LoadLatestCheckpoint(ctx)
	if err != nil {
		t.Fatalf("LoadLatestCheckpoint: %v", err)
	}
	if cp == nil {
		t.Fatalf("expected checkpoint")
	}
	if cp.LastSeq != 2 {
		t.Fatalf("expected LastSeq=2, got %d", cp.LastSeq)
	}
	if string(cp.Snapshot) != "snapshot" {
		t.Fatalf("unexpected snapshot payload")
	}

	// ReplayFrom should yield only seq>2 (one batch).
	count := 0
	if err := w.ReplayFrom(ctx, 2, func(types.Batch) error {
		count++
		return nil
	}); err != nil {
		t.Fatalf("ReplayFrom: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected 1 batch from ReplayFrom, got %d", count)
	}
}
