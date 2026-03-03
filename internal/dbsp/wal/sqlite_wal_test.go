package wal

import (
	"context"
	"database/sql"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/ariyn/dbsp/internal/dbsp/types"

	_ "github.com/mattn/go-sqlite3"
)

func pragmaInt(t *testing.T, db *sql.DB, name string) int64 {
	t.Helper()
	var out int64
	if err := db.QueryRow("PRAGMA " + name + ";").Scan(&out); err != nil {
		t.Fatalf("PRAGMA %s: %v", name, err)
	}
	return out
}

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

func TestSQLiteWAL_PragmasApplied(t *testing.T) {
	tmp := t.TempDir()
	dbPath := filepath.Join(tmp, "wal.db")

	cfg := SQLiteWALConfig{
		TempStore:     "FILE",
		CacheSize:     2000,
		MmapSize:      1 << 20,
		BusyTimeoutMS: 5000,
		ExtraPragmas: map[string]string{
			"journal_size_limit": "1048576",
		},
	}

	w, err := NewSQLiteWALWithConfig(dbPath, cfg)
	if err != nil {
		t.Fatalf("NewSQLiteWALWithConfig: %v", err)
	}
	defer w.Close()

	if got := pragmaInt(t, w.db, "temp_store"); got != 1 {
		t.Fatalf("temp_store pragma mismatch: got=%d want=1", got)
	}
	if got := pragmaInt(t, w.db, "cache_size"); got != 2000 {
		t.Fatalf("cache_size pragma mismatch: got=%d want=2000", got)
	}
	if got := pragmaInt(t, w.db, "mmap_size"); got != 1<<20 {
		t.Fatalf("mmap_size pragma mismatch: got=%d want=%d", got, 1<<20)
	}
	if got := pragmaInt(t, w.db, "busy_timeout"); got != 5000 {
		t.Fatalf("busy_timeout pragma mismatch: got=%d want=5000", got)
	}
	if got := pragmaInt(t, w.db, "journal_size_limit"); got != 1048576 {
		t.Fatalf("journal_size_limit pragma mismatch: got=%d want=1048576", got)
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

func TestSQLiteWAL_Checkpoint_IncrementalMeta_AndLoadLatestFullBefore(t *testing.T) {
	tmp := t.TempDir()
	dbPath := filepath.Join(tmp, "wal.db")

	w, err := NewSQLiteWAL(dbPath)
	if err != nil {
		t.Fatalf("NewSQLiteWAL: %v", err)
	}
	defer w.Close()

	ctx := context.Background()
	if err := w.SaveCheckpoint(ctx, Checkpoint{Mode: "full", LastSeq: 10, BaseSeq: 10, Snapshot: []byte("full-10")}); err != nil {
		t.Fatalf("SaveCheckpoint(full): %v", err)
	}
	if err := w.SaveCheckpoint(ctx, Checkpoint{Mode: "incremental", LastSeq: 15, BaseSeq: 10}); err != nil {
		t.Fatalf("SaveCheckpoint(incremental): %v", err)
	}

	latest, err := w.LoadLatestCheckpoint(ctx)
	if err != nil {
		t.Fatalf("LoadLatestCheckpoint: %v", err)
	}
	if latest == nil {
		t.Fatalf("expected latest checkpoint")
	}
	if latest.Mode != "incremental" {
		t.Fatalf("expected incremental mode, got %q", latest.Mode)
	}
	if latest.BaseSeq != 10 {
		t.Fatalf("expected BaseSeq=10, got %d", latest.BaseSeq)
	}

	full, err := w.LoadLatestFullCheckpointBefore(ctx, latest.BaseSeq)
	if err != nil {
		t.Fatalf("LoadLatestFullCheckpointBefore: %v", err)
	}
	if full == nil {
		t.Fatalf("expected full checkpoint lookup result")
	}
	if full.LastSeq != 10 {
		t.Fatalf("expected full LastSeq=10, got %d", full.LastSeq)
	}
	if string(full.Snapshot) != "full-10" {
		t.Fatalf("unexpected full snapshot payload: %q", string(full.Snapshot))
	}
}

func TestSQLiteWAL_Checkpoint_IncrementalMeta_AndFullLookup(t *testing.T) {
	tmp := t.TempDir()
	dbPath := filepath.Join(tmp, "wal.db")

	w, err := NewSQLiteWAL(dbPath)
	if err != nil {
		t.Fatalf("NewSQLiteWAL: %v", err)
	}
	defer w.Close()

	ctx := context.Background()

	if err := w.SaveCheckpoint(ctx, Checkpoint{Mode: "full", LastSeq: 10, BaseSeq: 10, Snapshot: []byte("full-10")}); err != nil {
		t.Fatalf("SaveCheckpoint full-10: %v", err)
	}
	if err := w.SaveCheckpoint(ctx, Checkpoint{Mode: "incremental", LastSeq: 15, BaseSeq: 10}); err != nil {
		t.Fatalf("SaveCheckpoint inc-15: %v", err)
	}
	if err := w.SaveCheckpoint(ctx, Checkpoint{Mode: "full", LastSeq: 20, BaseSeq: 20, Snapshot: []byte("full-20")}); err != nil {
		t.Fatalf("SaveCheckpoint full-20: %v", err)
	}

	latest, err := w.LoadLatestCheckpoint(ctx)
	if err != nil {
		t.Fatalf("LoadLatestCheckpoint: %v", err)
	}
	if latest == nil || latest.Mode != "full" || latest.LastSeq != 20 {
		t.Fatalf("expected latest full checkpoint seq=20, got %+v", latest)
	}

	// Create a newest incremental checkpoint and verify metadata decode.
	if err := w.SaveCheckpoint(ctx, Checkpoint{Mode: "incremental", LastSeq: 25, BaseSeq: 20}); err != nil {
		t.Fatalf("SaveCheckpoint inc-25: %v", err)
	}
	latest, err = w.LoadLatestCheckpoint(ctx)
	if err != nil {
		t.Fatalf("LoadLatestCheckpoint after inc: %v", err)
	}
	if latest == nil {
		t.Fatalf("expected latest checkpoint")
	}
	if latest.Mode != "incremental" {
		t.Fatalf("expected incremental mode, got %q", latest.Mode)
	}
	if latest.BaseSeq != 20 || latest.LastSeq != 25 {
		t.Fatalf("expected incremental base=20 last=25, got %+v", latest)
	}

	fullBefore, err := w.LoadLatestFullCheckpointBefore(ctx, latest.BaseSeq)
	if err != nil {
		t.Fatalf("LoadLatestFullCheckpointBefore: %v", err)
	}
	if fullBefore == nil {
		t.Fatalf("expected full checkpoint before seq=%d", latest.BaseSeq)
	}
	if fullBefore.LastSeq != 20 {
		t.Fatalf("expected full checkpoint seq=20, got %d", fullBefore.LastSeq)
	}
}

func TestSQLiteWAL_SaveCheckpoint_FullCompactsOldWALAndCheckpoints(t *testing.T) {
	tmp := t.TempDir()
	dbPath := filepath.Join(tmp, "wal.db")

	w, err := NewSQLiteWAL(dbPath)
	if err != nil {
		t.Fatalf("NewSQLiteWAL: %v", err)
	}
	defer w.Close()

	ctx := context.Background()
	for i := 1; i <= 5; i++ {
		if err := w.Append(ctx, types.Batch{{Tuple: types.Tuple{"id": int64(i)}, Count: 1}}); err != nil {
			t.Fatalf("Append #%d: %v", i, err)
		}
	}

	if err := w.SaveCheckpoint(ctx, Checkpoint{Mode: "full", LastSeq: 2, BaseSeq: 2, Snapshot: []byte("full-2")}); err != nil {
		t.Fatalf("SaveCheckpoint full-2: %v", err)
	}
	if err := w.SaveCheckpoint(ctx, Checkpoint{Mode: "incremental", LastSeq: 3, BaseSeq: 2}); err != nil {
		t.Fatalf("SaveCheckpoint inc-3: %v", err)
	}
	if err := w.SaveCheckpoint(ctx, Checkpoint{Mode: "full", LastSeq: 4, BaseSeq: 4, Snapshot: []byte("full-4")}); err != nil {
		t.Fatalf("SaveCheckpoint full-4: %v", err)
	}

	var cpCount int
	if err := w.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM wal_checkpoints`).Scan(&cpCount); err != nil {
		t.Fatalf("count wal_checkpoints: %v", err)
	}
	if cpCount != 1 {
		t.Fatalf("expected only latest full checkpoint to remain after compaction, got %d", cpCount)
	}

	replayed := 0
	if err := w.Replay(ctx, func(types.Batch) error {
		replayed++
		return nil
	}); err != nil {
		t.Fatalf("Replay after compaction: %v", err)
	}
	if replayed != 1 {
		t.Fatalf("expected only seq>4 WAL batch to remain, got %d", replayed)
	}
}

func TestSQLiteWAL_ResolveCheckpointSnapshot_IncrementalDelta(t *testing.T) {
	tmp := t.TempDir()
	dbPath := filepath.Join(tmp, "wal.db")

	w, err := NewSQLiteWAL(dbPath)
	if err != nil {
		t.Fatalf("NewSQLiteWAL: %v", err)
	}
	defer w.Close()

	ctx := context.Background()
	baseSnapshot := []byte("snapshot-A")
	if err := w.SaveCheckpoint(ctx, Checkpoint{Mode: "full", LastSeq: 10, BaseSeq: 10, Snapshot: baseSnapshot}); err != nil {
		t.Fatalf("SaveCheckpoint(full): %v", err)
	}

	targetSnapshot := []byte("snapshot-B")
	if err := w.SaveCheckpoint(ctx, Checkpoint{Mode: "incremental", LastSeq: 12, BaseSeq: 10, Snapshot: targetSnapshot}); err != nil {
		t.Fatalf("SaveCheckpoint(incremental): %v", err)
	}

	latest, err := w.LoadLatestCheckpoint(ctx)
	if err != nil {
		t.Fatalf("LoadLatestCheckpoint: %v", err)
	}
	if latest == nil {
		t.Fatalf("expected latest checkpoint")
	}
	if latest.Codec != sqliteCodecGraphIncDV1 {
		t.Fatalf("expected incremental delta codec %q, got %q", sqliteCodecGraphIncDV1, latest.Codec)
	}

	resolvedSnapshot, afterSeq, err := w.ResolveCheckpointSnapshot(ctx, latest)
	if err != nil {
		t.Fatalf("ResolveCheckpointSnapshot: %v", err)
	}
	if string(resolvedSnapshot) != string(targetSnapshot) {
		t.Fatalf("expected resolved snapshot %q, got %q", string(targetSnapshot), string(resolvedSnapshot))
	}
	if afterSeq != latest.LastSeq {
		t.Fatalf("expected afterSeq=%d, got %d", latest.LastSeq, afterSeq)
	}
}

func TestSQLiteWAL_ResolveCheckpointSnapshot_ChainedIncrementalDeltas(t *testing.T) {
	tmp := t.TempDir()
	dbPath := filepath.Join(tmp, "wal.db")

	w, err := NewSQLiteWAL(dbPath)
	if err != nil {
		t.Fatalf("NewSQLiteWAL: %v", err)
	}
	defer w.Close()

	ctx := context.Background()
	if err := w.SaveCheckpoint(ctx, Checkpoint{Mode: "full", LastSeq: 1, BaseSeq: 1, Snapshot: []byte("snap-1")}); err != nil {
		t.Fatalf("SaveCheckpoint(full): %v", err)
	}
	if err := w.SaveCheckpoint(ctx, Checkpoint{Mode: "incremental", LastSeq: 2, BaseSeq: 1, Snapshot: []byte("snap-2")}); err != nil {
		t.Fatalf("SaveCheckpoint(inc-2): %v", err)
	}
	if err := w.SaveCheckpoint(ctx, Checkpoint{Mode: "incremental", LastSeq: 3, BaseSeq: 2, Snapshot: []byte("snap-3")}); err != nil {
		t.Fatalf("SaveCheckpoint(inc-3): %v", err)
	}

	latest, err := w.LoadLatestCheckpoint(ctx)
	if err != nil {
		t.Fatalf("LoadLatestCheckpoint: %v", err)
	}
	if latest == nil {
		t.Fatalf("expected latest checkpoint")
	}
	if latest.Mode != "incremental" || latest.BaseSeq != 2 {
		t.Fatalf("expected chained incremental checkpoint with base_seq=2, got %+v", latest)
	}

	resolved, afterSeq, err := w.ResolveCheckpointSnapshot(ctx, latest)
	if err != nil {
		t.Fatalf("ResolveCheckpointSnapshot: %v", err)
	}
	if string(resolved) != "snap-3" {
		t.Fatalf("expected resolved snapshot snap-3, got %q", string(resolved))
	}
	if afterSeq != 3 {
		t.Fatalf("expected afterSeq=3, got %d", afterSeq)
	}
}

func TestSQLiteWAL_LoadLatestCheckpoint_IncrementalCarriesMutations(t *testing.T) {
	tmp := t.TempDir()
	dbPath := filepath.Join(tmp, "wal.db")

	w, err := NewSQLiteWAL(dbPath)
	if err != nil {
		t.Fatalf("NewSQLiteWAL: %v", err)
	}
	defer w.Close()

	ctx := context.Background()
	if err := w.SaveCheckpoint(ctx, Checkpoint{Mode: "full", LastSeq: 5, BaseSeq: 5, Snapshot: []byte("snap-5")}); err != nil {
		t.Fatalf("SaveCheckpoint(full): %v", err)
	}

	mutations := []CheckpointMutation{
		{Type: "put", Key: []byte("join/L|R"), Value: []byte("payload-1")},
		{Type: "delete", Key: []byte("group/K")},
	}
	if err := w.SaveCheckpoint(ctx, Checkpoint{Mode: "incremental", LastSeq: 6, BaseSeq: 5, Snapshot: []byte("snap-6"), Mutations: mutations}); err != nil {
		t.Fatalf("SaveCheckpoint(incremental): %v", err)
	}

	latest, err := w.LoadLatestCheckpoint(ctx)
	if err != nil {
		t.Fatalf("LoadLatestCheckpoint: %v", err)
	}
	if latest == nil {
		t.Fatalf("expected latest checkpoint")
	}
	if len(latest.Mutations) != len(mutations) {
		t.Fatalf("expected %d mutations, got %d", len(mutations), len(latest.Mutations))
	}
	if latest.Mutations[0].Type != "put" || string(latest.Mutations[0].Key) != "join/L|R" {
		t.Fatalf("unexpected first mutation: %+v", latest.Mutations[0])
	}
	if latest.Mutations[1].Type != "delete" || string(latest.Mutations[1].Key) != "group/K" {
		t.Fatalf("unexpected second mutation: %+v", latest.Mutations[1])
	}
}

func TestSQLiteWAL_ResolveCheckpointSnapshotWithMutations_Chained(t *testing.T) {
	tmp := t.TempDir()
	dbPath := filepath.Join(tmp, "wal.db")

	w, err := NewSQLiteWAL(dbPath)
	if err != nil {
		t.Fatalf("NewSQLiteWAL: %v", err)
	}
	defer w.Close()

	ctx := context.Background()
	if err := w.SaveCheckpoint(ctx, Checkpoint{Mode: "full", LastSeq: 1, BaseSeq: 1, Snapshot: []byte("snap-1")}); err != nil {
		t.Fatalf("SaveCheckpoint(full): %v", err)
	}
	if err := w.SaveCheckpoint(ctx, Checkpoint{Mode: "incremental", LastSeq: 2, BaseSeq: 1, Snapshot: []byte("snap-2"), Mutations: []CheckpointMutation{{Type: "put", Key: []byte("k1"), Value: []byte("v1")}}}); err != nil {
		t.Fatalf("SaveCheckpoint(inc-2): %v", err)
	}
	if err := w.SaveCheckpoint(ctx, Checkpoint{Mode: "incremental", LastSeq: 3, BaseSeq: 2, Snapshot: []byte("snap-3"), Mutations: []CheckpointMutation{{Type: "delete", Key: []byte("k1")}}}); err != nil {
		t.Fatalf("SaveCheckpoint(inc-3): %v", err)
	}

	latest, err := w.LoadLatestCheckpoint(ctx)
	if err != nil {
		t.Fatalf("LoadLatestCheckpoint: %v", err)
	}
	if latest == nil {
		t.Fatalf("expected latest checkpoint")
	}

	resolvedSnapshot, afterSeq, resolvedMutations, err := w.ResolveCheckpointSnapshotWithMutations(ctx, latest)
	if err != nil {
		t.Fatalf("ResolveCheckpointSnapshotWithMutations: %v", err)
	}
	if string(resolvedSnapshot) != "snap-3" {
		t.Fatalf("expected resolved snapshot snap-3, got %q", string(resolvedSnapshot))
	}
	if afterSeq != 3 {
		t.Fatalf("expected afterSeq=3, got %d", afterSeq)
	}
	if len(resolvedMutations) != 2 {
		t.Fatalf("expected 2 resolved mutations, got %d", len(resolvedMutations))
	}
	if resolvedMutations[0].Type != "put" || string(resolvedMutations[0].Key) != "k1" {
		t.Fatalf("unexpected resolved mutation[0]: %+v", resolvedMutations[0])
	}
	if resolvedMutations[1].Type != "delete" || string(resolvedMutations[1].Key) != "k1" {
		t.Fatalf("unexpected resolved mutation[1]: %+v", resolvedMutations[1])
	}
}

func TestSQLiteWAL_IncrementalChainDepth(t *testing.T) {
	tmp := t.TempDir()
	dbPath := filepath.Join(tmp, "wal.db")

	w, err := NewSQLiteWAL(dbPath)
	if err != nil {
		t.Fatalf("NewSQLiteWAL: %v", err)
	}
	defer w.Close()

	ctx := context.Background()
	if err := w.SaveCheckpoint(ctx, Checkpoint{Mode: "full", LastSeq: 1, BaseSeq: 1, Snapshot: []byte("snap-1")}); err != nil {
		t.Fatalf("SaveCheckpoint(full-1): %v", err)
	}
	if err := w.SaveCheckpoint(ctx, Checkpoint{Mode: "incremental", LastSeq: 2, BaseSeq: 1, Snapshot: []byte("snap-2")}); err != nil {
		t.Fatalf("SaveCheckpoint(inc-2): %v", err)
	}
	if err := w.SaveCheckpoint(ctx, Checkpoint{Mode: "incremental", LastSeq: 3, BaseSeq: 2, Snapshot: []byte("snap-3")}); err != nil {
		t.Fatalf("SaveCheckpoint(inc-3): %v", err)
	}

	depth, err := w.IncrementalChainDepth(ctx, 3)
	if err != nil {
		t.Fatalf("IncrementalChainDepth: %v", err)
	}
	if depth != 2 {
		t.Fatalf("expected incremental chain depth 2, got %d", depth)
	}

	if err := w.SaveCheckpoint(ctx, Checkpoint{Mode: "full", LastSeq: 4, BaseSeq: 4, Snapshot: []byte("snap-4")}); err != nil {
		t.Fatalf("SaveCheckpoint(full-4): %v", err)
	}
	depth, err = w.IncrementalChainDepth(ctx, 4)
	if err != nil {
		t.Fatalf("IncrementalChainDepth after full: %v", err)
	}
	if depth != 0 {
		t.Fatalf("expected incremental chain depth 0 after full checkpoint, got %d", depth)
	}
}

func TestSQLiteWAL_RetentionTTL_PrunesOldRowsOnAppend(t *testing.T) {
	tmp := t.TempDir()
	dbPath := filepath.Join(tmp, "wal.db")

	w, err := NewSQLiteWAL(dbPath)
	if err != nil {
		t.Fatalf("NewSQLiteWAL: %v", err)
	}
	defer w.Close()

	ctx := context.Background()
	if err := w.Append(ctx, types.Batch{{Tuple: types.Tuple{"id": int64(1)}, Count: 1}}); err != nil {
		t.Fatalf("Append #1: %v", err)
	}
	if err := w.Append(ctx, types.Batch{{Tuple: types.Tuple{"id": int64(2)}, Count: 1}}); err != nil {
		t.Fatalf("Append #2: %v", err)
	}

	old := time.Now().Add(-2 * time.Hour).UnixMilli()
	if _, err := w.db.ExecContext(ctx, `UPDATE wal_batches SET created_at_unix_ms = ?`, old); err != nil {
		t.Fatalf("mark old wal_batches: %v", err)
	}
	if _, err := w.db.ExecContext(ctx, `UPDATE wal_checkpoints SET created_at_unix_ms = ?`, old); err != nil {
		t.Fatalf("mark old wal_checkpoints: %v", err)
	}

	w.SetRetentionTTL(1 * time.Hour)
	if err := w.Append(ctx, types.Batch{{Tuple: types.Tuple{"id": int64(3)}, Count: 1}}); err != nil {
		t.Fatalf("Append #3 with retention: %v", err)
	}

	var count int
	if err := w.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM wal_batches`).Scan(&count); err != nil {
		t.Fatalf("count wal_batches: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected only newest row to remain after retention prune, got %d", count)
	}
}
