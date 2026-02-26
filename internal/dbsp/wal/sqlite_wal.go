package wal

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/gob"
	"fmt"
	"time"

	"github.com/ariyn/dbsp/internal/dbsp/types"

	_ "github.com/mattn/go-sqlite3"
)

const (
	sqliteCodecGobV1      = "gob-v1"
	sqliteCodecGraphGobV1 = "graph-gob-v1"
	sqliteCodecGraphIncV1 = "graph-inc-meta-v1"
	sqliteCodecGraphIncDV1 = "graph-inc-delta-v1"
)

// Checkpoint represents a persisted operator-graph snapshot paired with a WAL position.
// lastSeq is the maximum seq included in the snapshot; replay should continue with seq > lastSeq.
type Checkpoint struct {
	LastSeq  int64
	Codec    string
	Snapshot []byte
	Mode     string
	BaseSeq  int64
	Mutations []CheckpointMutation
}

type CheckpointMutation struct {
	Type  string
	Key   []byte
	Value []byte
}

type incrementalCheckpointMetaV1 struct {
	BaseSeq int64
}

type incrementalCheckpointPayloadV1 struct {
	BaseSeq   int64
	Kind      string // meta|delta
	Delta     *byteDeltaV1
	Mutations []CheckpointMutation
}

type byteDeltaV1 struct {
	NewLen int
	Chunks []byteDeltaChunkV1
}

type byteDeltaChunkV1 struct {
	Offset int
	Data   []byte
}

type SQLiteWAL struct {
	db           *sql.DB
	insertStmt   *sql.Stmt
	retentionTTL time.Duration
}

func NewSQLiteWAL(path string) (*SQLiteWAL, error) {
	if path == "" {
		return nil, fmt.Errorf("wal sqlite path is empty")
	}

	db, err := sql.Open("sqlite3", path)
	if err != nil {
		return nil, fmt.Errorf("open sqlite wal: %w", err)
	}

	// Ensure we close db if initialization fails.
	w := &SQLiteWAL{db: db}
	if err := w.init(); err != nil {
		_ = db.Close()
		return nil, err
	}

	stmt, err := db.Prepare(`INSERT INTO wal_batches(created_at_unix_ms, codec, payload) VALUES (?, ?, ?)`)
	if err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("prepare wal insert: %w", err)
	}
	w.insertStmt = stmt

	return w, nil
}

func (w *SQLiteWAL) init() error {
	// Tuning for many small appends.
	// We prefer durability/perf balance; callers can override via DSN if needed.
	pragmas := []string{
		`PRAGMA journal_mode=WAL;`,
		`PRAGMA synchronous=NORMAL;`,
		`PRAGMA temp_store=MEMORY;`,
		`PRAGMA foreign_keys=ON;`,
	}
	for _, p := range pragmas {
		if _, err := w.db.Exec(p); err != nil {
			return fmt.Errorf("sqlite pragma failed (%s): %w", p, err)
		}
	}

	// Minimal append-only table.
	_, err := w.db.Exec(`
CREATE TABLE IF NOT EXISTS wal_batches (
	seq INTEGER PRIMARY KEY AUTOINCREMENT,
	created_at_unix_ms INTEGER NOT NULL,
	codec TEXT NOT NULL,
	payload BLOB NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_wal_batches_created_at ON wal_batches(created_at_unix_ms);

CREATE TABLE IF NOT EXISTS wal_checkpoints (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	created_at_unix_ms INTEGER NOT NULL,
	last_seq INTEGER NOT NULL,
	codec TEXT NOT NULL,
	snapshot BLOB NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_wal_checkpoints_created_at ON wal_checkpoints(created_at_unix_ms);
`)
	if err != nil {
		return fmt.Errorf("create wal schema: %w", err)
	}

	return nil
}

func (w *SQLiteWAL) Append(ctx context.Context, batch types.Batch) error {
	if w == nil || w.db == nil {
		return fmt.Errorf("wal is nil")
	}

	payload, err := encodeBatchGobV1(batch)
	if err != nil {
		return err
	}

	now := time.Now()
	_, err = w.insertStmt.ExecContext(ctx, now.UnixMilli(), sqliteCodecGobV1, payload)
	if err != nil {
		return fmt.Errorf("append wal: %w", err)
	}

	if w.retentionTTL > 0 {
		if err := w.PruneBefore(ctx, now.Add(-w.retentionTTL).UnixMilli()); err != nil {
			return err
		}
	}
	return nil
}

func (w *SQLiteWAL) SetRetentionTTL(ttl time.Duration) {
	if w == nil {
		return
	}
	w.retentionTTL = ttl
}

// PruneBefore deletes WAL batches/checkpoints older than the given unix-ms cutoff.
func (w *SQLiteWAL) PruneBefore(ctx context.Context, cutoffUnixMS int64) error {
	if w == nil || w.db == nil {
		return fmt.Errorf("wal is nil")
	}
	if cutoffUnixMS <= 0 {
		return nil
	}
	if _, err := w.db.ExecContext(ctx, `DELETE FROM wal_batches WHERE created_at_unix_ms < ?`, cutoffUnixMS); err != nil {
		return fmt.Errorf("prune wal batches: %w", err)
	}
	if _, err := w.db.ExecContext(ctx, `DELETE FROM wal_checkpoints WHERE created_at_unix_ms < ?`, cutoffUnixMS); err != nil {
		return fmt.Errorf("prune wal checkpoints: %w", err)
	}
	return nil
}

func (w *SQLiteWAL) Replay(ctx context.Context, apply func(types.Batch) error) error {
	if w == nil || w.db == nil {
		return fmt.Errorf("wal is nil")
	}
	if apply == nil {
		return fmt.Errorf("apply callback is nil")
	}

	rows, err := w.db.QueryContext(ctx, `SELECT codec, payload FROM wal_batches ORDER BY seq ASC`)
	if err != nil {
		return fmt.Errorf("query wal: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		var codec string
		var payload []byte
		if err := rows.Scan(&codec, &payload); err != nil {
			return fmt.Errorf("scan wal row: %w", err)
		}

		var batch types.Batch
		switch codec {
		case sqliteCodecGobV1:
			b, err := decodeBatchGobV1(payload)
			if err != nil {
				return err
			}
			batch = b
		default:
			return fmt.Errorf("unknown wal codec: %s", codec)
		}

		if err := apply(batch); err != nil {
			return err
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate wal rows: %w", err)
	}

	return nil
}

// MaxSeq returns the current maximum wal_batches.seq (or 0 if empty).
func (w *SQLiteWAL) MaxSeq(ctx context.Context) (int64, error) {
	if w == nil || w.db == nil {
		return 0, fmt.Errorf("wal is nil")
	}
	var maxSeq sql.NullInt64
	if err := w.db.QueryRowContext(ctx, `SELECT MAX(seq) FROM wal_batches`).Scan(&maxSeq); err != nil {
		return 0, fmt.Errorf("query max seq: %w", err)
	}
	if !maxSeq.Valid {
		return 0, nil
	}
	return maxSeq.Int64, nil
}

// ReplayFrom replays wal_batches with seq > afterSeq.
func (w *SQLiteWAL) ReplayFrom(ctx context.Context, afterSeq int64, apply func(types.Batch) error) error {
	if w == nil || w.db == nil {
		return fmt.Errorf("wal is nil")
	}
	if apply == nil {
		return fmt.Errorf("apply callback is nil")
	}

	rows, err := w.db.QueryContext(ctx, `SELECT codec, payload FROM wal_batches WHERE seq > ? ORDER BY seq ASC`, afterSeq)
	if err != nil {
		return fmt.Errorf("query wal from seq: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		var codec string
		var payload []byte
		if err := rows.Scan(&codec, &payload); err != nil {
			return fmt.Errorf("scan wal row: %w", err)
		}

		var batch types.Batch
		switch codec {
		case sqliteCodecGobV1:
			b, err := decodeBatchGobV1(payload)
			if err != nil {
				return err
			}
			batch = b
		default:
			return fmt.Errorf("unknown wal codec: %s", codec)
		}

		if err := apply(batch); err != nil {
			return err
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate wal rows: %w", err)
	}
	return nil
}

// SaveCheckpoint stores a snapshot with a WAL position.
func (w *SQLiteWAL) SaveCheckpoint(ctx context.Context, cp Checkpoint) error {
	if w == nil || w.db == nil {
		return fmt.Errorf("wal is nil")
	}
	if cp.Mode == "" {
		cp.Mode = "full"
	}
	if cp.BaseSeq < 0 {
		return fmt.Errorf("checkpoint base seq is negative")
	}
	if cp.Mode == "incremental" {
		if cp.LastSeq < cp.BaseSeq {
			return fmt.Errorf("incremental checkpoint last seq (%d) is smaller than base seq (%d)", cp.LastSeq, cp.BaseSeq)
		}
		if cp.Codec == "" {
			cp.Codec = sqliteCodecGraphIncDV1
		}

		if len(cp.Snapshot) > 0 {
			baseCP, err := w.LoadLatestCheckpointAtOrBefore(ctx, cp.BaseSeq)
			if err != nil {
				return err
			}
			if baseCP != nil {
				baseSnapshot, _, err := w.ResolveCheckpointSnapshot(ctx, baseCP)
				if err != nil {
					return err
				}
				if len(baseSnapshot) > 0 {
					delta := buildByteDelta(baseSnapshot, cp.Snapshot)
					payload, err := encodeIncrementalCheckpointPayload(incrementalCheckpointPayloadV1{
						BaseSeq:   cp.BaseSeq,
						Kind:      "delta",
						Delta:     &delta,
						Mutations: cloneCheckpointMutations(cp.Mutations),
					})
					if err != nil {
						return err
					}
					cp.Snapshot = payload
					cp.Codec = sqliteCodecGraphIncDV1
				} else {
					payload, codec, err := buildIncrementalMetaOrMutationPayload(cp.BaseSeq, cp.Mutations)
					if err != nil {
						return err
					}
					cp.Snapshot = payload
					cp.Codec = codec
				}
			} else {
				payload, codec, err := buildIncrementalMetaOrMutationPayload(cp.BaseSeq, cp.Mutations)
				if err != nil {
					return err
				}
				cp.Snapshot = payload
				cp.Codec = codec
			}
		} else {
			payload, codec, err := buildIncrementalMetaOrMutationPayload(cp.BaseSeq, cp.Mutations)
			if err != nil {
				return err
			}
			cp.Snapshot = payload
			cp.Codec = codec
		}
	} else {
		if len(cp.Snapshot) == 0 {
			return fmt.Errorf("checkpoint snapshot is empty")
		}
		if cp.Codec == "" {
			cp.Codec = sqliteCodecGraphGobV1
		}
		if cp.BaseSeq == 0 {
			cp.BaseSeq = cp.LastSeq
		}
	}
	if cp.LastSeq < 0 {
		return fmt.Errorf("checkpoint last seq is negative")
	}

	tx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin save checkpoint tx: %w", err)
	}
	defer func() {
		_ = tx.Rollback()
	}()

	res, err := tx.ExecContext(ctx,
		`INSERT INTO wal_checkpoints(created_at_unix_ms, last_seq, codec, snapshot) VALUES (?, ?, ?, ?)`,
		time.Now().UnixMilli(), cp.LastSeq, cp.Codec, cp.Snapshot,
	)
	if err != nil {
		return fmt.Errorf("save checkpoint: %w", err)
	}

	if cp.Mode == "full" {
		insertedID, err := res.LastInsertId()
		if err != nil {
			return fmt.Errorf("read inserted checkpoint id: %w", err)
		}

		if _, err := tx.ExecContext(ctx,
			`DELETE FROM wal_checkpoints WHERE id < ? AND last_seq <= ?`,
			insertedID, cp.LastSeq,
		); err != nil {
			return fmt.Errorf("compact old checkpoints: %w", err)
		}

		if _, err := tx.ExecContext(ctx,
			`DELETE FROM wal_batches WHERE seq <= ?`,
			cp.LastSeq,
		); err != nil {
			return fmt.Errorf("compact wal batches up to checkpoint seq: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit save checkpoint tx: %w", err)
	}
	return nil
}

// LoadLatestCheckpoint returns the most recent checkpoint, or (nil, nil) if none.
func (w *SQLiteWAL) LoadLatestCheckpoint(ctx context.Context) (*Checkpoint, error) {
	if w == nil || w.db == nil {
		return nil, fmt.Errorf("wal is nil")
	}
	row := w.db.QueryRowContext(ctx,
		`SELECT last_seq, codec, snapshot FROM wal_checkpoints ORDER BY id DESC LIMIT 1`)
	var lastSeq int64
	var codec string
	var snapshot []byte
	if err := row.Scan(&lastSeq, &codec, &snapshot); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("load latest checkpoint: %w", err)
	}
	cp := &Checkpoint{LastSeq: lastSeq, Codec: codec, Snapshot: snapshot, Mode: "full", BaseSeq: lastSeq}
	if codec == sqliteCodecGraphIncV1 || codec == sqliteCodecGraphIncDV1 {
		cp.Mode = "incremental"
		cp.BaseSeq = 0
		if baseSeq, ok := decodeIncrementalCheckpointBaseSeq(snapshot, codec); ok {
			cp.BaseSeq = baseSeq
		}
		if codec == sqliteCodecGraphIncDV1 {
			payload, err := decodeIncrementalCheckpointPayload(snapshot)
			if err == nil {
				cp.Mutations = cloneCheckpointMutations(payload.Mutations)
			}
		}
	}
	return cp, nil
}

// LoadLatestFullCheckpointBefore returns the latest non-incremental checkpoint whose last_seq <= upToSeq.
func (w *SQLiteWAL) LoadLatestFullCheckpointBefore(ctx context.Context, upToSeq int64) (*Checkpoint, error) {
	if w == nil || w.db == nil {
		return nil, fmt.Errorf("wal is nil")
	}
	row := w.db.QueryRowContext(ctx,
		`SELECT last_seq, codec, snapshot FROM wal_checkpoints WHERE last_seq <= ? AND codec NOT IN (?, ?) ORDER BY id DESC LIMIT 1`,
		upToSeq, sqliteCodecGraphIncV1, sqliteCodecGraphIncDV1,
	)
	var lastSeq int64
	var codec string
	var snapshot []byte
	if err := row.Scan(&lastSeq, &codec, &snapshot); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("load latest full checkpoint before seq: %w", err)
	}
	return &Checkpoint{LastSeq: lastSeq, Codec: codec, Snapshot: snapshot, Mode: "full", BaseSeq: lastSeq}, nil
}

// LoadLatestCheckpointAtOrBefore returns the latest checkpoint whose last_seq <= upToSeq.
func (w *SQLiteWAL) LoadLatestCheckpointAtOrBefore(ctx context.Context, upToSeq int64) (*Checkpoint, error) {
	if w == nil || w.db == nil {
		return nil, fmt.Errorf("wal is nil")
	}
	row := w.db.QueryRowContext(ctx,
		`SELECT last_seq, codec, snapshot FROM wal_checkpoints WHERE last_seq <= ? ORDER BY id DESC LIMIT 1`,
		upToSeq,
	)
	var lastSeq int64
	var codec string
	var snapshot []byte
	if err := row.Scan(&lastSeq, &codec, &snapshot); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("load latest checkpoint at or before seq: %w", err)
	}
	cp := &Checkpoint{LastSeq: lastSeq, Codec: codec, Snapshot: snapshot, Mode: "full", BaseSeq: lastSeq}
	if codec == sqliteCodecGraphIncV1 || codec == sqliteCodecGraphIncDV1 {
		cp.Mode = "incremental"
		if baseSeq, ok := decodeIncrementalCheckpointBaseSeq(snapshot, codec); ok {
			cp.BaseSeq = baseSeq
		} else {
			cp.BaseSeq = 0
		}
		if codec == sqliteCodecGraphIncDV1 {
			payload, err := decodeIncrementalCheckpointPayload(snapshot)
			if err == nil {
				cp.Mutations = cloneCheckpointMutations(payload.Mutations)
			}
		}
	}
	return cp, nil
}

// ResolveCheckpointSnapshot resolves which snapshot bytes should be restored for a checkpoint,
// and returns the WAL sequence after which replay should continue.
func (w *SQLiteWAL) ResolveCheckpointSnapshot(ctx context.Context, cp *Checkpoint) ([]byte, int64, error) {
	return w.resolveCheckpointSnapshot(ctx, cp, 0)
}

func (w *SQLiteWAL) ResolveCheckpointSnapshotWithMutations(ctx context.Context, cp *Checkpoint) ([]byte, int64, []CheckpointMutation, error) {
	snapshot, afterSeq, err := w.resolveCheckpointSnapshot(ctx, cp, 0)
	if err != nil {
		return nil, 0, nil, err
	}
	mutations, err := w.resolveCheckpointMutations(ctx, cp, 0)
	if err != nil {
		return nil, 0, nil, err
	}
	return snapshot, afterSeq, mutations, nil
}

// IncrementalChainDepth returns how many consecutive incremental checkpoints
// exist when walking backwards from the latest checkpoint at-or-before upToSeq.
func (w *SQLiteWAL) IncrementalChainDepth(ctx context.Context, upToSeq int64) (int, error) {
	cp, err := w.LoadLatestCheckpointAtOrBefore(ctx, upToSeq)
	if err != nil {
		return 0, err
	}
	if cp == nil {
		return 0, nil
	}

	depth := 0
	seen := make(map[int64]bool)
	for cp != nil && cp.Mode == "incremental" {
		if seen[cp.LastSeq] {
			return depth, fmt.Errorf("checkpoint chain cycle detected at seq=%d", cp.LastSeq)
		}
		seen[cp.LastSeq] = true
		depth++
		if cp.BaseSeq <= 0 {
			break
		}
		next, err := w.LoadLatestCheckpointAtOrBefore(ctx, cp.BaseSeq)
		if err != nil {
			return depth, err
		}
		if next == nil || next.LastSeq == cp.LastSeq {
			break
		}
		cp = next
	}
	return depth, nil
}

func (w *SQLiteWAL) resolveCheckpointSnapshot(ctx context.Context, cp *Checkpoint, depth int) ([]byte, int64, error) {
	if depth > 64 {
		return nil, 0, fmt.Errorf("checkpoint resolve recursion limit exceeded")
	}
	if cp == nil {
		return nil, 0, nil
	}
	if cp.Mode != "incremental" {
		return cp.Snapshot, cp.LastSeq, nil
	}

	baseCP, err := w.LoadLatestCheckpointAtOrBefore(ctx, cp.BaseSeq)
	if err != nil {
		return nil, 0, err
	}
	if baseCP == nil {
		return nil, 0, nil
	}
	baseSnapshot, baseAfterSeq, err := w.resolveCheckpointSnapshot(ctx, baseCP, depth+1)
	if err != nil {
		return nil, 0, err
	}

	if cp.Codec == sqliteCodecGraphIncDV1 {
		payload, err := decodeIncrementalCheckpointPayload(cp.Snapshot)
		if err == nil && payload.Kind == "delta" && payload.Delta != nil {
			merged, err := applyByteDelta(baseSnapshot, *payload.Delta)
			if err == nil {
				return merged, cp.LastSeq, nil
			}
		}
	}

	return baseSnapshot, baseAfterSeq, nil
}

func (w *SQLiteWAL) resolveCheckpointMutations(ctx context.Context, cp *Checkpoint, depth int) ([]CheckpointMutation, error) {
	if depth > 64 {
		return nil, fmt.Errorf("checkpoint mutation resolve recursion limit exceeded")
	}
	if cp == nil {
		return nil, nil
	}
	if cp.Mode != "incremental" {
		return nil, nil
	}

	baseCP, err := w.LoadLatestCheckpointAtOrBefore(ctx, cp.BaseSeq)
	if err != nil {
		return nil, err
	}
	baseMutations := []CheckpointMutation(nil)
	if baseCP != nil {
		baseMutations, err = w.resolveCheckpointMutations(ctx, baseCP, depth+1)
		if err != nil {
			return nil, err
		}
	}

	out := make([]CheckpointMutation, 0, len(baseMutations)+len(cp.Mutations))
	out = append(out, baseMutations...)
	out = append(out, cloneCheckpointMutations(cp.Mutations)...)
	return out, nil
}

func (w *SQLiteWAL) Close() error {
	if w == nil {
		return nil
	}
	if w.insertStmt != nil {
		_ = w.insertStmt.Close()
	}
	if w.db != nil {
		return w.db.Close()
	}
	return nil
}

func encodeBatchGobV1(batch types.Batch) ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(batch); err != nil {
		return nil, fmt.Errorf("encode batch: %w", err)
	}
	return buf.Bytes(), nil
}

func decodeBatchGobV1(payload []byte) (types.Batch, error) {
	dec := gob.NewDecoder(bytes.NewReader(payload))
	var batch types.Batch
	if err := dec.Decode(&batch); err != nil {
		return nil, fmt.Errorf("decode batch: %w", err)
	}
	return batch, nil
}

func encodeIncrementalCheckpointMeta(meta incrementalCheckpointMetaV1) ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(meta); err != nil {
		return nil, fmt.Errorf("encode incremental checkpoint meta: %w", err)
	}
	return buf.Bytes(), nil
}

func decodeIncrementalCheckpointMeta(payload []byte) (incrementalCheckpointMetaV1, error) {
	dec := gob.NewDecoder(bytes.NewReader(payload))
	var meta incrementalCheckpointMetaV1
	if err := dec.Decode(&meta); err != nil {
		return incrementalCheckpointMetaV1{}, fmt.Errorf("decode incremental checkpoint meta: %w", err)
	}
	return meta, nil
}

func encodeIncrementalCheckpointPayload(payload incrementalCheckpointPayloadV1) ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(payload); err != nil {
		return nil, fmt.Errorf("encode incremental checkpoint payload: %w", err)
	}
	return buf.Bytes(), nil
}

func decodeIncrementalCheckpointPayload(payload []byte) (incrementalCheckpointPayloadV1, error) {
	dec := gob.NewDecoder(bytes.NewReader(payload))
	var out incrementalCheckpointPayloadV1
	if err := dec.Decode(&out); err != nil {
		return incrementalCheckpointPayloadV1{}, fmt.Errorf("decode incremental checkpoint payload: %w", err)
	}
	return out, nil
}

func decodeIncrementalCheckpointBaseSeq(payload []byte, codec string) (int64, bool) {
	if codec == sqliteCodecGraphIncDV1 {
		p, err := decodeIncrementalCheckpointPayload(payload)
		if err != nil {
			return 0, false
		}
		return p.BaseSeq, true
	}
	m, err := decodeIncrementalCheckpointMeta(payload)
	if err != nil {
		return 0, false
	}
	return m.BaseSeq, true
}

func buildByteDelta(base, target []byte) byteDeltaV1 {
	delta := byteDeltaV1{NewLen: len(target)}
	maxLen := len(target)
	if len(base) > maxLen {
		maxLen = len(base)
	}
	index := 0
	for index < maxLen {
		same := index < len(base) && index < len(target) && base[index] == target[index]
		if same {
			index++
			continue
		}
		start := index
		for index < maxLen {
			same = index < len(base) && index < len(target) && base[index] == target[index]
			if same {
				break
			}
			index++
		}
		end := index
		if start >= len(target) {
			continue
		}
		if end > len(target) {
			end = len(target)
		}
		chunk := byteDeltaChunkV1{Offset: start, Data: append([]byte(nil), target[start:end]...)}
		delta.Chunks = append(delta.Chunks, chunk)
	}
	return delta
}

func applyByteDelta(base []byte, delta byteDeltaV1) ([]byte, error) {
	if delta.NewLen < 0 {
		return nil, fmt.Errorf("invalid delta new length: %d", delta.NewLen)
	}
	out := make([]byte, delta.NewLen)
	copy(out, base)
	for _, chunk := range delta.Chunks {
		if chunk.Offset < 0 {
			return nil, fmt.Errorf("invalid delta chunk offset: %d", chunk.Offset)
		}
		end := chunk.Offset + len(chunk.Data)
		if end > len(out) {
			return nil, fmt.Errorf("delta chunk out of bounds: offset=%d size=%d len=%d", chunk.Offset, len(chunk.Data), len(out))
		}
		copy(out[chunk.Offset:end], chunk.Data)
	}
	return out, nil
}

func buildIncrementalMetaOrMutationPayload(baseSeq int64, mutations []CheckpointMutation) ([]byte, string, error) {
	if len(mutations) > 0 {
		payload, err := encodeIncrementalCheckpointPayload(incrementalCheckpointPayloadV1{
			BaseSeq:   baseSeq,
			Kind:      "meta",
			Mutations: cloneCheckpointMutations(mutations),
		})
		if err != nil {
			return nil, "", err
		}
		return payload, sqliteCodecGraphIncDV1, nil
	}
	payload, err := encodeIncrementalCheckpointMeta(incrementalCheckpointMetaV1{BaseSeq: baseSeq})
	if err != nil {
		return nil, "", err
	}
	return payload, sqliteCodecGraphIncV1, nil
}

func cloneCheckpointMutations(in []CheckpointMutation) []CheckpointMutation {
	if len(in) == 0 {
		return nil
	}
	out := make([]CheckpointMutation, 0, len(in))
	for _, mutation := range in {
		out = append(out, CheckpointMutation{
			Type:  mutation.Type,
			Key:   append([]byte(nil), mutation.Key...),
			Value: append([]byte(nil), mutation.Value...),
		})
	}
	return out
}
