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
	sqliteCodecGobV1 = "gob-v1"
)

type SQLiteWAL struct {
	db         *sql.DB
	insertStmt *sql.Stmt
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

	_, err = w.insertStmt.ExecContext(ctx, time.Now().UnixMilli(), sqliteCodecGobV1, payload)
	if err != nil {
		return fmt.Errorf("append wal: %w", err)
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
