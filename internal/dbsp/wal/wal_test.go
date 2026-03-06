package wal

import (
	"bytes"
	"os"
	"testing"

	"github.com/ariyn/dbsp/internal/dbsp/types"
)

func TestWALWriteAndRead(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "wal_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	writer, err := NewLogWriter(tmpDir, 1024, 4096)
	if err != nil {
		t.Fatal(err)
	}

	payload1 := []byte("hello world")
	payload2 := []byte("foobar")

	if err := writer.Append(&Record{Payload: payload1}); err != nil {
		t.Fatalf("failed to append 1: %v", err)
	}
	if err := writer.Append(&Record{Payload: payload2}); err != nil {
		t.Fatalf("failed to append 2: %v", err)
	}

	// Flush and rotate to ensure data is on disk.
	writer.mu.Lock()
	writer.buffered.Flush()
	writer.mu.Unlock()
	writer.Close()

	// Read back
	reader, err := NewLogReader(tmpDir)
	if err != nil {
		t.Fatal(err)
	}

	var readPayloads [][]byte
	err = reader.Replay(func(rec *Record) error {
		readPayloads = append(readPayloads, rec.Payload)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	if len(readPayloads) != 2 {
		t.Errorf("expected 2 records, got %d", len(readPayloads))
	}
	if !bytes.Equal(readPayloads[0], payload1) {
		t.Errorf("expected %s, got %s", payload1, readPayloads[0])
	}
}

func TestWALReplayFromSequence(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "wal_replay_from_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	writer, err := NewLogWriter(tmpDir, 1024, 4096)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()

	if err := writer.Append(&Record{Sequence: 10, Payload: []byte("a")}); err != nil {
		t.Fatal(err)
	}
	if err := writer.Append(&Record{Sequence: 20, Payload: []byte("b")}); err != nil {
		t.Fatal(err)
	}
	if err := writer.Append(&Record{Sequence: 30, Payload: []byte("c")}); err != nil {
		t.Fatal(err)
	}

	reader, err := NewLogReader(tmpDir)
	if err != nil {
		t.Fatal(err)
	}

	var seqs []uint64
	err = reader.ReplayFrom(20, func(rec *Record) error {
		seqs = append(seqs, rec.Sequence)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(seqs) != 1 || seqs[0] != 30 {
		t.Fatalf("expected only seq 30, got %v", seqs)
	}
}

func TestReplayCursorRoundTrip(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "wal_cursor_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	seq, err := LoadReplayCursor(tmpDir)
	if err != nil {
		t.Fatal(err)
	}
	if seq != 0 {
		t.Fatalf("expected empty cursor to read as 0, got %d", seq)
	}

	if err := SaveReplayCursor(tmpDir, 42); err != nil {
		t.Fatal(err)
	}
	seq, err = LoadReplayCursor(tmpDir)
	if err != nil {
		t.Fatal(err)
	}
	if seq != 42 {
		t.Fatalf("expected cursor 42, got %d", seq)
	}
}

func TestBatchGobEncodeDecodeRoundTrip(t *testing.T) {
	in := types.Batch{{Tuple: types.Tuple{"a": int64(1), "b": "x"}, Count: 1}}
	payload, err := EncodeBatchGobV1(in)
	if err != nil {
		t.Fatalf("encode batch: %v", err)
	}
	out, err := DecodeBatchGobV1(payload)
	if err != nil {
		t.Fatalf("decode batch: %v", err)
	}
	if len(out) != 1 {
		t.Fatalf("expected 1 decoded row, got %d", len(out))
	}
	if got := out[0].Tuple["a"].(int64); got != 1 {
		t.Fatalf("expected a=1, got %d", got)
	}
	if got := out[0].Tuple["b"].(string); got != "x" {
		t.Fatalf("expected b=x, got %q", got)
	}
	if out[0].Count != 1 {
		t.Fatalf("expected count 1, got %d", out[0].Count)
	}
}

func TestWALAppendRefAndLoadRecord(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "wal_record_ref")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	writer, err := NewLogWriter(tmpDir, 1024, 4096)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()

	ref, err := writer.AppendRef(&Record{Type: RecordTypeBatch, Sequence: 99, Payload: []byte("payload")})
	if err != nil {
		t.Fatalf("append ref: %v", err)
	}
	rec, err := LoadRecord(ref)
	if err != nil {
		t.Fatalf("load record: %v", err)
	}
	if rec.Sequence != 99 || rec.Type != RecordTypeBatch || !bytes.Equal(rec.Payload, []byte("payload")) {
		t.Fatalf("unexpected record loaded from ref: %+v", rec)
	}
	reader, err := NewLogReader(tmpDir)
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	err = reader.ReplayRefsFrom(0, func(got *RecordRef) error {
		count++
		if got.Sequence != 99 {
			t.Fatalf("expected seq 99, got %d", got.Sequence)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("expected 1 record ref, got %d", count)
	}
}
