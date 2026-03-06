package wal

import (
	"bytes"
	"os"
	"testing"
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
