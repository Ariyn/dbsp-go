package op

import (
	"path/filepath"
	"testing"
)

func TestMemoryStateBackend_BasicOps(t *testing.T) {
	backend := NewMemoryStateBackend()
	defer backend.Close()

	if err := backend.Put([]byte("a/1"), []byte("v1")); err != nil {
		t.Fatalf("Put failed: %v", err)
	}
	if err := backend.Put([]byte("a/2"), []byte("v2")); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	v, ok, err := backend.Get([]byte("a/1"))
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if !ok || string(v) != "v1" {
		t.Fatalf("unexpected get result: ok=%v val=%q", ok, string(v))
	}

	seen := map[string]string{}
	err = backend.IterPrefix([]byte("a/"), func(k, v []byte) error {
		seen[string(k)] = string(v)
		return nil
	})
	if err != nil {
		t.Fatalf("IterPrefix failed: %v", err)
	}
	if len(seen) != 2 {
		t.Fatalf("expected 2 keys, got %d", len(seen))
	}

	if err := backend.Delete([]byte("a/2")); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
	_, ok, err = backend.Get([]byte("a/2"))
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if ok {
		t.Fatalf("expected deleted key")
	}
}

func TestMemoryStateBackend_BatchWrite(t *testing.T) {
	backend := NewMemoryStateBackend()
	defer backend.Close()

	err := backend.BatchWrite([]StateBatchOp{
		{Type: StateBatchPut, Key: []byte("k/1"), Value: []byte("1")},
		{Type: StateBatchPut, Key: []byte("k/2"), Value: []byte("2")},
		{Type: StateBatchDelete, Key: []byte("k/1")},
	})
	if err != nil {
		t.Fatalf("BatchWrite failed: %v", err)
	}

	_, ok, err := backend.Get([]byte("k/1"))
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if ok {
		t.Fatalf("k/1 should be deleted")
	}
	v, ok, err := backend.Get([]byte("k/2"))
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if !ok || string(v) != "2" {
		t.Fatalf("unexpected k/2 value: ok=%v val=%q", ok, string(v))
	}
}

func TestBoltStateBackend_PersistAndPrefix(t *testing.T) {
	tmp := t.TempDir()
	path := filepath.Join(tmp, "state.db")

	backend, err := NewBoltStateBackend(path)
	if err != nil {
		t.Fatalf("NewBoltStateBackend failed: %v", err)
	}
	if err := backend.BatchWrite([]StateBatchOp{
		{Type: StateBatchPut, Key: []byte("join/left/1"), Value: []byte("L1")},
		{Type: StateBatchPut, Key: []byte("join/right/1"), Value: []byte("R1")},
	}); err != nil {
		t.Fatalf("BatchWrite failed: %v", err)
	}
	if err := backend.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	reopened, err := NewBoltStateBackend(path)
	if err != nil {
		t.Fatalf("reopen failed: %v", err)
	}
	defer reopened.Close()

	v, ok, err := reopened.Get([]byte("join/left/1"))
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if !ok || string(v) != "L1" {
		t.Fatalf("unexpected persisted value: ok=%v val=%q", ok, string(v))
	}

	count := 0
	err = reopened.IterPrefix([]byte("join/"), func(_, _ []byte) error {
		count++
		return nil
	})
	if err != nil {
		t.Fatalf("IterPrefix failed: %v", err)
	}
	if count != 2 {
		t.Fatalf("expected 2 prefix keys, got %d", count)
	}
}

func TestPebbleStateBackend_PersistAndPrefix(t *testing.T) {
	tmp := t.TempDir()
	path := filepath.Join(tmp, "pebble")

	backend, err := NewPebbleStateBackend(path)
	if err != nil {
		t.Fatalf("NewPebbleStateBackend failed: %v", err)
	}
	if err := backend.BatchWrite([]StateBatchOp{
		{Type: StateBatchPut, Key: []byte("join/left/1"), Value: []byte("L1")},
		{Type: StateBatchPut, Key: []byte("join/right/1"), Value: []byte("R1")},
	}); err != nil {
		t.Fatalf("BatchWrite failed: %v", err)
	}
	if err := backend.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	reopened, err := NewPebbleStateBackend(path)
	if err != nil {
		t.Fatalf("reopen failed: %v", err)
	}
	defer reopened.Close()

	v, ok, err := reopened.Get([]byte("join/left/1"))
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if !ok || string(v) != "L1" {
		t.Fatalf("unexpected persisted value: ok=%v val=%q", ok, string(v))
	}

	count := 0
	err = reopened.IterPrefix([]byte("join/"), func(_, _ []byte) error {
		count++
		return nil
	})
	if err != nil {
		t.Fatalf("IterPrefix failed: %v", err)
	}
	if count != 2 {
		t.Fatalf("expected 2 prefix keys, got %d", count)
	}
}

func TestNewStateBackendFromConfig(t *testing.T) {
	if backend, err := NewStateBackendFromConfig(false, "kv", "/tmp/x"); err != nil || backend != nil {
		t.Fatalf("disabled backend should return nil,nil got backend=%v err=%v", backend, err)
	}
	if backend, err := NewStateBackendFromConfig(true, "memory", ""); err != nil || backend == nil {
		t.Fatalf("memory backend init failed backend=%v err=%v", backend, err)
	}
	if backend, err := NewStateBackendFromConfig(true, "pebble", t.TempDir()); err != nil || backend == nil {
		t.Fatalf("pebble backend init failed backend=%v err=%v", backend, err)
	}
	if _, err := NewStateBackendFromConfig(true, "sqlite", "/tmp/x"); err == nil {
		t.Fatalf("sqlite backend should be not implemented")
	}
}

func TestMutationTrackingStateBackend_TracksPutDeleteAndDrain(t *testing.T) {
	inner := NewMemoryStateBackend()
	backend := NewMutationTrackingStateBackend(inner)
	defer backend.Close()

	if err := backend.Put([]byte("k/1"), []byte("v1")); err != nil {
		t.Fatalf("Put failed: %v", err)
	}
	if err := backend.Delete([]byte("k/1")); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	if got := backend.PendingMutationCount(); got != 2 {
		t.Fatalf("expected 2 pending mutations, got %d", got)
	}
	drained := backend.DrainMutations()
	if len(drained) != 2 {
		t.Fatalf("expected 2 drained mutations, got %d", len(drained))
	}
	if drained[0].Type != StateBatchPut || string(drained[0].Key) != "k/1" {
		t.Fatalf("unexpected first mutation: %+v", drained[0])
	}
	if drained[1].Type != StateBatchDelete || string(drained[1].Key) != "k/1" {
		t.Fatalf("unexpected second mutation: %+v", drained[1])
	}
	if got := backend.PendingMutationCount(); got != 0 {
		t.Fatalf("expected 0 pending mutations after drain, got %d", got)
	}
}

func TestMutationTrackingStateBackend_BatchWriteTracksAllOps(t *testing.T) {
	inner := NewMemoryStateBackend()
	backend := NewMutationTrackingStateBackend(inner)
	defer backend.Close()

	err := backend.BatchWrite([]StateBatchOp{
		{Type: StateBatchPut, Key: []byte("k/1"), Value: []byte("v1")},
		{Type: StateBatchPut, Key: []byte("k/2"), Value: []byte("v2")},
		{Type: StateBatchDelete, Key: []byte("k/1")},
	})
	if err != nil {
		t.Fatalf("BatchWrite failed: %v", err)
	}

	drained := backend.DrainMutations()
	if len(drained) != 3 {
		t.Fatalf("expected 3 tracked mutations, got %d", len(drained))
	}
	if drained[0].Type != StateBatchPut || string(drained[0].Key) != "k/1" {
		t.Fatalf("unexpected mutation[0]: %+v", drained[0])
	}
	if drained[2].Type != StateBatchDelete || string(drained[2].Key) != "k/1" {
		t.Fatalf("unexpected mutation[2]: %+v", drained[2])
	}
}
