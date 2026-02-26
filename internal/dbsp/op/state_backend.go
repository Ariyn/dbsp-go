package op

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"go.etcd.io/bbolt"
)

type StateBatchOpType int

const (
	StateBatchPut StateBatchOpType = iota
	StateBatchDelete
)

type StateBatchOp struct {
	Type  StateBatchOpType
	Key   []byte
	Value []byte
}

type StateBackend interface {
	Get(key []byte) ([]byte, bool, error)
	Put(key, value []byte) error
	Delete(key []byte) error
	IterPrefix(prefix []byte, visit func(key, value []byte) error) error
	BatchWrite(ops []StateBatchOp) error
	Close() error
}

type MemoryStateBackend struct {
	mu   sync.RWMutex
	data map[string][]byte
}

func NewMemoryStateBackend() *MemoryStateBackend {
	return &MemoryStateBackend{data: make(map[string][]byte)}
}

func (m *MemoryStateBackend) Get(key []byte) ([]byte, bool, error) {
	if m == nil {
		return nil, false, fmt.Errorf("memory backend is nil")
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	v, ok := m.data[string(key)]
	if !ok {
		return nil, false, nil
	}
	return cloneBytes(v), true, nil
}

func (m *MemoryStateBackend) Put(key, value []byte) error {
	if m == nil {
		return fmt.Errorf("memory backend is nil")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data[string(key)] = cloneBytes(value)
	return nil
}

func (m *MemoryStateBackend) Delete(key []byte) error {
	if m == nil {
		return fmt.Errorf("memory backend is nil")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.data, string(key))
	return nil
}

func (m *MemoryStateBackend) IterPrefix(prefix []byte, visit func(key, value []byte) error) error {
	if m == nil {
		return fmt.Errorf("memory backend is nil")
	}
	if visit == nil {
		return fmt.Errorf("visit callback is nil")
	}

	m.mu.RLock()
	keys := make([]string, 0, len(m.data))
	for key := range m.data {
		if strings.HasPrefix(key, string(prefix)) {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	rows := make([]struct {
		k []byte
		v []byte
	}, 0, len(keys))
	for _, key := range keys {
		rows = append(rows, struct {
			k []byte
			v []byte
		}{k: []byte(key), v: cloneBytes(m.data[key])})
	}
	m.mu.RUnlock()

	for _, row := range rows {
		if err := visit(row.k, row.v); err != nil {
			return err
		}
	}
	return nil
}

func (m *MemoryStateBackend) BatchWrite(ops []StateBatchOp) error {
	if m == nil {
		return fmt.Errorf("memory backend is nil")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, op := range ops {
		switch op.Type {
		case StateBatchPut:
			m.data[string(op.Key)] = cloneBytes(op.Value)
		case StateBatchDelete:
			delete(m.data, string(op.Key))
		default:
			return fmt.Errorf("unknown batch op type: %d", op.Type)
		}
	}
	return nil
}

func (m *MemoryStateBackend) Close() error {
	return nil
}

type BoltStateBackend struct {
	db     *bbolt.DB
	bucket []byte
}

func NewBoltStateBackend(path string) (*BoltStateBackend, error) {
	if strings.TrimSpace(path) == "" {
		return nil, fmt.Errorf("state backend path is empty")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("creating state backend dir: %w", err)
	}
	db, err := bbolt.Open(path, 0o600, nil)
	if err != nil {
		return nil, fmt.Errorf("opening bolt state backend: %w", err)
	}
	backend := &BoltStateBackend{db: db, bucket: []byte("state")}
	if err := backend.db.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(backend.bucket)
		return err
	}); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("initializing bolt bucket: %w", err)
	}
	return backend, nil
}

func (b *BoltStateBackend) Get(key []byte) ([]byte, bool, error) {
	if b == nil || b.db == nil {
		return nil, false, fmt.Errorf("bolt backend is nil")
	}
	var out []byte
	err := b.db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(b.bucket)
		if bucket == nil {
			return nil
		}
		v := bucket.Get(key)
		if v == nil {
			return nil
		}
		out = cloneBytes(v)
		return nil
	})
	if err != nil {
		return nil, false, err
	}
	if out == nil {
		return nil, false, nil
	}
	return out, true, nil
}

func (b *BoltStateBackend) Put(key, value []byte) error {
	return b.BatchWrite([]StateBatchOp{{Type: StateBatchPut, Key: key, Value: value}})
}

func (b *BoltStateBackend) Delete(key []byte) error {
	return b.BatchWrite([]StateBatchOp{{Type: StateBatchDelete, Key: key}})
}

func (b *BoltStateBackend) IterPrefix(prefix []byte, visit func(key, value []byte) error) error {
	if b == nil || b.db == nil {
		return fmt.Errorf("bolt backend is nil")
	}
	if visit == nil {
		return fmt.Errorf("visit callback is nil")
	}
	return b.db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(b.bucket)
		if bucket == nil {
			return nil
		}
		cursor := bucket.Cursor()
		for k, v := cursor.Seek(prefix); k != nil; k, v = cursor.Next() {
			if !bytes.HasPrefix(k, prefix) {
				break
			}
			if err := visit(cloneBytes(k), cloneBytes(v)); err != nil {
				return err
			}
		}
		return nil
	})
}

func (b *BoltStateBackend) BatchWrite(ops []StateBatchOp) error {
	if b == nil || b.db == nil {
		return fmt.Errorf("bolt backend is nil")
	}
	return b.db.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(b.bucket)
		if bucket == nil {
			return fmt.Errorf("state bucket missing")
		}
		for _, op := range ops {
			switch op.Type {
			case StateBatchPut:
				if err := bucket.Put(op.Key, op.Value); err != nil {
					return err
				}
			case StateBatchDelete:
				if err := bucket.Delete(op.Key); err != nil {
					return err
				}
			default:
				return fmt.Errorf("unknown batch op type: %d", op.Type)
			}
		}
		return nil
	})
}

func (b *BoltStateBackend) Close() error {
	if b == nil || b.db == nil {
		return nil
	}
	return b.db.Close()
}

func NewStateBackendFromConfig(enabled bool, backendType, path string) (StateBackend, error) {
	if !enabled {
		return nil, nil
	}
	switch strings.TrimSpace(strings.ToLower(backendType)) {
	case "", "memory":
		return NewMemoryStateBackend(), nil
	case "kv", "bolt", "bbolt":
		return NewBoltStateBackend(path)
	case "sqlite":
		return nil, fmt.Errorf("sqlite state backend is not implemented yet")
	default:
		return nil, fmt.Errorf("unsupported state backend type: %s", backendType)
	}
}

func cloneBytes(in []byte) []byte {
	if in == nil {
		return nil
	}
	out := make([]byte, len(in))
	copy(out, in)
	return out
}
