package op

import (
	"fmt"
	"sort"
	"strings"
	"sync"
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

func (s StateBatchOp) MutationType() string {
	if s.Type == StateBatchDelete {
		return "delete"
	}
	return "put"
}

func StateBatchOpFromMutation(mType string, key, value []byte) StateBatchOp {
	typ := StateBatchPut
	if strings.EqualFold(strings.TrimSpace(mType), "delete") {
		typ = StateBatchDelete
	}
	return StateBatchOp{
		Type:  typ,
		Key:   cloneBytes(key),
		Value: cloneBytes(value),
	}
}

type StateBackend interface {
	Get(key []byte) ([]byte, bool, error)
	Put(key, value []byte) error
	Delete(key []byte) error
	IterPrefix(prefix []byte, visit func(key, value []byte) error) error
	BatchWrite(ops []StateBatchOp) error
	Reset() error
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

func (m *MemoryStateBackend) Reset() error {
	if m == nil {
		return fmt.Errorf("memory backend is nil")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data = make(map[string][]byte)
	return nil
}

func (m *MemoryStateBackend) Close() error {
	return nil
}

func NewStateBackendFromConfig(enabled bool, backendType, _ string) (StateBackend, error) {
	if !enabled {
		return nil, nil
	}
	switch strings.TrimSpace(strings.ToLower(backendType)) {
	case "", "memory":
		return NewMemoryStateBackend(), nil
	default:
		return nil, fmt.Errorf("unsupported state backend type in minimal runtime: %s", backendType)
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
