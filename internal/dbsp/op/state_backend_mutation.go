package op

import "sync"

type StateMutationTracker interface {
	DrainMutations() []StateBatchOp
	PendingMutationCount() int
	RestoreMutations([]StateBatchOp)
}

type MutationTrackingStateBackend struct {
	inner StateBackend
	mu    sync.Mutex
	log   []StateBatchOp
}

func NewMutationTrackingStateBackend(inner StateBackend) *MutationTrackingStateBackend {
	if inner == nil {
		return nil
	}
	return &MutationTrackingStateBackend{inner: inner}
}

func (m *MutationTrackingStateBackend) Get(key []byte) ([]byte, bool, error) {
	return m.inner.Get(key)
}

func (m *MutationTrackingStateBackend) Put(key, value []byte) error {
	if err := m.inner.Put(key, value); err != nil {
		return err
	}
	m.appendLog(StateBatchOp{Type: StateBatchPut, Key: cloneBytes(key), Value: cloneBytes(value)})
	return nil
}

func (m *MutationTrackingStateBackend) Delete(key []byte) error {
	if err := m.inner.Delete(key); err != nil {
		return err
	}
	m.appendLog(StateBatchOp{Type: StateBatchDelete, Key: cloneBytes(key)})
	return nil
}

func (m *MutationTrackingStateBackend) IterPrefix(prefix []byte, visit func(key, value []byte) error) error {
	return m.inner.IterPrefix(prefix, visit)
}

func (m *MutationTrackingStateBackend) BatchWrite(ops []StateBatchOp) error {
	if err := m.inner.BatchWrite(ops); err != nil {
		return err
	}
	cloned := make([]StateBatchOp, 0, len(ops))
	for _, op := range ops {
		cloned = append(cloned, StateBatchOp{Type: op.Type, Key: cloneBytes(op.Key), Value: cloneBytes(op.Value)})
	}
	m.appendLogs(cloned)
	return nil
}

func (m *MutationTrackingStateBackend) Close() error {
	if m == nil || m.inner == nil {
		return nil
	}
	return m.inner.Close()
}

func (m *MutationTrackingStateBackend) PendingMutationCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.log)
}

func (m *MutationTrackingStateBackend) DrainMutations() []StateBatchOp {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.log) == 0 {
		return nil
	}
	out := make([]StateBatchOp, len(m.log))
	copy(out, m.log)
	m.log = m.log[:0]
	return out
}

func (m *MutationTrackingStateBackend) RestoreMutations(ops []StateBatchOp) {
	if len(ops) == 0 {
		return
	}
	cloned := make([]StateBatchOp, 0, len(ops))
	for _, op := range ops {
		cloned = append(cloned, StateBatchOp{Type: op.Type, Key: cloneBytes(op.Key), Value: cloneBytes(op.Value)})
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.log = append(cloned, m.log...)
}

func (m *MutationTrackingStateBackend) appendLog(op StateBatchOp) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.log = append(m.log, op)
}

func (m *MutationTrackingStateBackend) appendLogs(ops []StateBatchOp) {
	if len(ops) == 0 {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.log = append(m.log, ops...)
}
