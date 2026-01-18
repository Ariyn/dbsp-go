package op

import (
	"fmt"

	"github.com/ariyn/dbsp/internal/dbsp/types"
)

// IntegrateApply is the legacy placeholder for an integrate operator.
//
// NOTE: The RFC introduces IntegrateOp as a first-class operator. This helper is
// kept to avoid breaking existing call sites.
func IntegrateApply(state map[any]any, delta types.Batch) (types.Batch, error) {
	// No-op placeholder: real integrate would merge delta into snapshot state.
	return delta, nil
}

// IntegrateOp accumulates a delta stream into a value stream (Z-set snapshot).
//
// Semantics (value stream):
//
//	I[t] = I[t-1] + ΔS[t]
//
// Apply(delta) updates internal state and returns the current snapshot materialized
// as a Batch (tuple,count pairs).
type IntegrateOp struct {
	store *ZSetStore
}

type integrateSnapshotV1 struct {
	Entries map[string]zsetEntryV1
}

type zsetEntryV1 struct {
	Tuple types.Tuple
	Count int64
}

func NewIntegrateOp() *IntegrateOp {
	return &IntegrateOp{store: NewZSetStore()}
}

func (i *IntegrateOp) Ref() ZSetRef {
	return i.store
}

func (i *IntegrateOp) Apply(delta types.Batch) (types.Batch, error) {
	if i.store == nil {
		i.store = NewZSetStore()
	}
	if err := i.store.ApplyDelta(delta); err != nil {
		return nil, err
	}
	return i.store.ToBatch(), nil
}

func (i *IntegrateOp) Snapshot() (any, error) {
	if i == nil || i.store == nil {
		return integrateSnapshotV1{Entries: map[string]zsetEntryV1{}}, nil
	}
	entries := make(map[string]zsetEntryV1, len(i.store.entries))
	for k, e := range i.store.entries {
		if e == nil || e.count == 0 {
			continue
		}
		entries[k] = zsetEntryV1{Tuple: cloneTupleLocal(e.tuple), Count: e.count}
	}
	return integrateSnapshotV1{Entries: entries}, nil
}

func (i *IntegrateOp) Restore(state any) error {
	if i == nil {
		return fmt.Errorf("IntegrateOp is nil")
	}
	s, ok := state.(integrateSnapshotV1)
	if !ok {
		return fmt.Errorf("unexpected snapshot type %T", state)
	}
	if i.store == nil {
		i.store = NewZSetStore()
	}
	if i.store.entries == nil {
		i.store.entries = make(map[string]*zsetEntry)
	} else {
		for k := range i.store.entries {
			delete(i.store.entries, k)
		}
	}
	for k, e := range s.Entries {
		i.store.entries[k] = &zsetEntry{tuple: cloneTupleLocal(e.Tuple), count: e.Count}
	}
	return nil
}
