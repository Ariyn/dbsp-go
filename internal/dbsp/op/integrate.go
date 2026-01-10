package op

import "github.com/ariyn/dbsp/internal/dbsp/types"

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
//   I[t] = I[t-1] + ΔS[t]
//
// Apply(delta) updates internal state and returns the current snapshot materialized
// as a Batch (tuple,count pairs).
type IntegrateOp struct {
	store *ZSetStore
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
