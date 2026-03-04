package op

import (
	"fmt"
	"sync"

	"github.com/ariyn/dbsp/internal/dbsp/types"
)

// DelayOp is a 1-tick register.
//
// Semantics:
//
//	out[t] = in[t-1] (with out[0] = seed)
//
// DelayOp state updates are finalized by the cyclic executor via Commit().
// When used outside ExecuteTickCyclic, its state will not advance.
type DelayOp struct {
	mu sync.Mutex

	seed types.Batch
	prev types.Batch
	next types.Batch

	initialized bool
}

type delaySnapshotV1 struct {
	Seed        types.Batch
	Prev        types.Batch
	Next        types.Batch
	Initialized bool
}

func (d *DelayOp) Snapshot() (any, error) {
	if d == nil {
		return delaySnapshotV1{}, nil
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	return delaySnapshotV1{
		Seed:        types.CloneBatch(d.seed),
		Prev:        types.CloneBatch(d.prev),
		Next:        types.CloneBatch(d.next),
		Initialized: d.initialized,
	}, nil
}

func (d *DelayOp) Restore(state any) error {
	if d == nil {
		return fmt.Errorf("DelayOp is nil")
	}
	s, ok := state.(delaySnapshotV1)
	if !ok {
		return fmt.Errorf("unexpected snapshot type %T", state)
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.seed = types.CloneBatch(s.Seed)
	d.prev = types.CloneBatch(s.Prev)
	d.next = types.CloneBatch(s.Next)
	d.initialized = s.Initialized
	return nil
}

func NewDelayOp(seed types.Batch) *DelayOp {
	return &DelayOp{seed: types.CloneBatch(seed)}
}

func (d *DelayOp) Prev() types.Batch {
	d.mu.Lock()
	defer d.mu.Unlock()
	if !d.initialized {
		d.prev = types.CloneBatch(d.seed)
		d.initialized = true
	}
	return types.CloneBatch(d.prev)
}

func (d *DelayOp) SetNext(next types.Batch) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if !d.initialized {
		d.prev = types.CloneBatch(d.seed)
		d.initialized = true
	}
	d.next = types.CloneBatch(next)
}

func (d *DelayOp) Commit() {
	d.mu.Lock()
	defer d.mu.Unlock()
	if !d.initialized {
		d.prev = types.CloneBatch(d.seed)
		d.initialized = true
	}
	d.prev = types.CloneBatch(d.next)
	d.next = nil
}

// Apply satisfies Operator. For register semantics you should use ExecuteTickCyclic,
// which controls when SetNext/Commit happens. Apply returns the current Prev().
func (d *DelayOp) Apply(batch types.Batch) (types.Batch, error) {
	// Best-effort behavior: capture input as next and return previous.
	// The cyclic executor will call SetNext + Commit explicitly.
	d.SetNext(batch)
	return d.Prev(), nil
}
