package op

import (
	"sync"

	"github.com/ariyn/dbsp/internal/dbsp/types"
)

// DelayOp is a 1-tick register.
//
// Semantics:
//   out[t] = in[t-1] (with out[0] = seed)
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

func NewDelayOp(seed types.Batch) *DelayOp {
	return &DelayOp{seed: cloneBatch(seed)}
}

func (d *DelayOp) Prev() types.Batch {
	d.mu.Lock()
	defer d.mu.Unlock()
	if !d.initialized {
		d.prev = cloneBatch(d.seed)
		d.initialized = true
	}
	return cloneBatch(d.prev)
}

func (d *DelayOp) SetNext(next types.Batch) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if !d.initialized {
		d.prev = cloneBatch(d.seed)
		d.initialized = true
	}
	d.next = cloneBatch(next)
}

func (d *DelayOp) Commit() {
	d.mu.Lock()
	defer d.mu.Unlock()
	if !d.initialized {
		d.prev = cloneBatch(d.seed)
		d.initialized = true
	}
	d.prev = cloneBatch(d.next)
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

func cloneBatch(b types.Batch) types.Batch {
	if b == nil {
		return nil
	}
	out := make(types.Batch, 0, len(b))
	for _, td := range b {
		out = append(out, types.TupleDelta{Tuple: cloneTuple(td.Tuple), Count: td.Count})
	}
	return out
}

func cloneTuple(t types.Tuple) types.Tuple {
	if t == nil {
		return nil
	}
	out := make(types.Tuple, len(t))
	for k, v := range t {
		out[k] = v
	}
	return out
}
