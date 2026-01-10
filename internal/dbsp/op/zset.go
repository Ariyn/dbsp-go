package op

import (
	"fmt"

	"github.com/ariyn/dbsp/internal/dbsp/types"
)

// ZSetRef is a read-only view of a Z-set (multiset relation).
//
// This is the Value-stream representation in the RFC.
// For now, LookupByKey is implemented via scanning; it is correct but may be O(n).
//
// NOTE: This interface is intentionally minimal; it can be extended later with
// indexed lookups without changing the semantic contract.
type ZSetRef interface {
	// ForEach iterates over all tuples with non-zero counts.
	// If f returns false, iteration stops.
	ForEach(f func(t types.Tuple, count int64) bool)

	// LookupByKey returns all tuples whose keyFn(tuple)==key.
	LookupByKey(key any, keyFn func(types.Tuple) any) []types.TupleDelta

	// ToBatch materializes the Z-set as a delta batch (tuple,count pairs).
	ToBatch() types.Batch
}

type zsetEntry struct {
	tuple types.Tuple
	count int64
}

// ZSetStore is a mutable Z-set store (tuple -> count).
// It is used by IntegrateOp to accumulate delta batches into a Value snapshot.
type ZSetStore struct {
	entries map[string]*zsetEntry
}

func NewZSetStore() *ZSetStore {
	return &ZSetStore{entries: make(map[string]*zsetEntry)}
}

func (s *ZSetStore) ApplyDelta(delta types.Batch) error {
	if s.entries == nil {
		s.entries = make(map[string]*zsetEntry)
	}
	for _, td := range delta {
		tk := stableTupleKey(td.Tuple)
		e, ok := s.entries[tk]
		if !ok {
			if td.Count < 0 {
				return fmt.Errorf("zset underflow for tuple=%v count=%d", td.Tuple, td.Count)
			}
			e = &zsetEntry{tuple: cloneTupleLocal(td.Tuple)}
			s.entries[tk] = e
		}

		e.count += td.Count
		if e.count == 0 {
			delete(s.entries, tk)
			continue
		}
		if e.count < 0 {
			return fmt.Errorf("zset underflow for tuple=%v resultingCount=%d", td.Tuple, e.count)
		}

		// Keep latest tuple materialization.
		e.tuple = cloneTupleLocal(td.Tuple)
	}
	return nil
}

func (s *ZSetStore) ForEach(f func(t types.Tuple, count int64) bool) {
	if s == nil {
		return
	}
	for _, e := range s.entries {
		if e == nil || e.count == 0 {
			continue
		}
		if !f(cloneTupleLocal(e.tuple), e.count) {
			return
		}
	}
}

func (s *ZSetStore) LookupByKey(key any, keyFn func(types.Tuple) any) []types.TupleDelta {
	var out types.Batch
	s.ForEach(func(t types.Tuple, count int64) bool {
		if keyFn == nil {
			return true
		}
		if keyFn(t) == key {
			out = append(out, types.TupleDelta{Tuple: t, Count: count})
		}
		return true
	})
	return out
}

func (s *ZSetStore) ToBatch() types.Batch {
	var out types.Batch
	s.ForEach(func(t types.Tuple, count int64) bool {
		out = append(out, types.TupleDelta{Tuple: t, Count: count})
		return true
	})
	return out
}

func cloneTupleLocal(t types.Tuple) types.Tuple {
	if t == nil {
		return nil
	}
	out := make(types.Tuple, len(t))
	for k, v := range t {
		out[k] = v
	}
	return out
}
