package op

import (
	"sort"

	"github.com/ariyn/dbsp/internal/dbsp/types"
)

// SortOp sorts a batch by specified columns
type SortOp struct {
	OrderColumns []string // Columns to sort by
	Descending   []bool   // Whether each column is descending
}

// NewSortOp creates a new sort operator
func NewSortOp(orderColumns []string, descending []bool) *SortOp {
	if len(descending) == 0 {
		descending = make([]bool, len(orderColumns))
	}
	return &SortOp{
		OrderColumns: orderColumns,
		Descending:   descending,
	}
}

// Apply sorts the batch
func (s *SortOp) Apply(batch types.Batch) (types.Batch, error) {
	if len(s.OrderColumns) == 0 {
		return batch, nil
	}

	// Create a copy to avoid modifying original
	result := make(types.Batch, len(batch))
	copy(result, batch)

	// Sort by order columns
	sort.SliceStable(result, func(i, j int) bool {
		for idx, col := range s.OrderColumns {
			vi := result[i].Tuple[col]
			vj := result[j].Tuple[col]

			cmp := compareValues(vi, vj)
			
			// Apply descending if specified
			if len(s.Descending) > idx && s.Descending[idx] {
				cmp = -cmp
			}

			if cmp != 0 {
				return cmp < 0
			}
		}
		return false
	})

	return result, nil
}

// LimitOp limits the number of output tuples
type LimitOp struct {
	Limit  int64 // Maximum number of tuples
	Offset int64 // Number of tuples to skip
}

// NewLimitOp creates a new limit operator
func NewLimitOp(limit, offset int64) *LimitOp {
	return &LimitOp{
		Limit:  limit,
		Offset: offset,
	}
}

// Apply applies limit and offset to the batch
func (l *LimitOp) Apply(batch types.Batch) (types.Batch, error) {
	start := int(l.Offset)
	if start < 0 {
		start = 0
	}
	if start >= len(batch) {
		return types.Batch{}, nil
	}

	end := start + int(l.Limit)
	if l.Limit < 0 || end > len(batch) {
		end = len(batch)
	}

	return batch[start:end], nil
}

// TopKOp maintains top-K elements efficiently (incremental)
type TopKOp struct {
	K            int
	OrderColumn  string
	Descending   bool
	state        []types.TupleDelta // Current top-K
	stateChanged bool
}

// NewTopKOp creates a new top-K operator
func NewTopKOp(k int, orderColumn string, descending bool) *TopKOp {
	return &TopKOp{
		K:           k,
		OrderColumn: orderColumn,
		Descending:  descending,
		state:       make([]types.TupleDelta, 0, k),
	}
}

// Apply maintains top-K incrementally
func (t *TopKOp) Apply(batch types.Batch) (types.Batch, error) {
	t.stateChanged = false

	// Add new elements to state
	for _, td := range batch {
		t.state = append(t.state, td)
		t.stateChanged = true
	}

	if !t.stateChanged {
		return types.Batch{}, nil
	}

	// Sort and keep top K
	sort.SliceStable(t.state, func(i, j int) bool {
		vi := t.state[i].Tuple[t.OrderColumn]
		vj := t.state[j].Tuple[t.OrderColumn]
		cmp := compareValues(vi, vj)
		if t.Descending {
			cmp = -cmp
		}
		return cmp < 0
	})

	if len(t.state) > t.K {
		t.state = t.state[:t.K]
	}

	// Return current top-K as delta
	return t.state, nil
}

// GetState returns the current top-K state
func (t *TopKOp) GetState() types.Batch {
	return t.state
}
