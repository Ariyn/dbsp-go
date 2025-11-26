package op

import (
	"fmt"
	"sort"

	"github.com/ariyn/dbsp/internal/dbsp/types"
)

// SortedMultiset is a monoid structure that maintains a sorted collection
// of values with their multiplicities. This enables incremental MIN/MAX
// computation with support for insertions and deletions.
//
// Monoid properties:
// - Identity: empty multiset
// - Associative: union of multisets
// - Invertible: supports both Add (count > 0) and Remove (count < 0)
type SortedMultiset struct {
	// values maps each distinct value to its count (multiplicity)
	values map[string]int64
	// sorted maintains the sorted order of distinct values
	sorted []string
}

// NewSortedMultiset creates an empty SortedMultiset (identity element).
func NewSortedMultiset() SortedMultiset {
	return SortedMultiset{
		values: make(map[string]int64),
		sorted: []string{},
	}
}

// Add inserts a value with the given count (multiplicity).
// Positive count adds, negative count removes.
func (s *SortedMultiset) Add(value any, count int64) {
	if count == 0 {
		return
	}

	key := fmt.Sprintf("%v", value)
	oldCount := s.values[key]
	newCount := oldCount + count

	if newCount <= 0 {
		// Remove value completely
		delete(s.values, key)
		// Remove from sorted list
		for i, v := range s.sorted {
			if v == key {
				s.sorted = append(s.sorted[:i], s.sorted[i+1:]...)
				break
			}
		}
	} else {
		// Update count
		s.values[key] = newCount

		// If this is a new value, insert it in sorted order
		if oldCount == 0 {
			s.insertSorted(key)
		}
	}
}

// insertSorted inserts a key into the sorted list maintaining order.
func (s *SortedMultiset) insertSorted(key string) {
	pos := sort.Search(len(s.sorted), func(i int) bool {
		return s.sorted[i] >= key
	})
	// Insert at position
	s.sorted = append(s.sorted, "")
	copy(s.sorted[pos+1:], s.sorted[pos:])
	s.sorted[pos] = key
}

// Min returns the minimum value, or nil if the multiset is empty.
func (s *SortedMultiset) Min() any {
	if len(s.sorted) == 0 {
		return nil
	}
	return s.sorted[0]
}

// Max returns the maximum value, or nil if the multiset is empty.
func (s *SortedMultiset) Max() any {
	if len(s.sorted) == 0 {
		return nil
	}
	return s.sorted[len(s.sorted)-1]
}

// IsEmpty returns true if the multiset contains no values.
func (s *SortedMultiset) IsEmpty() bool {
	return len(s.sorted) == 0
}

// Combine merges another multiset into this one (monoid operation).
func (s SortedMultiset) Combine(other SortedMultiset) SortedMultiset {
	result := NewSortedMultiset()

	// Add all values from s
	for key, count := range s.values {
		result.values[key] = count
	}

	// Combine with values from other
	for key, count := range other.values {
		result.values[key] += count
	}

	// Rebuild sorted list
	for key := range result.values {
		if result.values[key] > 0 {
			result.sorted = append(result.sorted, key)
		}
	}
	sort.Strings(result.sorted)

	return result
}

// MinAgg computes the minimum value using a SortedMultiset monoid.
// It supports incremental updates including deletions.
type MinAgg struct {
	ColName string // Column to compute MIN over
}

func (m *MinAgg) Apply(prev any, td types.TupleDelta) (any, *types.TupleDelta) {
	// Extract or initialize monoid state
	var monoid SortedMultiset
	if prev != nil {
		var ok bool
		monoid, ok = prev.(SortedMultiset)
		if !ok {
			monoid = NewSortedMultiset()
		}
	} else {
		monoid = NewSortedMultiset()
	}

	// Get old min
	oldMin := monoid.Min()

	// Extract value from tuple
	col := m.ColName
	if col == "" {
		col = "v"
	}
	value, ok := td.Tuple[col]
	if !ok || value == nil {
		return monoid, nil
	}

	// Update monoid
	monoid.Add(value, td.Count)

	// Get new min
	newMin := monoid.Min()

	// Generate output delta if min changed
	if oldMin != newMin {
		outT := types.Tuple{"min": newMin}
		if newMin == nil {
			outT["min"] = nil
		}
		out := &types.TupleDelta{Tuple: outT, Count: 1}
		return monoid, out
	}

	return monoid, nil
}

// MaxAgg computes the maximum value using a SortedMultiset monoid.
// It supports incremental updates including deletions.
type MaxAgg struct {
	ColName string // Column to compute MAX over
}

func (m *MaxAgg) Apply(prev any, td types.TupleDelta) (any, *types.TupleDelta) {
	// Extract or initialize monoid state
	var monoid SortedMultiset
	if prev != nil {
		var ok bool
		monoid, ok = prev.(SortedMultiset)
		if !ok {
			monoid = NewSortedMultiset()
		}
	} else {
		monoid = NewSortedMultiset()
	}

	// Get old max
	oldMax := monoid.Max()

	// Extract value from tuple
	col := m.ColName
	if col == "" {
		col = "v"
	}
	value, ok := td.Tuple[col]
	if !ok || value == nil {
		return monoid, nil
	}

	// Update monoid
	monoid.Add(value, td.Count)

	// Get new max
	newMax := monoid.Max()

	// Generate output delta if max changed
	if oldMax != newMax {
		outT := types.Tuple{"max": newMax}
		if newMax == nil {
			outT["max"] = nil
		}
		out := &types.TupleDelta{Tuple: outT, Count: 1}
		return monoid, out
	}

	return monoid, nil
}
