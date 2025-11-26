package op

import (
	"fmt"
	"sort"

	"github.com/ariyn/dbsp/internal/dbsp/types"
)

// OrderedBuffer is a monoid that maintains a sorted collection of rows.
// This enables incremental LAG computation with support for insertions and deletions.
//
// Monoid properties:
// - Identity: empty buffer
// - Associative: merge and sort
// - Invertible: supports both Add (count > 0) and Remove (count < 0)
type OrderedBuffer struct {
	// entries maintains the sorted order of rows by ORDER BY column
	entries []BufferEntry
	// orderByCol is the column name used for sorting
	orderByCol string
}

// BufferEntry represents a single row in the buffer with its order value and multiplicity
type BufferEntry struct {
	OrderValue any         // Value of ORDER BY column
	Tuple      types.Tuple // Original tuple
	Count      int64       // Multiplicity (for duplicate handling)
}

// NewOrderedBuffer creates an empty OrderedBuffer (identity element)
func NewOrderedBuffer(orderByCol string) OrderedBuffer {
	return OrderedBuffer{
		entries:    []BufferEntry{},
		orderByCol: orderByCol,
	}
}

// Add inserts or removes a row with the given count (multiplicity).
// Returns the position where the row was inserted/updated and affected positions.
func (o *OrderedBuffer) Add(tuple types.Tuple, count int64) (insertPos int, affected []int) {
	if count == 0 {
		return -1, nil
	}

	orderValue, ok := tuple[o.orderByCol]
	if !ok {
		return -1, nil
	}

	// Find position for this order value
	pos := o.findPosition(orderValue, tuple)

	if pos < len(o.entries) && o.isSameTuple(o.entries[pos].Tuple, tuple) {
		// Update existing entry
		o.entries[pos].Count += count
		if o.entries[pos].Count <= 0 {
			// Remove entry
			o.entries = append(o.entries[:pos], o.entries[pos+1:]...)
			// Next row is affected (its LAG changed)
			if pos < len(o.entries) {
				affected = append(affected, pos)
			}
			return pos, affected
		}
		// Count updated but entry still exists - no LAG changes
		return pos, nil
	}

	if count < 0 {
		// Trying to delete non-existent entry
		return -1, nil
	}

	// Insert new entry
	newEntry := BufferEntry{
		OrderValue: orderValue,
		Tuple:      tuple,
		Count:      count,
	}
	o.entries = append(o.entries, BufferEntry{})
	copy(o.entries[pos+1:], o.entries[pos:])
	o.entries[pos] = newEntry

	// Next row is affected (its LAG changed)
	if pos+1 < len(o.entries) {
		affected = append(affected, pos+1)
	}

	return pos, affected
}

// findPosition finds the insertion position for the given order value
func (o *OrderedBuffer) findPosition(orderValue any, tuple types.Tuple) int {
	return sort.Search(len(o.entries), func(i int) bool {
		cmp := compareValues(o.entries[i].OrderValue, orderValue)
		if cmp > 0 {
			return true
		}
		if cmp == 0 {
			// Same order value - use tuple comparison for stability
			return compareTuples(o.entries[i].Tuple, tuple) >= 0
		}
		return false
	})
}

// isSameTuple checks if two tuples are identical
func (o *OrderedBuffer) isSameTuple(t1, t2 types.Tuple) bool {
	if len(t1) != len(t2) {
		return false
	}
	for k, v1 := range t1 {
		v2, ok := t2[k]
		if !ok || v1 != v2 {
			return false
		}
	}
	return true
}

// GetLagValue returns the LAG value for the row at the given position
func (o *OrderedBuffer) GetLagValue(pos int, offset int, lagCol string) any {
	if pos < offset || pos >= len(o.entries) {
		return nil
	}
	lagPos := pos - offset
	if lagPos < 0 {
		return nil
	}
	return o.entries[lagPos].Tuple[lagCol]
}

// GetEntry returns the entry at the given position
func (o *OrderedBuffer) GetEntry(pos int) *BufferEntry {
	if pos < 0 || pos >= len(o.entries) {
		return nil
	}
	return &o.entries[pos]
}

// Len returns the number of entries in the buffer
func (o *OrderedBuffer) Len() int {
	return len(o.entries)
}

// compareValues compares two values for ordering
func compareValues(a, b any) int {
	// Handle nil
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		return -1
	}
	if b == nil {
		return 1
	}

	// Numeric comparison
	switch av := a.(type) {
	case int:
		switch bv := b.(type) {
		case int:
			return compareInt(av, bv)
		case int64:
			return compareInt64(int64(av), bv)
		case float64:
			return compareFloat64(float64(av), bv)
		}
	case int64:
		switch bv := b.(type) {
		case int:
			return compareInt64(av, int64(bv))
		case int64:
			return compareInt64(av, bv)
		case float64:
			return compareFloat64(float64(av), bv)
		}
	case float64:
		switch bv := b.(type) {
		case int:
			return compareFloat64(av, float64(bv))
		case int64:
			return compareFloat64(av, float64(bv))
		case float64:
			return compareFloat64(av, bv)
		}
	case string:
		if bv, ok := b.(string); ok {
			if av < bv {
				return -1
			}
			if av > bv {
				return 1
			}
			return 0
		}
	}

	// Fallback: string comparison
	aStr := fmt.Sprintf("%v", a)
	bStr := fmt.Sprintf("%v", b)
	if aStr < bStr {
		return -1
	}
	if aStr > bStr {
		return 1
	}
	return 0
}

func compareInt(a, b int) int {
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	return 0
}

func compareInt64(a, b int64) int {
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	return 0
}

func compareFloat64(a, b float64) int {
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	return 0
}

// compareTuples provides a stable ordering for tuples with same order value
func compareTuples(t1, t2 types.Tuple) int {
	// Simple comparison: compare string representations
	s1 := fmt.Sprintf("%v", t1)
	s2 := fmt.Sprintf("%v", t2)
	if s1 < s2 {
		return -1
	}
	if s1 > s2 {
		return 1
	}
	return 0
}

// LagAgg computes the LAG window function using an OrderedBuffer monoid.
// It supports incremental updates including deletions.
//
// LAG(col, offset) OVER (PARTITION BY ... ORDER BY orderCol)
type LagAgg struct {
	OrderByCol string // Column to order by
	LagCol     string // Column to get LAG value from
	Offset     int    // LAG offset (default 1)
	OutputCol  string // Output column name for LAG result
}

// LagMonoid is the monoid state for LAG aggregation
type LagMonoid struct {
	Buffer OrderedBuffer
}

func (l *LagAgg) Apply(prev any, td types.TupleDelta) (any, *types.TupleDelta) {
	// Extract or initialize monoid state
	var monoid LagMonoid
	if prev != nil {
		var ok bool
		monoid, ok = prev.(LagMonoid)
		if !ok {
			monoid = LagMonoid{
				Buffer: NewOrderedBuffer(l.OrderByCol),
			}
		}
	} else {
		monoid = LagMonoid{
			Buffer: NewOrderedBuffer(l.OrderByCol),
		}
	}

	// Get LAG column name
	lagCol := l.LagCol
	if lagCol == "" {
		lagCol = "value"
	}

	// Get offset
	offset := l.Offset
	if offset <= 0 {
		offset = 1
	}

	// Update buffer and get affected positions
	insertPos, affected := monoid.Buffer.Add(td.Tuple, td.Count)
	if insertPos < 0 {
		return monoid, nil
	}

	// Generate output deltas for affected rows
	var outDeltas types.Batch

	// The inserted/updated row itself
	entry := monoid.Buffer.GetEntry(insertPos)
	if entry != nil && td.Count > 0 {
		lagValue := monoid.Buffer.GetLagValue(insertPos, offset, lagCol)
		outTuple := copyTuple(entry.Tuple)
		outTuple[l.getOutputCol()] = lagValue
		outDeltas = append(outDeltas, types.TupleDelta{
			Tuple: outTuple,
			Count: 1,
		})
	}

	// Affected downstream rows (their LAG values changed)
	for _, pos := range affected {
		entry := monoid.Buffer.GetEntry(pos)
		if entry == nil {
			continue
		}

		oldLagValue := td.Tuple[lagCol] // Previous LAG was from deleted/inserted row
		newLagValue := monoid.Buffer.GetLagValue(pos, offset, lagCol)

		if oldLagValue != newLagValue {
			// Emit delta: cancel old value, add new value
			outTuple := copyTuple(entry.Tuple)

			// Cancel old LAG value
			if oldLagValue != nil {
				outTuple[l.getOutputCol()] = oldLagValue
				outDeltas = append(outDeltas, types.TupleDelta{
					Tuple: outTuple,
					Count: -1,
				})
			}

			// Add new LAG value
			outTuple[l.getOutputCol()] = newLagValue
			outDeltas = append(outDeltas, types.TupleDelta{
				Tuple: outTuple,
				Count: 1,
			})
		}
	}

	// Return single delta if only one, otherwise need to merge
	if len(outDeltas) == 0 {
		return monoid, nil
	}
	if len(outDeltas) == 1 {
		return monoid, &outDeltas[0]
	}

	// Multiple deltas - return first one for now
	// In full implementation, would need to batch these
	return monoid, &outDeltas[0]
}

func (l *LagAgg) getOutputCol() string {
	if l.OutputCol != "" {
		return l.OutputCol
	}
	return "lag_" + l.LagCol
}

func copyTuple(t types.Tuple) types.Tuple {
	result := make(types.Tuple, len(t))
	for k, v := range t {
		result[k] = v
	}
	return result
}
