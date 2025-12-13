package op

import (
	"fmt"

	"github.com/ariyn/dbsp/internal/dbsp/types"
)

// AggFunc applies a TupleDelta to previous aggregate value and returns new value
// and an optional TupleDelta describing the aggregate's output delta.
type AggFunc interface {
	Apply(prev any, td types.TupleDelta) (new any, outDelta *types.TupleDelta)
}

// GroupAggOp maintains aggregate state per key.
type GroupAggOp struct {
	KeyFn   func(types.Tuple) any
	AggInit func() any
	AggFn   AggFunc

	state      map[any]any
	KeyColName string // Optional: name of the key column to include in output
}

func NewGroupAggOp(keyFn func(types.Tuple) any, aggInit func() any, aggFn AggFunc) *GroupAggOp {
	return &GroupAggOp{KeyFn: keyFn, AggInit: aggInit, AggFn: aggFn, state: make(map[any]any)}
}

func (g *GroupAggOp) SetKeyColName(name string) {
	g.KeyColName = name
}

func (g *GroupAggOp) Apply(batch types.Batch) (types.Batch, error) {
	var out types.Batch
	if g.state == nil {
		g.state = make(map[any]any)
	}
	for _, td := range batch {
		key := g.KeyFn(td.Tuple)
		prev, ok := g.state[key]
		if !ok {
			prev = g.AggInit()
		}
		newVal, outDelta := g.AggFn.Apply(prev, td)
		g.state[key] = newVal
		if outDelta != nil {
			// If KeyColName is set, inject the key into the output tuple
			if g.KeyColName != "" {
				outDelta.Tuple[g.KeyColName] = key
			}
			out = append(out, *outDelta)
		}
	}
	return out, nil
}

// State returns a copy of the internal aggregate state (for testing/inspection).
func (g *GroupAggOp) State() map[any]any {
	copy := make(map[any]any, len(g.state))
	for k, v := range g.state {
		copy[k] = v
	}
	return copy
}

// SumAgg is a simple AggFunc that sums a numeric field multiplied by Count.
type SumAgg struct {
	ColName string // Column to sum (defaults to "v" if empty)
}

func (s *SumAgg) Apply(prev any, td types.TupleDelta) (any, *types.TupleDelta) {
	var prevF float64
	if prev != nil {
		switch x := prev.(type) {
		case int:
			prevF = float64(x)
		case int64:
			prevF = float64(x)
		case float64:
			prevF = x
		default:
			prevF = 0
		}
	}

	// extract value from tuple
	colName := s.ColName
	if colName == "" {
		colName = "v"
	}

	raw, ok := td.Tuple[colName]
	// Ignore NULL values (standard SQL behavior)
	if !ok || raw == nil {
		return prev, nil
	}

	var v float64
	switch x := raw.(type) {
	case int:
		v = float64(x)
	case int64:
		v = float64(x)
	case float64:
		v = x
	default:
		v = 0
	}

	newVal := prevF + v*float64(td.Count)

	// outDelta reports the change in aggregate (new - prev)
	diff := newVal - prevF
	if diff == 0 {
		return newVal, nil
	}

	tup := types.Tuple{"agg_delta": diff}
	out := &types.TupleDelta{Tuple: tup, Count: 1}
	return newVal, out
}

// Convenience: simple CountAgg implementation
type CountAgg struct {
	ColName string // Column to count (empty string means COUNT(*))
}

func (c *CountAgg) Apply(prev any, td types.TupleDelta) (any, *types.TupleDelta) {
	var prevI int64
	if prev != nil {
		switch x := prev.(type) {
		case int:
			prevI = int64(x)
		case int64:
			prevI = x
		case float64:
			prevI = int64(x)
		}
	}

	// If ColName is specified, check if the value is NULL
	// COUNT(col) ignores NULL values
	if c.ColName != "" {
		val, ok := td.Tuple[c.ColName]
		if !ok || val == nil {
			// NULL value, don't count it
			return prev, nil
		}
	}
	// COUNT(*) counts all rows regardless of NULL values

	newI := prevI + td.Count
	diff := newI - prevI
	if diff == 0 {
		return newI, nil
	}
	tup := types.Tuple{"count_delta": diff}
	out := &types.TupleDelta{Tuple: tup, Count: 1}
	return newI, out
}

// AvgAgg maintains a running average using a monoid structure (sum,count) pair.
// This follows DBSP's monoid pattern for aggregates that need composite state.
//
// Monoid properties:
// - Identity: AvgMonoid{sum:0, count:0}
// - Associative: (a ⊕ b) ⊕ c = a ⊕ (b ⊕ c)
// - Invertible: supports both insertion (Count=+1) and deletion (Count=-1)
//
// The aggregate value is computed as sum/count, and the delta reports
// changes in the "avg_delta" column.
type AvgAgg struct {
	ColName string
}

// AvgMonoid is the monoid structure for AVG aggregation.
// It maintains sum and count separately to support incremental updates
// including deletions.
type AvgMonoid struct {
	Sum   float64
	Count float64
}

// Zero returns the identity element of the AVG monoid.
func (a AvgMonoid) Zero() AvgMonoid {
	return AvgMonoid{Sum: 0, Count: 0}
}

// Combine merges two AVG monoids (associative operation).
func (a AvgMonoid) Combine(other AvgMonoid) AvgMonoid {
	return AvgMonoid{
		Sum:   a.Sum + other.Sum,
		Count: a.Count + other.Count,
	}
}

// Value computes the aggregate result (average).
func (a AvgMonoid) Value() float64 {
	if a.Count == 0 {
		return 0
	}
	return a.Sum / a.Count
}

// Apply updates the monoid state with a delta tuple and returns the new state
// and output delta.
func (a *AvgAgg) Apply(prev any, td types.TupleDelta) (any, *types.TupleDelta) {
	// Extract or initialize monoid state
	var monoid AvgMonoid
	if prev != nil {
		var ok bool
		monoid, ok = prev.(AvgMonoid)
		if !ok {
			// Migration path: handle old AvgState format
			if oldState, ok := prev.(AvgState); ok {
				monoid = AvgMonoid{Sum: oldState.sum, Count: oldState.count}
			}
		}
	}

	// Compute old average
	oldAvg := monoid.Value()

	// Extract value from tuple
	col := a.ColName
	if col == "" {
		col = "v"
	}

	raw, ok := td.Tuple[col]
	// Ignore NULL values (standard SQL behavior)
	if !ok || raw == nil {
		return monoid, nil
	}

	var v float64
	switch x := raw.(type) {
	case int:
		v = float64(x)
	case int64:
		v = float64(x)
	case float64:
		v = x
	default:
		v = 0
	}

	// Create delta monoid and combine
	delta := AvgMonoid{
		Sum:   v * float64(td.Count),
		Count: float64(td.Count),
	}
	monoid = monoid.Combine(delta)

	// Compute new average
	newAvg := monoid.Value()

	// Generate output delta
	diff := newAvg - oldAvg
	if diff == 0 {
		return monoid, nil
	}

	outT := types.Tuple{"avg_delta": diff}
	out := &types.TupleDelta{Tuple: outT, Count: 1}
	return monoid, out
}

// AvgState is deprecated; kept for backward compatibility.
// Use AvgMonoid instead.
type AvgState struct {
	sum   float64
	count float64
}

// Small helper for debug printing
func (g *GroupAggOp) String() string {
	return fmt.Sprintf("GroupAggOp(state=%v)", g.state)
}
