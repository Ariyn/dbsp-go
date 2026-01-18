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
	Aggs    []AggSlot

	state      map[any]any
	multiState map[any][]any
	KeyColName string // Optional: name of the key column to include in output (legacy single-key mode)

	// GroupKeyColNames, when set, injects the original GROUP BY key columns from
	// the input tuple into the output delta tuple.
	//
	// This is preferred for multi-key grouping because the internal key may be an
	// encoded composite string.
	GroupKeyColNames []string
}

// AggSlot describes a single aggregate inside a multi-aggregate GroupAggOp.
// Init returns the initial aggregate state; Fn applies TupleDelta updates.
type AggSlot struct {
	Init func() any
	Fn   AggFunc
}

func NewGroupAggOp(keyFn func(types.Tuple) any, aggInit func() any, aggFn AggFunc) *GroupAggOp {
	return &GroupAggOp{KeyFn: keyFn, AggInit: aggInit, AggFn: aggFn, state: make(map[any]any)}
}

func NewGroupAggMultiOp(keyFn func(types.Tuple) any, aggs []AggSlot) *GroupAggOp {
	return &GroupAggOp{KeyFn: keyFn, Aggs: append([]AggSlot(nil), aggs...), multiState: make(map[any][]any)}
}

func (g *GroupAggOp) SetKeyColName(name string) {
	g.KeyColName = name
}

func (g *GroupAggOp) SetGroupKeyColNames(names []string) {
	if len(names) == 0 {
		g.GroupKeyColNames = nil
		return
	}
	// Copy to avoid accidental external mutation.
	g.GroupKeyColNames = append([]string(nil), names...)
}

func (g *GroupAggOp) Apply(batch types.Batch) (types.Batch, error) {
	if len(g.Aggs) > 0 {
		return g.applyMulti(batch)
	}

	var out types.Batch
	// For delta-style aggregates (SUM/COUNT/AVG), compact per group key within
	// the batch so that net-zero changes don't emit output.
	pending := make(map[any]*types.TupleDelta)
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
			if outDelta.Tuple == nil {
				outDelta.Tuple = types.Tuple{}
			}

			if len(g.GroupKeyColNames) > 0 {
				for _, col := range g.GroupKeyColNames {
					outDelta.Tuple[col] = td.Tuple[col]
				}
			} else if g.KeyColName != "" {
				// Legacy single-key mode.
				outDelta.Tuple[g.KeyColName] = key
			}

			// If this looks like an additive "delta" aggregate, compact by summing
			// the delta field per group key.
			if outDelta.Count == 1 {
				if _, ok := outDelta.Tuple["agg_delta"]; ok {
					existing := pending[key]
					if existing == nil {
						cpy := &types.TupleDelta{Tuple: cloneTupleLocal(outDelta.Tuple), Count: 1}
						pending[key] = cpy
					} else {
						existing.Tuple["agg_delta"] = toFloat64Local(existing.Tuple["agg_delta"]) + toFloat64Local(outDelta.Tuple["agg_delta"])
					}
					if existing := pending[key]; existing != nil && toFloat64Local(existing.Tuple["agg_delta"]) == 0 {
						delete(pending, key)
					}
					continue
				}
				if _, ok := outDelta.Tuple["avg_delta"]; ok {
					existing := pending[key]
					if existing == nil {
						cpy := &types.TupleDelta{Tuple: cloneTupleLocal(outDelta.Tuple), Count: 1}
						pending[key] = cpy
					} else {
						existing.Tuple["avg_delta"] = toFloat64Local(existing.Tuple["avg_delta"]) + toFloat64Local(outDelta.Tuple["avg_delta"])
					}
					if existing := pending[key]; existing != nil && toFloat64Local(existing.Tuple["avg_delta"]) == 0 {
						delete(pending, key)
					}
					continue
				}
				if _, ok := outDelta.Tuple["count_delta"]; ok {
					existing := pending[key]
					if existing == nil {
						cpy := &types.TupleDelta{Tuple: cloneTupleLocal(outDelta.Tuple), Count: 1}
						pending[key] = cpy
					} else {
						existing.Tuple["count_delta"] = toInt64Local(existing.Tuple["count_delta"]) + toInt64Local(outDelta.Tuple["count_delta"])
					}
					if existing := pending[key]; existing != nil && toInt64Local(existing.Tuple["count_delta"]) == 0 {
						delete(pending, key)
					}
					continue
				}
			}

			out = append(out, *outDelta)
		}
	}

	for _, td := range pending {
		out = append(out, *td)
	}
	return out, nil
}

func (g *GroupAggOp) applyMulti(batch types.Batch) (types.Batch, error) {
	var out types.Batch
	// Compact additive deltas per group key within the batch so that net-zero
	// changes don't emit output.
	pending := make(map[any]*types.TupleDelta)

	if g.multiState == nil {
		g.multiState = make(map[any][]any)
	}

	skipCols := make(map[string]struct{})
	if len(g.GroupKeyColNames) > 0 {
		for _, c := range g.GroupKeyColNames {
			skipCols[c] = struct{}{}
		}
	} else if g.KeyColName != "" {
		skipCols[g.KeyColName] = struct{}{}
	}

	for _, td := range batch {
		key := g.KeyFn(td.Tuple)
		states, ok := g.multiState[key]
		if !ok || len(states) != len(g.Aggs) {
			states = make([]any, len(g.Aggs))
			for i, a := range g.Aggs {
				if a.Init != nil {
					states[i] = a.Init()
				}
			}
		}

		for i, a := range g.Aggs {
			newVal, outDelta := a.Fn.Apply(states[i], td)
			states[i] = newVal
			if outDelta == nil {
				continue
			}
			if outDelta.Tuple == nil {
				outDelta.Tuple = types.Tuple{}
			}
			if len(g.GroupKeyColNames) > 0 {
				for _, col := range g.GroupKeyColNames {
					outDelta.Tuple[col] = td.Tuple[col]
				}
			} else if g.KeyColName != "" {
				outDelta.Tuple[g.KeyColName] = key
			}

			mergePendingTupleDeltaLocal(pending, key, outDelta, skipCols)
		}

		g.multiState[key] = states
	}

	for _, td := range pending {
		out = append(out, *td)
	}
	return out, nil
}

func mergePendingTupleDeltaLocal(pending map[any]*types.TupleDelta, key any, delta *types.TupleDelta, skipCols map[string]struct{}) {
	if delta == nil {
		return
	}
	// Only compact additive deltas (Count==1). For other styles, just emit.
	if delta.Count != 1 {
		pending[key] = delta
		return
	}
	if delta.Tuple == nil {
		return
	}

	ex := pending[key]
	if ex == nil {
		pending[key] = &types.TupleDelta{Tuple: cloneTupleLocal(delta.Tuple), Count: 1}
		ex = pending[key]
	} else {
		for k, v := range delta.Tuple {
			if _, skip := skipCols[k]; skip {
				continue
			}
			if prev, ok := ex.Tuple[k]; ok {
				ex.Tuple[k] = addNumericLocal(prev, v)
			} else {
				ex.Tuple[k] = v
			}
		}
	}

	// If all numeric delta fields cancel to 0, drop the pending output.
	if isAllNumericZeroLocal(ex.Tuple, skipCols) {
		delete(pending, key)
	}
}

func addNumericLocal(a, b any) any {
	if isFloatyLocal(a) || isFloatyLocal(b) {
		return toFloat64Local(a) + toFloat64Local(b)
	}
	return toInt64Local(a) + toInt64Local(b)
}

func isFloatyLocal(v any) bool {
	switch v.(type) {
	case float64, float32:
		return true
	default:
		return false
	}
}

func isAllNumericZeroLocal(t types.Tuple, skipCols map[string]struct{}) bool {
	for k, v := range t {
		if _, skip := skipCols[k]; skip {
			continue
		}
		switch x := v.(type) {
		case float64:
			if x != 0 {
				return false
			}
		case float32:
			if x != 0 {
				return false
			}
		case int64:
			if x != 0 {
				return false
			}
		case int:
			if x != 0 {
				return false
			}
		case uint64:
			if x != 0 {
				return false
			}
		default:
			// Non-numeric fields are assumed to be group key columns.
			continue
		}
	}
	return true
}

func toFloat64Local(v any) float64 {
	switch x := v.(type) {
	case float64:
		return x
	case float32:
		return float64(x)
	case int:
		return float64(x)
	case int64:
		return float64(x)
	case uint64:
		return float64(x)
	default:
		return 0
	}
}

func toInt64Local(v any) int64 {
	switch x := v.(type) {
	case int64:
		return x
	case int:
		return int64(x)
	case uint64:
		return int64(x)
	case float64:
		return int64(x)
	case float32:
		return int64(x)
	default:
		return 0
	}
}

// State returns a copy of the internal aggregate state (for testing/inspection).
func (g *GroupAggOp) State() map[any]any {
	if len(g.Aggs) > 0 {
		copy := make(map[any]any, len(g.multiState))
		for k, v := range g.multiState {
			copy[k] = append([]any(nil), v...)
		}
		return copy
	}
	copy := make(map[any]any, len(g.state))
	for k, v := range g.state {
		copy[k] = v
	}
	return copy
}

// SumAgg is a simple AggFunc that sums a numeric field multiplied by Count.
type SumAgg struct {
	ColName  string // Column to sum (defaults to "v" if empty)
	DeltaCol string
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

	deltaCol := s.DeltaCol
	if deltaCol == "" {
		deltaCol = "agg_delta"
	}
	tup := types.Tuple{deltaCol: diff}
	out := &types.TupleDelta{Tuple: tup, Count: 1}
	return newVal, out
}

// Convenience: simple CountAgg implementation
type CountAgg struct {
	ColName  string // Column to count (empty string means COUNT(*))
	DeltaCol string
}

func (c *CountAgg) Apply(prev any, td types.TupleDelta) (any, *types.TupleDelta) {
	// Defensive normalization: treat "*" the same as COUNT(*).
	// If ColName accidentally becomes "*" (e.g., from SQL parsing), COUNT would
	// incorrectly ignore all rows because "*" is not a real column.
	colName := c.ColName
	if colName == "*" {
		colName = ""
	}

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
	if colName != "" {
		val, ok := td.Tuple[colName]
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
	deltaCol := c.DeltaCol
	if deltaCol == "" {
		deltaCol = "count_delta"
	}
	tup := types.Tuple{deltaCol: diff}
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
