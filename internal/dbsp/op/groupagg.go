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

	state map[any]any
}

func NewGroupAggOp(keyFn func(types.Tuple) any, aggInit func() any, aggFn AggFunc) *GroupAggOp {
	return &GroupAggOp{KeyFn: keyFn, AggInit: aggInit, AggFn: aggFn, state: make(map[any]any)}
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
	var v float64
	raw, ok := td.Tuple[colName]
	if ok {
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
	} else {
		return prev, nil
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
type CountAgg struct{}

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
	newI := prevI + td.Count
	diff := newI - prevI
	if diff == 0 {
		return newI, nil
	}
	tup := types.Tuple{"count_delta": diff}
	out := &types.TupleDelta{Tuple: tup, Count: 1}
	return newI, out
}

// Small helper for debug printing
func (g *GroupAggOp) String() string {
	return fmt.Sprintf("GroupAggOp(state=%v)", g.state)
}
