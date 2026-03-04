package op

import (
	"github.com/ariyn/dbsp/internal/dbsp/types"
)

type ExplicitJoinMode int

const (
	JoinDeltaValue ExplicitJoinMode = iota
	JoinValueDelta
	JoinDeltaDelta
)

// ExplicitJoinOp computes joins between two batches.
// Depending on Mode, one or both inputs may be treated as delta batches or value snapshots.
type ExplicitJoinOp struct {
	Mode       ExplicitJoinMode
	LeftKeyFn  func(types.Tuple) any
	RightKeyFn func(types.Tuple) any
	CombineFn  func(l, r types.Tuple) types.Tuple
}

func NewExplicitJoinOp(
	mode ExplicitJoinMode,
	leftKeyFn func(types.Tuple) any,
	rightKeyFn func(types.Tuple) any,
	combineFn func(l, r types.Tuple) types.Tuple,
) *ExplicitJoinOp {
	return &ExplicitJoinOp{
		Mode:       mode,
		LeftKeyFn:  leftKeyFn,
		RightKeyFn: rightKeyFn,
		CombineFn:  combineFn,
	}
}

func (j *ExplicitJoinOp) Apply(batch types.Batch) (types.Batch, error) {
	_ = batch
	return nil, nil // Join expects two inputs via Apply2
}

func (j *ExplicitJoinOp) Apply2(left, right types.Batch) (types.Batch, error) {
	switch j.Mode {
	case JoinDeltaValue:
		return joinDeltaValue(left, right, j.LeftKeyFn, j.RightKeyFn, j.CombineFn), nil
	case JoinValueDelta:
		return joinValueDelta(left, right, j.LeftKeyFn, j.RightKeyFn, j.CombineFn), nil
	case JoinDeltaDelta:
		return joinDeltaDelta(left, right, j.LeftKeyFn, j.RightKeyFn, j.CombineFn), nil
	default:
		return nil, nil
	}
}

func indexByKey(batch types.Batch, keyFn func(types.Tuple) any) map[any][]types.TupleDelta {
	m := make(map[any][]types.TupleDelta)
	for _, td := range batch {
		key := keyFn(td.Tuple)
		if key == nil {
			continue
		}
		m[key] = append(m[key], td)
	}
	return m
}

func joinDeltaValue(
	leftDelta, rightValue types.Batch,
	leftKeyFn func(types.Tuple) any,
	rightKeyFn func(types.Tuple) any,
	combineFn func(l, r types.Tuple) types.Tuple,
) types.Batch {
	var out types.Batch
	// Index value side by join key.
	rightByKey := indexByKey(rightValue, rightKeyFn)
	for _, ld := range leftDelta {
		k := leftKeyFn(ld.Tuple)
		if k == nil {
			continue
		}
		for _, rv := range rightByKey[k] {
			cnt := ld.Count * rv.Count
			if cnt == 0 {
				continue
			}
			out = append(out, types.TupleDelta{Tuple: combineFn(ld.Tuple, rv.Tuple), Count: cnt})
		}
	}
	return out
}

func joinValueDelta(
	leftValue, rightDelta types.Batch,
	leftKeyFn func(types.Tuple) any,
	rightKeyFn func(types.Tuple) any,
	combineFn func(l, r types.Tuple) types.Tuple,
) types.Batch {
	var out types.Batch
	leftByKey := indexByKey(leftValue, leftKeyFn)
	for _, rd := range rightDelta {
		k := rightKeyFn(rd.Tuple)
		if k == nil {
			continue
		}
		for _, lv := range leftByKey[k] {
			cnt := lv.Count * rd.Count
			if cnt == 0 {
				continue
			}
			out = append(out, types.TupleDelta{Tuple: combineFn(lv.Tuple, rd.Tuple), Count: cnt})
		}
	}
	return out
}

func joinDeltaDelta(
	leftDelta, rightDelta types.Batch,
	leftKeyFn func(types.Tuple) any,
	rightKeyFn func(types.Tuple) any,
	combineFn func(l, r types.Tuple) types.Tuple,
) types.Batch {
	var out types.Batch
	rightByKey := indexByKey(rightDelta, rightKeyFn)
	for _, ld := range leftDelta {
		k := leftKeyFn(ld.Tuple)
		if k == nil {
			continue
		}
		for _, rd := range rightByKey[k] {
			cnt := ld.Count * rd.Count
			if cnt == 0 {
				continue
			}
			out = append(out, types.TupleDelta{Tuple: combineFn(ld.Tuple, rd.Tuple), Count: cnt})
		}
	}
	return out
}
