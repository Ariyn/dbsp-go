package op

import (
	"github.com/ariyn/dbsp/internal/dbsp/types"
)

// JoinDeltaValueOp computes (ΔR ⋈ S_value) where left is a delta batch and right is a value snapshot.
// The right input is expected to be a materialized Z-set snapshot encoded as Batch (tuple,count).
// Output is a delta batch.
type JoinDeltaValueOp struct {
	LeftKeyFn  func(types.Tuple) any
	RightKeyFn func(types.Tuple) any
	CombineFn  func(l, r types.Tuple) types.Tuple
}

func NewJoinDeltaValueOp(
	leftKeyFn func(types.Tuple) any,
	rightKeyFn func(types.Tuple) any,
	combineFn func(l, r types.Tuple) types.Tuple,
) *JoinDeltaValueOp {
	return &JoinDeltaValueOp{LeftKeyFn: leftKeyFn, RightKeyFn: rightKeyFn, CombineFn: combineFn}
}

func (j *JoinDeltaValueOp) Apply(batch types.Batch) (types.Batch, error) {
	_ = batch
	return nil, nil
}

func (j *JoinDeltaValueOp) Apply2(leftDelta, rightValue types.Batch) (types.Batch, error) {
	return joinDeltaValue(leftDelta, rightValue, j.LeftKeyFn, j.RightKeyFn, j.CombineFn), nil
}

// JoinValueDeltaOp computes (R_value ⋈ ΔS) where left is a value snapshot and right is a delta batch.
// Output is a delta batch.
type JoinValueDeltaOp struct {
	LeftKeyFn  func(types.Tuple) any
	RightKeyFn func(types.Tuple) any
	CombineFn  func(l, r types.Tuple) types.Tuple
}

func NewJoinValueDeltaOp(
	leftKeyFn func(types.Tuple) any,
	rightKeyFn func(types.Tuple) any,
	combineFn func(l, r types.Tuple) types.Tuple,
) *JoinValueDeltaOp {
	return &JoinValueDeltaOp{LeftKeyFn: leftKeyFn, RightKeyFn: rightKeyFn, CombineFn: combineFn}
}

func (j *JoinValueDeltaOp) Apply(batch types.Batch) (types.Batch, error) {
	_ = batch
	return nil, nil
}

func (j *JoinValueDeltaOp) Apply2(leftValue, rightDelta types.Batch) (types.Batch, error) {
	// Symmetric implementation: treat rightDelta as delta and leftValue as value.
	return joinValueDelta(leftValue, rightDelta, j.LeftKeyFn, j.RightKeyFn, j.CombineFn), nil
}

// JoinDeltaDeltaOp computes (ΔR ⋈ ΔS) for two delta batches.
// Output is a delta batch.
type JoinDeltaDeltaOp struct {
	LeftKeyFn  func(types.Tuple) any
	RightKeyFn func(types.Tuple) any
	CombineFn  func(l, r types.Tuple) types.Tuple
}

func NewJoinDeltaDeltaOp(
	leftKeyFn func(types.Tuple) any,
	rightKeyFn func(types.Tuple) any,
	combineFn func(l, r types.Tuple) types.Tuple,
) *JoinDeltaDeltaOp {
	return &JoinDeltaDeltaOp{LeftKeyFn: leftKeyFn, RightKeyFn: rightKeyFn, CombineFn: combineFn}
}

func (j *JoinDeltaDeltaOp) Apply(batch types.Batch) (types.Batch, error) {
	_ = batch
	return nil, nil
}

func (j *JoinDeltaDeltaOp) Apply2(leftDelta, rightDelta types.Batch) (types.Batch, error) {
	return joinDeltaDelta(leftDelta, rightDelta, j.LeftKeyFn, j.RightKeyFn, j.CombineFn), nil
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
