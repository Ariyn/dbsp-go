package op

import (
	"github.com/ariyn/dbsp/internal/dbsp/types"
)

// BinaryOpType defines the type of binary operation.
type BinaryOpType int

const (
	BinaryUnion BinaryOpType = iota
	BinaryDifference
	BinaryJoin
)

// BinaryOp represents a binary operation on two input streams.
// For DBSP differentiation: d(S ⊙ T) = (dS ⊙ T) + (S ⊙ dT) + (dS ⊙ dT)
type BinaryOp struct {
	Type BinaryOpType
	Left  Operator
	Right Operator
	
	// For Join: key extraction and combine functions
	LeftKeyFn  func(types.Tuple) any
	RightKeyFn func(types.Tuple) any
	CombineFn  func(l, r types.Tuple) types.Tuple
	
	// State for incremental processing
	leftState  map[any][]types.TupleDelta
	rightState map[any][]types.TupleDelta
}

// NewUnionOp creates a union operator.
func NewUnionOp() *BinaryOp {
	return &BinaryOp{
		Type:       BinaryUnion,
		leftState:  make(map[any][]types.TupleDelta),
		rightState: make(map[any][]types.TupleDelta),
	}
}

// NewDifferenceOp creates a difference operator (multiset difference).
func NewDifferenceOp() *BinaryOp {
	return &BinaryOp{
		Type:       BinaryDifference,
		leftState:  make(map[any][]types.TupleDelta),
		rightState: make(map[any][]types.TupleDelta),
	}
}

// NewJoinOp creates a join operator with key extraction and combine functions.
func NewJoinOp(
	leftKeyFn func(types.Tuple) any,
	rightKeyFn func(types.Tuple) any,
	combineFn func(l, r types.Tuple) types.Tuple,
) *BinaryOp {
	return &BinaryOp{
		Type:       BinaryJoin,
		LeftKeyFn:  leftKeyFn,
		RightKeyFn: rightKeyFn,
		CombineFn:  combineFn,
		leftState:  make(map[any][]types.TupleDelta),
		rightState: make(map[any][]types.TupleDelta),
	}
}

// Apply processes batches from both inputs and returns the output batch.
// This is a stateful operator that maintains left and right state.
func (b *BinaryOp) Apply(batch types.Batch) (types.Batch, error) {
	// Note: This simple Apply only works for single-input operators.
	// Binary operators need special handling in the execution engine.
	// For now, we'll implement ApplyBinary which takes two batches.
	return nil, nil
}

// ApplyBinary processes delta batches from both left and right inputs.
// Implements: d(S ⊙ T) = (dS ⊙ T) + (S ⊙ dT) + (dS ⊙ dT)
func (b *BinaryOp) ApplyBinary(leftDelta, rightDelta types.Batch) (types.Batch, error) {
	if b.leftState == nil {
		b.leftState = make(map[any][]types.TupleDelta)
	}
	if b.rightState == nil {
		b.rightState = make(map[any][]types.TupleDelta)
	}
	
	switch b.Type {
	case BinaryJoin:
		return b.applyJoin(leftDelta, rightDelta)
	case BinaryUnion:
		return b.applyUnion(leftDelta, rightDelta)
	case BinaryDifference:
		return b.applyDifference(leftDelta, rightDelta)
	default:
		return nil, nil
	}
}

// applyJoin implements incremental join: ΔJoin = (ΔR⋈S) + (R⋈ΔS) + (ΔR⋈ΔS)
func (b *BinaryOp) applyJoin(leftDelta, rightDelta types.Batch) (types.Batch, error) {
	var out types.Batch
	
	// Part 1: ΔR ⋈ S (new left tuples join with existing right state)
	for _, ld := range leftDelta {
		leftKey := b.LeftKeyFn(ld.Tuple)
		if rightTuples, ok := b.rightState[leftKey]; ok {
			for _, rd := range rightTuples {
				combined := b.CombineFn(ld.Tuple, rd.Tuple)
				// Multiply counts: insertions and deletions propagate
				count := ld.Count * rd.Count
				if count != 0 {
					out = append(out, types.TupleDelta{
						Tuple: combined,
						Count: count,
					})
				}
			}
		}
	}
	
	// Part 2: R ⋈ ΔS (existing left state join with new right tuples)
	for _, rd := range rightDelta {
		rightKey := b.RightKeyFn(rd.Tuple)
		if leftTuples, ok := b.leftState[rightKey]; ok {
			for _, ld := range leftTuples {
				combined := b.CombineFn(ld.Tuple, rd.Tuple)
				count := ld.Count * rd.Count
				if count != 0 {
					out = append(out, types.TupleDelta{
						Tuple: combined,
						Count: count,
					})
				}
			}
		}
	}
	
	// Part 3: ΔR ⋈ ΔS (new left tuples join with new right tuples)
	for _, ld := range leftDelta {
		leftKey := b.LeftKeyFn(ld.Tuple)
		for _, rd := range rightDelta {
			rightKey := b.RightKeyFn(rd.Tuple)
			if leftKey == rightKey {
				combined := b.CombineFn(ld.Tuple, rd.Tuple)
				count := ld.Count * rd.Count
				if count != 0 {
					out = append(out, types.TupleDelta{
						Tuple: combined,
						Count: count,
					})
				}
			}
		}
	}
	
	// Update state: add new deltas to the state
	for _, ld := range leftDelta {
		key := b.LeftKeyFn(ld.Tuple)
		b.leftState[key] = append(b.leftState[key], ld)
	}
	for _, rd := range rightDelta {
		key := b.RightKeyFn(rd.Tuple)
		b.rightState[key] = append(b.rightState[key], rd)
	}
	
	return out, nil
}

// applyUnion implements multiset union (simply combines both delta batches).
func (b *BinaryOp) applyUnion(leftDelta, rightDelta types.Batch) (types.Batch, error) {
	out := make(types.Batch, 0, len(leftDelta)+len(rightDelta))
	out = append(out, leftDelta...)
	out = append(out, rightDelta...)
	return out, nil
}

// applyDifference implements multiset difference (left - right).
func (b *BinaryOp) applyDifference(leftDelta, rightDelta types.Batch) (types.Batch, error) {
	out := make(types.Batch, 0, len(leftDelta)+len(rightDelta))
	out = append(out, leftDelta...)
	
	// Negate right deltas (subtract from left)
	for _, rd := range rightDelta {
		out = append(out, types.TupleDelta{
			Tuple: rd.Tuple,
			Count: -rd.Count,
		})
	}
	
	return out, nil
}

// JoinOp is a specialized join operator that wraps BinaryOp for convenience.
type JoinOp struct {
	*BinaryOp
}

// NewJoinOpSimple creates a join operator with simpler interface.
func NewJoinOpSimple(
	leftKeyFn func(types.Tuple) any,
	rightKeyFn func(types.Tuple) any,
	combineFn func(l, r types.Tuple) types.Tuple,
) *JoinOp {
	return &JoinOp{
		BinaryOp: NewJoinOp(leftKeyFn, rightKeyFn, combineFn),
	}
}
