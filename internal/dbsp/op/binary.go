package op

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/ariyn/dbsp/internal/dbsp/types"
)

type joinEntry struct {
	tuple     types.Tuple
	count     int64
	expiresAt time.Time
}

type joinBucket map[string]*joinEntry

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
	Type  BinaryOpType
	Left  Operator
	Right Operator

	// For Join: key extraction and combine functions
	LeftKeyFn  func(types.Tuple) any
	RightKeyFn func(types.Tuple) any
	CombineFn  func(l, r types.Tuple) types.Tuple

	// State for incremental processing
	// Join uses compacted multiset state: key -> (tupleKey -> {tuple,count,expiresAt})
	leftState  map[any]joinBucket
	rightState map[any]joinBucket

	// JoinTTL enables processing-time TTL eviction for join state.
	// When > 0, expired tuples are evicted before processing new deltas,
	// and corresponding join outputs are retracted.
	JoinTTL time.Duration

	// Now is used for TTL; defaults to time.Now when nil.
	Now func() time.Time

	// Optional backend-based join state storage (Unit 3).
	// When nil, join state uses in-memory maps (existing behavior).
	joinStateBackend StateBackend
	joinStatePrefix  string
}

type binarySnapshotV1 struct {
	Type    BinaryOpType
	JoinTTL time.Duration
	Left    map[any]map[string]binaryJoinEntryV1
	Right   map[any]map[string]binaryJoinEntryV1
}

type binaryJoinEntryV1 struct {
	Tuple     types.Tuple
	Count     int64
	ExpiresAt time.Time
}

func (b *BinaryOp) Snapshot() (any, error) {
	if b == nil {
		return binarySnapshotV1{}, nil
	}
	// Note: function fields (key/combine) are part of the graph definition and are
	// not snapshotted.
	bs := binarySnapshotV1{Type: b.Type, JoinTTL: b.JoinTTL}
	bs.Left = make(map[any]map[string]binaryJoinEntryV1)
	bs.Right = make(map[any]map[string]binaryJoinEntryV1)
	for k, bucket := range b.leftState {
		m := make(map[string]binaryJoinEntryV1, len(bucket))
		for tk, e := range bucket {
			if e == nil {
				continue
			}
			m[tk] = binaryJoinEntryV1{Tuple: types.CloneTuple(e.tuple), Count: e.count, ExpiresAt: e.expiresAt}
		}
		if len(m) > 0 {
			bs.Left[k] = m
		}
	}
	for k, bucket := range b.rightState {
		m := make(map[string]binaryJoinEntryV1, len(bucket))
		for tk, e := range bucket {
			if e == nil {
				continue
			}
			m[tk] = binaryJoinEntryV1{Tuple: types.CloneTuple(e.tuple), Count: e.count, ExpiresAt: e.expiresAt}
		}
		if len(m) > 0 {
			bs.Right[k] = m
		}
	}
	return bs, nil
}

func (b *BinaryOp) Restore(state any) error {
	if b == nil {
		return fmt.Errorf("BinaryOp is nil")
	}
	bs, ok := state.(binarySnapshotV1)
	if !ok {
		return fmt.Errorf("unexpected snapshot type %T", state)
	}
	b.Type = bs.Type
	b.JoinTTL = bs.JoinTTL

	if b.leftState == nil {
		b.leftState = make(map[any]joinBucket)
	} else {
		for k := range b.leftState {
			delete(b.leftState, k)
		}
	}
	if b.rightState == nil {
		b.rightState = make(map[any]joinBucket)
	} else {
		for k := range b.rightState {
			delete(b.rightState, k)
		}
	}

	for k, bucket := range bs.Left {
		jb := make(joinBucket, len(bucket))
		for tk, e := range bucket {
			jb[tk] = &joinEntry{tuple: types.CloneTuple(e.Tuple), count: e.Count, expiresAt: e.ExpiresAt}
		}
		if len(jb) > 0 {
			b.leftState[k] = jb
		}
	}
	for k, bucket := range bs.Right {
		jb := make(joinBucket, len(bucket))
		for tk, e := range bucket {
			jb[tk] = &joinEntry{tuple: types.CloneTuple(e.Tuple), count: e.Count, expiresAt: e.ExpiresAt}
		}
		if len(jb) > 0 {
			b.rightState[k] = jb
		}
	}
	return nil
}

// NewUnionOp creates a union operator.
func NewUnionOp() *BinaryOp {
	return &BinaryOp{
		Type:       BinaryUnion,
		leftState:  make(map[any]joinBucket),
		rightState: make(map[any]joinBucket),
	}
}

// NewDifferenceOp creates a difference operator (multiset difference).
func NewDifferenceOp() *BinaryOp {
	return &BinaryOp{
		Type:       BinaryDifference,
		leftState:  make(map[any]joinBucket),
		rightState: make(map[any]joinBucket),
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
		leftState:  make(map[any]joinBucket),
		rightState: make(map[any]joinBucket),
	}
}

// Apply is kept only to satisfy the unary Operator interface, but BinaryOp requires
// two inputs in a true 2-input DAG.
func (b *BinaryOp) Apply(batch types.Batch) (types.Batch, error) {
	_ = batch
	return nil, fmt.Errorf("BinaryOp requires two inputs; use Apply2 or ApplyBinary")
}

// Apply2 evaluates this binary operator with explicit left/right input batches.
func (b *BinaryOp) Apply2(left, right types.Batch) (types.Batch, error) {
	return b.ApplyBinary(left, right)
}

// ApplyBinary processes delta batches from both left and right inputs.
// Implements: d(S ⊙ T) = (dS ⊙ T) + (S ⊙ dT) + (dS ⊙ dT)
func (b *BinaryOp) ApplyBinary(leftDelta, rightDelta types.Batch) (types.Batch, error) {
	if b.leftState == nil {
		b.leftState = make(map[any]joinBucket)
	}
	if b.rightState == nil {
		b.rightState = make(map[any]joinBucket)
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

func (b *BinaryOp) now() time.Time {
	if b.Now != nil {
		return b.Now()
	}
	return time.Now()
}

func stableTupleKey(t types.Tuple) string {
	return stableTupleKeyCanonical(t)
}

func stableJoinKey(key any) string {
	return stableAnyKey(key)
}

func (b *BinaryOp) SetJoinStateBackend(backend StateBackend, prefix string) {
	b.joinStateBackend = backend
	b.joinStatePrefix = prefix
	if b.joinStatePrefix == "" {
		b.joinStatePrefix = "join/default"
	}
}

// SetStateTTL applies a global state TTL to join state when unset.
func (b *BinaryOp) SetStateTTL(ttl time.Duration) {
	if ttl <= 0 {
		return
	}
	if b.Type != BinaryJoin {
		return
	}
	if b.JoinTTL <= 0 || b.JoinTTL > ttl {
		b.JoinTTL = ttl
	}
}

func (b *BinaryOp) joinBackendEnabled() bool {
	return b != nil && b.Type == BinaryJoin && b.joinStateBackend != nil
}

func (b *BinaryOp) applyDeltaToJoinState(state map[any]joinBucket, key any, td types.TupleDelta, now time.Time) error {
	bucket, ok := state[key]
	if !ok {
		bucket = make(joinBucket)
		state[key] = bucket
	}

	tk := stableTupleKey(td.Tuple)
	entry, ok := bucket[tk]
	if !ok {
		if td.Count < 0 {
			return fmt.Errorf("join state underflow for key=%v tuple=%v count=%d", key, td.Tuple, td.Count)
		}
		entry = &joinEntry{tuple: td.Tuple}
		bucket[tk] = entry
	}

	entry.count += td.Count
	if entry.count == 0 {
		delete(bucket, tk)
		if len(bucket) == 0 {
			delete(state, key)
		}
		return nil
	}
	if entry.count < 0 {
		return fmt.Errorf("join state underflow for key=%v tuple=%v resultingCount=%d", key, td.Tuple, entry.count)
	}

	// Keep latest tuple materialization.
	entry.tuple = td.Tuple

	if b.JoinTTL > 0 && td.Count > 0 {
		entry.expiresAt = now.Add(b.JoinTTL)
	}

	return nil
}

func (b *BinaryOp) evictExpiredJoinState(now time.Time) (types.Batch, error) {
	if b.JoinTTL <= 0 {
		return nil, nil
	}

	var out types.Batch

	// Evict left first (retract joins against current right), then evict right.
	for key, leftBucket := range b.leftState {
		for tk, le := range leftBucket {
			if le.expiresAt.IsZero() || now.Before(le.expiresAt) {
				continue
			}
			// Retract joins produced by this left tuple(s).
			if rightBucket, ok := b.rightState[key]; ok {
				for _, re := range rightBucket {
					count := -(le.count * re.count)
					if count != 0 {
						out = append(out, types.TupleDelta{Tuple: b.CombineFn(le.tuple, re.tuple), Count: count})
					}
				}
			}
			delete(leftBucket, tk)
		}
		if len(leftBucket) == 0 {
			delete(b.leftState, key)
		}
	}

	for key, rightBucket := range b.rightState {
		for tk, re := range rightBucket {
			if re.expiresAt.IsZero() || now.Before(re.expiresAt) {
				continue
			}
			// Retract joins produced by this right tuple(s) against remaining left.
			if leftBucket, ok := b.leftState[key]; ok {
				for _, le := range leftBucket {
					count := -(le.count * re.count)
					if count != 0 {
						out = append(out, types.TupleDelta{Tuple: b.CombineFn(le.tuple, re.tuple), Count: count})
					}
				}
			}
			delete(rightBucket, tk)
		}
		if len(rightBucket) == 0 {
			delete(b.rightState, key)
		}
	}

	return out, nil
}

func (b *BinaryOp) joinBackendBucketKey(side, encodedJoinKey string) []byte {
	return []byte(fmt.Sprintf("%s/%s/%s", b.joinStatePrefix, side, encodedJoinKey))
}

func encodeJoinBucket(bucket joinBucket) ([]byte, error) {
	m := make(map[string]binaryJoinEntryV1, len(bucket))
	for tk, entry := range bucket {
		if entry == nil {
			continue
		}
		m[tk] = binaryJoinEntryV1{Tuple: types.CloneTuple(entry.tuple), Count: entry.count, ExpiresAt: entry.expiresAt}
	}
	return json.Marshal(m)
}

func decodeJoinBucket(payload []byte) (joinBucket, error) {
	if len(payload) == 0 {
		return make(joinBucket), nil
	}
	var m map[string]binaryJoinEntryV1
	if err := json.Unmarshal(payload, &m); err != nil {
		return nil, err
	}
	bucket := make(joinBucket, len(m))
	for tk, entry := range m {
		bucket[tk] = &joinEntry{tuple: types.CloneTuple(entry.Tuple), count: entry.Count, expiresAt: entry.ExpiresAt}
	}
	return bucket, nil
}

func (b *BinaryOp) loadJoinBucket(side, encodedJoinKey string) (joinBucket, error) {
	payload, ok, err := b.joinStateBackend.Get(b.joinBackendBucketKey(side, encodedJoinKey))
	if err != nil {
		return nil, err
	}
	if !ok {
		return make(joinBucket), nil
	}
	return decodeJoinBucket(payload)
}

func (b *BinaryOp) saveJoinBucket(side, encodedJoinKey string, bucket joinBucket) error {
	key := b.joinBackendBucketKey(side, encodedJoinKey)
	if len(bucket) == 0 {
		return b.joinStateBackend.Delete(key)
	}
	payload, err := encodeJoinBucket(bucket)
	if err != nil {
		return err
	}
	return b.joinStateBackend.Put(key, payload)
}

func (b *BinaryOp) listJoinBuckets(side string) ([]struct {
	keyEncoded string
	bucket     joinBucket
}, error) {
	prefix := []byte(fmt.Sprintf("%s/%s/", b.joinStatePrefix, side))
	rows := make([]struct {
		keyEncoded string
		bucket     joinBucket
	}, 0)
	err := b.joinStateBackend.IterPrefix(prefix, func(key, value []byte) error {
		s := string(key)
		if len(s) <= len(string(prefix)) {
			return nil
		}
		enc := s[len(prefix):]
		bucket, err := decodeJoinBucket(value)
		if err != nil {
			return fmt.Errorf("decode join bucket (%s): %w", s, err)
		}
		rows = append(rows, struct {
			keyEncoded string
			bucket     joinBucket
		}{keyEncoded: enc, bucket: bucket})
		return nil
	})
	if err != nil {
		return nil, err
	}
	return rows, nil
}

func (b *BinaryOp) applyDeltaToJoinBucket(bucket joinBucket, encodedKey string, td types.TupleDelta, now time.Time) (joinBucket, error) {
	if bucket == nil {
		bucket = make(joinBucket)
	}
	tk := stableTupleKey(td.Tuple)
	entry, ok := bucket[tk]
	if !ok {
		if td.Count < 0 {
			return nil, fmt.Errorf("join state underflow for key=%s tuple=%v count=%d", encodedKey, td.Tuple, td.Count)
		}
		entry = &joinEntry{tuple: td.Tuple}
		bucket[tk] = entry
	}

	entry.count += td.Count
	if entry.count == 0 {
		delete(bucket, tk)
		return bucket, nil
	}
	if entry.count < 0 {
		return nil, fmt.Errorf("join state underflow for key=%s tuple=%v resultingCount=%d", encodedKey, td.Tuple, entry.count)
	}

	entry.tuple = td.Tuple
	if b.JoinTTL > 0 && td.Count > 0 {
		entry.expiresAt = now.Add(b.JoinTTL)
	}
	return bucket, nil
}

func (b *BinaryOp) evictExpiredJoinStateBackend(now time.Time) (types.Batch, error) {
	if b.JoinTTL <= 0 {
		return nil, nil
	}
	var out types.Batch

	leftRows, err := b.listJoinBuckets("left")
	if err != nil {
		return nil, err
	}
	for _, row := range leftRows {
		leftBucket := row.bucket
		rightBucket, err := b.loadJoinBucket("right", row.keyEncoded)
		if err != nil {
			return nil, err
		}
		for tk, le := range leftBucket {
			if le.expiresAt.IsZero() || now.Before(le.expiresAt) {
				continue
			}
			for _, re := range rightBucket {
				count := -(le.count * re.count)
				if count != 0 {
					out = append(out, types.TupleDelta{Tuple: b.CombineFn(le.tuple, re.tuple), Count: count})
				}
			}
			delete(leftBucket, tk)
		}
		if err := b.saveJoinBucket("left", row.keyEncoded, leftBucket); err != nil {
			return nil, err
		}
	}

	rightRows, err := b.listJoinBuckets("right")
	if err != nil {
		return nil, err
	}
	for _, row := range rightRows {
		rightBucket := row.bucket
		leftBucket, err := b.loadJoinBucket("left", row.keyEncoded)
		if err != nil {
			return nil, err
		}
		for tk, re := range rightBucket {
			if re.expiresAt.IsZero() || now.Before(re.expiresAt) {
				continue
			}
			for _, le := range leftBucket {
				count := -(le.count * re.count)
				if count != 0 {
					out = append(out, types.TupleDelta{Tuple: b.CombineFn(le.tuple, re.tuple), Count: count})
				}
			}
			delete(rightBucket, tk)
		}
		if err := b.saveJoinBucket("right", row.keyEncoded, rightBucket); err != nil {
			return nil, err
		}
	}

	return out, nil
}

func (b *BinaryOp) applyJoinWithBackend(leftDelta, rightDelta types.Batch) (types.Batch, error) {
	var out types.Batch
	now := b.now()

	if b.JoinTTL > 0 {
		evicted, err := b.evictExpiredJoinStateBackend(now)
		if err != nil {
			return nil, err
		}
		if len(evicted) > 0 {
			out = append(out, evicted...)
		}
	}

	for _, ld := range leftDelta {
		leftKey := b.LeftKeyFn(ld.Tuple)
		if leftKey == nil {
			continue
		}
		rightBucket, err := b.loadJoinBucket("right", stableJoinKey(leftKey))
		if err != nil {
			return nil, err
		}
		for _, re := range rightBucket {
			count := ld.Count * re.count
			if count != 0 {
				out = append(out, types.TupleDelta{Tuple: b.CombineFn(ld.Tuple, re.tuple), Count: count})
			}
		}
	}

	for _, rd := range rightDelta {
		rightKey := b.RightKeyFn(rd.Tuple)
		if rightKey == nil {
			continue
		}
		leftBucket, err := b.loadJoinBucket("left", stableJoinKey(rightKey))
		if err != nil {
			return nil, err
		}
		for _, le := range leftBucket {
			count := le.count * rd.Count
			if count != 0 {
				out = append(out, types.TupleDelta{Tuple: b.CombineFn(le.tuple, rd.Tuple), Count: count})
			}
		}
	}

	rightByKey := make(map[string][]types.TupleDelta)
	for _, rd := range rightDelta {
		k := b.RightKeyFn(rd.Tuple)
		if k == nil {
			continue
		}
		rightByKey[stableJoinKey(k)] = append(rightByKey[stableJoinKey(k)], rd)
	}
	for _, ld := range leftDelta {
		k := b.LeftKeyFn(ld.Tuple)
		if k == nil {
			continue
		}
		enc := stableJoinKey(k)
		for _, rd := range rightByKey[enc] {
			count := ld.Count * rd.Count
			if count != 0 {
				out = append(out, types.TupleDelta{Tuple: b.CombineFn(ld.Tuple, rd.Tuple), Count: count})
			}
		}
	}

	for _, ld := range leftDelta {
		k := b.LeftKeyFn(ld.Tuple)
		if k == nil {
			continue
		}
		enc := stableJoinKey(k)
		bucket, err := b.loadJoinBucket("left", enc)
		if err != nil {
			return nil, err
		}
		bucket, err = b.applyDeltaToJoinBucket(bucket, enc, ld, now)
		if err != nil {
			return nil, err
		}
		if err := b.saveJoinBucket("left", enc, bucket); err != nil {
			return nil, err
		}
	}
	for _, rd := range rightDelta {
		k := b.RightKeyFn(rd.Tuple)
		if k == nil {
			continue
		}
		enc := stableJoinKey(k)
		bucket, err := b.loadJoinBucket("right", enc)
		if err != nil {
			return nil, err
		}
		bucket, err = b.applyDeltaToJoinBucket(bucket, enc, rd, now)
		if err != nil {
			return nil, err
		}
		if err := b.saveJoinBucket("right", enc, bucket); err != nil {
			return nil, err
		}
	}

	return out, nil
}

// applyJoin implements incremental join: ΔJoin = (ΔR⋈S) + (R⋈ΔS) + (ΔR⋈ΔS)
func (b *BinaryOp) applyJoin(leftDelta, rightDelta types.Batch) (types.Batch, error) {
	if b.joinBackendEnabled() {
		return b.applyJoinWithBackend(leftDelta, rightDelta)
	}

	var out types.Batch

	now := b.now()
	if b.JoinTTL > 0 {
		evicted, err := b.evictExpiredJoinState(now)
		if err != nil {
			return nil, err
		}
		if len(evicted) > 0 {
			out = append(out, evicted...)
		}
	}

	// Part 1: ΔR ⋈ S (new left tuples join with existing right state)
	for _, ld := range leftDelta {
		leftKey := b.LeftKeyFn(ld.Tuple)
		// Skip NULL keys
		if leftKey == nil {
			continue
		}
		if rightBucket, ok := b.rightState[leftKey]; ok {
			for _, re := range rightBucket {
				combined := b.CombineFn(ld.Tuple, re.tuple)
				count := ld.Count * re.count
				if count != 0 {
					out = append(out, types.TupleDelta{Tuple: combined, Count: count})
				}
			}
		}
	}

	// Part 2: R ⋈ ΔS (existing left state join with new right tuples)
	for _, rd := range rightDelta {
		rightKey := b.RightKeyFn(rd.Tuple)
		// Skip NULL keys
		if rightKey == nil {
			continue
		}
		if leftBucket, ok := b.leftState[rightKey]; ok {
			for _, le := range leftBucket {
				combined := b.CombineFn(le.tuple, rd.Tuple)
				count := le.count * rd.Count
				if count != 0 {
					out = append(out, types.TupleDelta{Tuple: combined, Count: count})
				}
			}
		}
	}

	// Part 3: ΔR ⋈ ΔS (new left tuples join with new right tuples)
	rightByKey := make(map[any][]types.TupleDelta)
	for _, rd := range rightDelta {
		rightKey := b.RightKeyFn(rd.Tuple)
		if rightKey == nil {
			continue
		}
		rightByKey[rightKey] = append(rightByKey[rightKey], rd)
	}
	for _, ld := range leftDelta {
		leftKey := b.LeftKeyFn(ld.Tuple)
		if leftKey == nil {
			continue
		}
		if rds, ok := rightByKey[leftKey]; ok {
			for _, rd := range rds {
				combined := b.CombineFn(ld.Tuple, rd.Tuple)
				count := ld.Count * rd.Count
				if count != 0 {
					out = append(out, types.TupleDelta{Tuple: combined, Count: count})
				}
			}
		}
	}

	// Debug: log join output
	// fmt.Printf("[DEBUG] JOIN: leftDelta=%d, rightDelta=%d, output=%d\n", len(leftDelta), len(rightDelta), len(out))
	// for i, o := range out {
	// 	fmt.Printf("[DEBUG] JOIN output[%d]: %+v\n", i, o.Tuple)
	// }

	// Update state: apply deltas into compacted multiset state (skip NULL keys)
	for _, ld := range leftDelta {
		key := b.LeftKeyFn(ld.Tuple)
		if key == nil {
			continue
		}
		if err := b.applyDeltaToJoinState(b.leftState, key, ld, now); err != nil {
			return nil, err
		}
	}
	for _, rd := range rightDelta {
		key := b.RightKeyFn(rd.Tuple)
		if key == nil {
			continue
		}
		if err := b.applyDeltaToJoinState(b.rightState, key, rd, now); err != nil {
			return nil, err
		}
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
