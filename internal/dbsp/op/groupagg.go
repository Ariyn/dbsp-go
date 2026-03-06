package op

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

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

	// EmitValue switches aggregate output from delta to current value.
	// When true, the operator emits (-1 old, +1 new) value tuples per key.
	EmitValue bool

	state      map[any]any
	multiState map[any][]any
	KeyColName string // Optional: name of the key column to include in output (legacy single-key mode)

	// GroupKeyColNames, when set, injects the original GROUP BY key columns from
	// the input tuple into the output delta tuple.
	//
	// This is preferred for multi-key grouping because the internal key may be an
	// encoded composite string.
	GroupKeyColNames []string

	// TimeWindowSpec, when set, applying a fixed tumbling window over the timestamp
	// column before aggregation.
	TimeWindowSpec WindowSpecLite

	// StateTTL evicts per-key aggregate state based on processing-time inactivity.
	StateTTL    time.Duration
	lastTouched map[any]time.Time

	stateBackend StateBackend
	statePrefix  string
}

type groupAggStateRecordV1 struct {
	Kind string `json:"kind"`

	Float64 float64           `json:"f64,omitempty"`
	Int64   int64             `json:"i64,omitempty"`
	Avg     *AvgMonoid        `json:"avg,omitempty"`
	Sorted  *groupAggSortedV1 `json:"sorted,omitempty"`
	Buffer  *groupAggBufferV1 `json:"buffer,omitempty"`
	JSON    []byte            `json:"json,omitempty"`
}

type groupAggSortedV1 struct {
	Values map[string]int64 `json:"values"`
	Sorted []string         `json:"sorted"`
}

type groupAggBufferV1 struct {
	Entries    []BufferEntry `json:"entries"`
	OrderByCol string        `json:"order_by_col"`
}

func (g *GroupAggOp) SetStateBackend(backend StateBackend, prefix string) {
	g.stateBackend = backend
	g.statePrefix = prefix
	if g.statePrefix == "" {
		g.statePrefix = "groupagg/default"
	}
}

func (g *GroupAggOp) SetStateTTL(ttl time.Duration) {
	g.StateTTL = ttl
}

func (g *GroupAggOp) touchKey(now time.Time, key any) {
	if g.StateTTL <= 0 {
		return
	}
	if g.lastTouched == nil {
		g.lastTouched = make(map[any]time.Time)
	}
	g.lastTouched[key] = now
}

func (g *GroupAggOp) evictExpired(now time.Time) error {
	if g.StateTTL <= 0 || len(g.lastTouched) == 0 {
		return nil
	}
	for key, touched := range g.lastTouched {
		if now.Sub(touched) <= g.StateTTL {
			continue
		}
		delete(g.lastTouched, key)
		if g.backendEnabled() {
			if len(g.Aggs) > 0 {
				if err := g.stateBackend.Delete(g.multiStateKey(key)); err != nil {
					return err
				}
			} else {
				if err := g.stateBackend.Delete(g.singleStateKey(key)); err != nil {
					return err
				}
			}
			continue
		}
		if len(g.Aggs) > 0 {
			delete(g.multiState, key)
		} else {
			delete(g.state, key)
		}
	}
	return nil
}

func (g *GroupAggOp) backendEnabled() bool {
	return g != nil && g.stateBackend != nil
}

func (g *GroupAggOp) singleStateKey(key any) []byte {
	return []byte(fmt.Sprintf("%s/single/%s", g.statePrefix, stableAnyKey(key)))
}

func (g *GroupAggOp) multiStateKey(key any) []byte {
	return []byte(fmt.Sprintf("%s/multi/%s", g.statePrefix, stableAnyKey(key)))
}

func encodeGroupAggStateRecord(value any) ([]byte, error) {
	rec := groupAggStateRecordV1{}
	switch v := value.(type) {
	case nil:
		rec.Kind = "nil"
	case float64:
		rec.Kind = "f64"
		rec.Float64 = v
	case int:
		rec.Kind = "i64"
		rec.Int64 = int64(v)
	case int64:
		rec.Kind = "i64"
		rec.Int64 = v
	case AvgMonoid:
		rec.Kind = "avg"
		tmp := v
		rec.Avg = &tmp
	case SortedMultiset:
		rec.Kind = "sorted"
		values := make(map[string]int64, len(v.values))
		for key, count := range v.values {
			values[key] = count
		}
		rec.Sorted = &groupAggSortedV1{Values: values, Sorted: append([]string(nil), v.sorted...)}
	case OrderedBuffer:
		rec.Kind = "buffer"
		rec.Buffer = &groupAggBufferV1{Entries: append([]BufferEntry(nil), v.entries...), OrderByCol: v.orderByCol}
	default:
		rec.Kind = "json"
		b, err := json.Marshal(v)
		if err != nil {
			return nil, fmt.Errorf("unsupported groupagg state type %T", value)
		}
		rec.JSON = b
	}
	return json.Marshal(rec)
}

func decodeGroupAggStateRecord(payload []byte) (any, error) {
	var rec groupAggStateRecordV1
	if err := json.Unmarshal(payload, &rec); err != nil {
		return nil, err
	}
	switch rec.Kind {
	case "nil":
		return nil, nil
	case "f64":
		return rec.Float64, nil
	case "i64":
		return rec.Int64, nil
	case "avg":
		if rec.Avg == nil {
			return AvgMonoid{}, nil
		}
		return *rec.Avg, nil
	case "sorted":
		ms := NewSortedMultiset()
		if rec.Sorted != nil {
			ms.values = make(map[string]int64, len(rec.Sorted.Values))
			for key, count := range rec.Sorted.Values {
				ms.values[key] = count
			}
			ms.sorted = append([]string(nil), rec.Sorted.Sorted...)
		}
		return ms, nil
	case "buffer":
		var ob OrderedBuffer
		if rec.Buffer != nil {
			ob.entries = append([]BufferEntry(nil), rec.Buffer.Entries...)
			ob.orderByCol = rec.Buffer.OrderByCol
		}
		return ob, nil
	case "json":
		var out any
		if len(rec.JSON) == 0 {
			return nil, nil
		}
		if err := json.Unmarshal(rec.JSON, &out); err != nil {
			return nil, err
		}
		return out, nil
	default:
		return nil, fmt.Errorf("unknown groupagg state kind %q", rec.Kind)
	}
}

func (g *GroupAggOp) getSingleState(key any) (any, bool, error) {
	if !g.backendEnabled() {
		prev, ok := g.state[key]
		return prev, ok, nil
	}
	payload, ok, err := g.stateBackend.Get(g.singleStateKey(key))
	if err != nil || !ok {
		return nil, ok, err
	}
	v, err := decodeGroupAggStateRecord(payload)
	if err != nil {
		return nil, false, err
	}
	return v, true, nil
}

func (g *GroupAggOp) putSingleState(key any, value any) error {
	if !g.backendEnabled() {
		g.state[key] = value
		return nil
	}
	payload, err := encodeGroupAggStateRecord(value)
	if err != nil {
		return err
	}
	return g.stateBackend.Put(g.singleStateKey(key), payload)
}

func (g *GroupAggOp) getMultiState(key any) ([]any, bool, error) {
	if !g.backendEnabled() {
		states, ok := g.multiState[key]
		if !ok {
			return nil, false, nil
		}
		cpy := append([]any(nil), states...)
		return cpy, true, nil
	}
	payload, ok, err := g.stateBackend.Get(g.multiStateKey(key))
	if err != nil || !ok {
		return nil, ok, err
	}
	var raws [][]byte
	if err := json.Unmarshal(payload, &raws); err != nil {
		return nil, false, err
	}
	out := make([]any, len(raws))
	for i, raw := range raws {
		v, err := decodeGroupAggStateRecord(raw)
		if err != nil {
			return nil, false, err
		}
		out[i] = v
	}
	return out, true, nil
}

func (g *GroupAggOp) putMultiState(key any, states []any) error {
	if !g.backendEnabled() {
		g.multiState[key] = append([]any(nil), states...)
		return nil
	}
	raws := make([][]byte, len(states))
	for i, state := range states {
		enc, err := encodeGroupAggStateRecord(state)
		if err != nil {
			return err
		}
		raws[i] = enc
	}
	payload, err := json.Marshal(raws)
	if err != nil {
		return err
	}
	return g.stateBackend.Put(g.multiStateKey(key), payload)
}

type groupAggSnapshotV1 struct {
	State            map[any]any
	MultiState       map[any][]any
	KeyColName       string
	GroupKeyColNames []string
}

func (g *GroupAggOp) Snapshot() (any, error) {
	if g == nil {
		return groupAggSnapshotV1{}, nil
	}
	snap := groupAggSnapshotV1{KeyColName: g.KeyColName}
	if len(g.GroupKeyColNames) > 0 {
		snap.GroupKeyColNames = append([]string(nil), g.GroupKeyColNames...)
	}
	if g.backendEnabled() {
		stateCopy := g.State()
		if len(g.Aggs) > 0 {
			snap.MultiState = make(map[any][]any, len(stateCopy))
			for key, val := range stateCopy {
				if vals, ok := val.([]any); ok {
					snap.MultiState[key] = append([]any(nil), vals...)
				}
			}
		} else {
			snap.State = make(map[any]any, len(stateCopy))
			for key, val := range stateCopy {
				snap.State[key] = val
			}
		}
		return snap, nil
	}
	if g.state != nil {
		snap.State = make(map[any]any, len(g.state))
		for k, v := range g.state {
			snap.State[k] = v
		}
	}
	if g.multiState != nil {
		snap.MultiState = make(map[any][]any, len(g.multiState))
		for k, v := range g.multiState {
			if v == nil {
				continue
			}
			cpy := make([]any, len(v))
			copy(cpy, v)
			snap.MultiState[k] = cpy
		}
	}
	return snap, nil
}

func (g *GroupAggOp) Restore(state any) error {
	if g == nil {
		return fmt.Errorf("GroupAggOp is nil")
	}
	s, ok := state.(groupAggSnapshotV1)
	if !ok {
		return fmt.Errorf("unexpected snapshot type %T", state)
	}
	g.KeyColName = s.KeyColName
	if len(s.GroupKeyColNames) > 0 {
		g.GroupKeyColNames = append([]string(nil), s.GroupKeyColNames...)
	} else {
		g.GroupKeyColNames = nil
	}

	if s.State != nil {
		g.state = make(map[any]any, len(s.State))
		for k, v := range s.State {
			g.state[k] = v
		}
	} else {
		g.state = make(map[any]any)
	}
	if s.MultiState != nil {
		g.multiState = make(map[any][]any, len(s.MultiState))
		for k, v := range s.MultiState {
			if v == nil {
				continue
			}
			cpy := make([]any, len(v))
			copy(cpy, v)
			g.multiState[k] = cpy
		}
	} else {
		g.multiState = make(map[any][]any)
	}

	if g.backendEnabled() {
		if len(g.Aggs) > 0 {
			for key, vals := range g.multiState {
				if err := g.putMultiState(key, vals); err != nil {
					return err
				}
			}
		} else {
			for key, val := range g.state {
				if err := g.putSingleState(key, val); err != nil {
					return err
				}
			}
		}
	}
	return nil
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

	now := time.Now()
	if err := g.evictExpired(now); err != nil {
		return nil, err
	}

	var out types.Batch
	if g.state == nil {
		g.state = make(map[any]any)
	}
	if g.EmitValue {
		for _, td := range batch {
			key := g.KeyFn(td.Tuple)
			prev, ok, err := g.getSingleState(key)
			if err != nil {
				return nil, err
			}
			if !ok {
				prev = g.AggInit()
			}
			oldVal := any(nil)
			if ok {
				oldVal = aggValueFromState(g.AggFn, prev)
			}
			newState, _ := g.AggFn.Apply(prev, td)
			if err := g.putSingleState(key, newState); err != nil {
				return nil, err
			}
			g.touchKey(now, key)
			newVal := aggValueFromState(g.AggFn, newState)
			if ok && types.EqualAny(oldVal, newVal) {
				continue
			}
			colName := aggOutputColumnName(g.AggFn)
			if ok && oldVal != nil {
				out = append(out, types.TupleDelta{Tuple: g.buildValueTuple(td, key, map[string]any{colName: oldVal}), Count: -1})
			}
			if newVal != nil {
				out = append(out, types.TupleDelta{Tuple: g.buildValueTuple(td, key, map[string]any{colName: newVal}), Count: 1})
			}
		}
		return out, nil
	}

	// For delta-style aggregates (SUM/COUNT/AVG), compact per group key within
	// the batch so that net-zero changes don't emit output.
	pending := make(map[any]*types.TupleDelta)
	for _, td := range batch {
		key := g.KeyFn(td.Tuple)
		prev, ok, err := g.getSingleState(key)
		if err != nil {
			return nil, err
		}
		if !ok {
			prev = g.AggInit()
		}
		newVal, outDelta := g.AggFn.Apply(prev, td)
		if lagAgg, ok := g.AggFn.(*LagAgg); ok {
			_ = lagAgg
			lm, ok := newVal.(LagMonoid)
			if !ok {
				return nil, fmt.Errorf("unexpected lag monoid type %T", newVal)
			}
			pendingLag := lm.Pending
			lm.Pending = nil
			if err := g.putSingleState(key, lm); err != nil {
				return nil, err
			}
			g.touchKey(now, key)
			for _, ld := range pendingLag {
				if ld.Tuple == nil {
					ld.Tuple = types.Tuple{}
				}
				if len(g.GroupKeyColNames) > 0 {
					for _, col := range g.GroupKeyColNames {
						ld.Tuple[col] = td.Tuple[col]
					}
				} else if g.KeyColName != "" {
					ld.Tuple[g.KeyColName] = key
				}
				out = append(out, ld)
			}
			continue
		}
		if err := g.putSingleState(key, newVal); err != nil {
			return nil, err
		}
		g.touchKey(now, key)
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
						cpy := &types.TupleDelta{Tuple: types.CloneTuple(outDelta.Tuple), Count: 1}
						pending[key] = cpy
					} else {
						existing.Tuple["agg_delta"] = types.ToFloat64(existing.Tuple["agg_delta"]) + types.ToFloat64(outDelta.Tuple["agg_delta"])
					}
					if existing := pending[key]; existing != nil && types.ToFloat64(existing.Tuple["agg_delta"]) == 0 {
						delete(pending, key)
					}
					continue
				}
				if _, ok := outDelta.Tuple["avg_delta"]; ok {
					existing := pending[key]
					if existing == nil {
						cpy := &types.TupleDelta{Tuple: types.CloneTuple(outDelta.Tuple), Count: 1}
						pending[key] = cpy
					} else {
						existing.Tuple["avg_delta"] = types.ToFloat64(existing.Tuple["avg_delta"]) + types.ToFloat64(outDelta.Tuple["avg_delta"])
					}
					if existing := pending[key]; existing != nil && types.ToFloat64(existing.Tuple["avg_delta"]) == 0 {
						delete(pending, key)
					}
					continue
				}
				if _, ok := outDelta.Tuple["count_delta"]; ok {
					existing := pending[key]
					if existing == nil {
						cpy := &types.TupleDelta{Tuple: types.CloneTuple(outDelta.Tuple), Count: 1}
						pending[key] = cpy
					} else {
						existing.Tuple["count_delta"] = types.ToInt64(existing.Tuple["count_delta"]) + types.ToInt64(outDelta.Tuple["count_delta"])
					}
					if existing := pending[key]; existing != nil && types.ToInt64(existing.Tuple["count_delta"]) == 0 {
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

	now := time.Now()
	if err := g.evictExpired(now); err != nil {
		return nil, err
	}
	if g.EmitValue {
		if g.multiState == nil {
			g.multiState = make(map[any][]any)
		}
		for _, td := range batch {
			key := g.KeyFn(td.Tuple)
			states, ok, err := g.getMultiState(key)
			if err != nil {
				return nil, err
			}
			if !ok || len(states) != len(g.Aggs) {
				states = make([]any, len(g.Aggs))
				for i, a := range g.Aggs {
					if a.Init != nil {
						states[i] = a.Init()
					}
				}
			}
			oldVals := make([]any, len(g.Aggs))
			if ok {
				for i, a := range g.Aggs {
					oldVals[i] = aggValueFromState(a.Fn, states[i])
				}
			}
			for i, a := range g.Aggs {
				newVal, _ := a.Fn.Apply(states[i], td)
				states[i] = newVal
			}
			if err := g.putMultiState(key, states); err != nil {
				return nil, err
			}
			g.touchKey(now, key)
			newVals := make([]any, len(g.Aggs))
			changed := !ok
			for i, a := range g.Aggs {
				newVals[i] = aggValueFromState(a.Fn, states[i])
				if ok && !types.EqualAny(oldVals[i], newVals[i]) {
					changed = true
				}
			}
			if !changed {
				continue
			}
			if ok {
				out = append(out, types.TupleDelta{Tuple: g.buildValueTuple(td, key, aggValueMap(g.Aggs, oldVals)), Count: -1})
			}
			out = append(out, types.TupleDelta{Tuple: g.buildValueTuple(td, key, aggValueMap(g.Aggs, newVals)), Count: 1})
		}
		return out, nil
	}
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
		states, ok, err := g.getMultiState(key)
		if err != nil {
			return nil, err
		}
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

		if err := g.putMultiState(key, states); err != nil {
			return nil, err
		}
		g.touchKey(now, key)
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
		pending[key] = &types.TupleDelta{Tuple: types.CloneTuple(delta.Tuple), Count: 1}
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

	// Keep merged deltas; higher-level consumers may rely on explicit zero/non-zero
	// updates for grouped outputs in complex plans.
}

func aggOutputColumnName(agg AggFunc) string {
	switch a := agg.(type) {
	case *SumAgg:
		if strings.TrimSpace(a.DeltaCol) != "" {
			return a.DeltaCol
		}
		return "agg_delta"
	case *AvgAgg:
		if strings.TrimSpace(a.DeltaCol) != "" {
			return a.DeltaCol
		}
		return "avg_delta"
	case *CountAgg:
		if strings.TrimSpace(a.DeltaCol) != "" {
			return a.DeltaCol
		}
		return "count_delta"
	case *MinAgg:
		return "min"
	case *MaxAgg:
		return "max"
	default:
		return "agg_delta"
	}
}

func aggValueFromState(agg AggFunc, state any) any {
	if state == nil {
		return nil
	}
	switch a := agg.(type) {
	case *SumAgg:
		switch v := state.(type) {
		case float64:
			return v
		case int64:
			return float64(v)
		case int:
			return float64(v)
		default:
			return types.ToFloat64(state)
		}
	case *CountAgg:
		switch v := state.(type) {
		case int64:
			return v
		case int:
			return int64(v)
		default:
			return types.ToInt64(state)
		}
	case *AvgAgg:
		switch v := state.(type) {
		case AvgMonoid:
			if v.Count == 0 {
				return nil
			}
			return v.Sum / float64(v.Count)
		case AvgState:
			if v.count == 0 {
				return nil
			}
			return v.sum / v.count
		default:
			return state
		}
	case *MinAgg:
		ms, ok := state.(SortedMultiset)
		if !ok || ms.IsEmpty() {
			return nil
		}
		_ = a
		return ms.Min()
	case *MaxAgg:
		ms, ok := state.(SortedMultiset)
		if !ok || ms.IsEmpty() {
			return nil
		}
		_ = a
		return ms.Max()
	default:
		return state
	}
}

func (g *GroupAggOp) buildValueTuple(td types.TupleDelta, key any, values map[string]any) types.Tuple {
	out := types.Tuple{}
	if len(g.GroupKeyColNames) > 0 {
		for _, col := range g.GroupKeyColNames {
			out[col] = td.Tuple[col]
		}
	} else if g.KeyColName != "" {
		out[g.KeyColName] = key
	}
	for k, v := range values {
		out[k] = v
	}
	return out
}

func aggValueMap(aggs []AggSlot, vals []any) map[string]any {
	out := make(map[string]any, len(aggs))
	for i, a := range aggs {
		col := aggOutputColumnName(a.Fn)
		if i < len(vals) {
			out[col] = vals[i]
		}
	}
	return out
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
	return types.ToFloat64(v)
}

func toInt64Local(v any) int64 {
	return types.ToInt64(v)
}

// State returns a copy of the internal aggregate state (for testing/inspection).
func (g *GroupAggOp) State() map[any]any {
	if g.backendEnabled() {
		copy := make(map[any]any)
		if len(g.Aggs) > 0 {
			prefix := []byte(fmt.Sprintf("%s/multi/", g.statePrefix))
			_ = g.stateBackend.IterPrefix(prefix, func(key, value []byte) error {
				encKey := strings.TrimPrefix(string(key), string(prefix))
				decodedKey, err := decodeAnyKey(encKey)
				if err != nil {
					decodedKey = encKey
				}
				var raws [][]byte
				if err := json.Unmarshal(value, &raws); err != nil {
					return nil
				}
				vals := make([]any, len(raws))
				for i, raw := range raws {
					v, err := decodeGroupAggStateRecord(raw)
					if err != nil {
						return nil
					}
					vals[i] = v
				}
				copy[decodedKey] = vals
				return nil
			})
			return copy
		}

		prefix := []byte(fmt.Sprintf("%s/single/", g.statePrefix))
		_ = g.stateBackend.IterPrefix(prefix, func(key, value []byte) error {
			encKey := strings.TrimPrefix(string(key), string(prefix))
			decodedKey, err := decodeAnyKey(encKey)
			if err != nil {
				decodedKey = encKey
			}
			v, err := decodeGroupAggStateRecord(value)
			if err != nil {
				return nil
			}
			copy[decodedKey] = v
			return nil
		})
		return copy
	}

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
	Expr     func(types.Tuple) (any, error)
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
	var raw any
	if s.Expr != nil {
		var err error
		raw, err = s.Expr(td.Tuple)
		if err != nil {
			return prev, nil
		}
	} else {
		colName := s.ColName
		if colName == "" {
			colName = "v"
		}
		raw = td.Tuple[colName]
	}

	// Ignore NULL values (standard SQL behavior)
	if raw == nil {
		return prev, nil
	}

	var v float64
	switch x := raw.(type) {
	case int:
		v = float64(x)
	case int32:
		v = float64(x)
	case int64:
		v = float64(x)
	case uint:
		v = float64(x)
	case uint32:
		v = float64(x)
	case uint64:
		v = float64(x)
	case float32:
		v = float64(x)
	case float64:
		v = x
	case json.Number:
		f, err := x.Float64()
		if err != nil {
			v = 0
			break
		}
		v = f
	case string:
		f, err := strconv.ParseFloat(strings.TrimSpace(x), 64)
		if err != nil {
			v = 0
			break
		}
		v = f
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
	Expr     func(types.Tuple) (any, error)
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

	// If ColName/Expr is specified, check if the value is NULL
	// COUNT(col) ignores NULL values
	if c.Expr != nil {
		val, err := c.Expr(td.Tuple)
		if err != nil || val == nil {
			return prev, nil
		}
	} else if colName != "" {
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
	ColName  string
	DeltaCol string
	Expr     func(types.Tuple) (any, error)
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
	var raw any
	if a.Expr != nil {
		var err error
		raw, err = a.Expr(td.Tuple)
		if err != nil {
			return monoid, nil
		}
	} else {
		col := a.ColName
		if col == "" {
			col = "v"
		}
		raw = td.Tuple[col]
	}

	// Ignore NULL values (standard SQL behavior)
	if raw == nil {
		return monoid, nil
	}

	var v float64
	switch x := raw.(type) {
	case int:
		v = float64(x)
	case int32:
		v = float64(x)
	case int64:
		v = float64(x)
	case uint:
		v = float64(x)
	case uint32:
		v = float64(x)
	case uint64:
		v = float64(x)
	case float32:
		v = float64(x)
	case float64:
		v = x
	case json.Number:
		f, err := x.Float64()
		if err != nil {
			v = 0
			break
		}
		v = f
	case string:
		f, err := strconv.ParseFloat(strings.TrimSpace(x), 64)
		if err != nil {
			v = 0
			break
		}
		v = f
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

	deltaCol := a.DeltaCol
	if strings.TrimSpace(deltaCol) == "" {
		deltaCol = "avg_delta"
	}
	outT := types.Tuple{deltaCol: diff}
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
