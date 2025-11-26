package op

import "github.com/ariyn/dbsp/internal/dbsp/types"

// WindowID identifies a single window by [start, end) in millis.
type WindowID struct {
	Start int64
	End   int64
}

// WindowAggState keeps aggregate state per (window, groupKey).
type WindowAggState struct {
	Data map[WindowID]map[any]any // window → (groupKey → aggValue)
}

// WindowSpecLite is a minimal view of window spec used by WindowAggOp.
// We keep it local to avoid import cycles with ir.
type WindowSpecLite struct {
	TimeCol    string
	SizeMillis int64
}

// WindowAggOp maintains per-window aggregate state and emits deltas only
// for windows affected by each input delta.
type WindowAggOp struct {
	Spec        WindowSpecLite
	KeyFn       func(types.Tuple) any
	GroupKeys   []string // column names for group keys (empty if no grouping)
	AggInit     func() any
	AggFn       AggFunc
	State       WindowAggState
	WatermarkFn func() int64 // optional; nil means no watermark/GC logic
}

func NewWindowAggOp(spec WindowSpecLite, keyFn func(types.Tuple) any, groupKeys []string, aggInit func() any, aggFn AggFunc) *WindowAggOp {
	return &WindowAggOp{
		Spec:      spec,
		KeyFn:     keyFn,
		GroupKeys: groupKeys,
		AggInit:   aggInit,
		AggFn:     aggFn,
		State: WindowAggState{
			Data: make(map[WindowID]map[any]any),
		},
	}
}

// windowIDsForTumble returns the single tumbling window that ts belongs to.
func windowIDsForTumble(spec WindowSpecLite, ts int64) []WindowID {
	if spec.SizeMillis <= 0 {
		return nil
	}
	start := (ts / spec.SizeMillis) * spec.SizeMillis
	return []WindowID{{Start: start, End: start + spec.SizeMillis}}
}

// Apply applies a delta-batch to windowed aggregates and returns the
// corresponding delta output for affected windows only.
func (w *WindowAggOp) Apply(batch types.Batch) (types.Batch, error) {
	var out types.Batch
	if w.State.Data == nil {
		w.State.Data = make(map[WindowID]map[any]any)
	}

	for _, td := range batch {
		// Extract event time
		rawTs, ok := td.Tuple[w.Spec.TimeCol]
		if !ok || rawTs == nil {
			// No time column – ignore for windowing purposes
			continue
		}
		ts, ok := rawTs.(int64)
		if !ok {
			// First version: only int64 millis supported
			continue
		}

		winIDs := windowIDsForTumble(w.Spec, ts)
		if len(winIDs) == 0 {
			continue
		}

		groupKey := w.KeyFn(td.Tuple)

		for _, wid := range winIDs {
			if w.WatermarkFn != nil {
				wm := w.WatermarkFn()
				// Simple policy: windows that end at or before watermark are closed
				if wid.End <= wm {
					continue
				}
			}

			gm, ok := w.State.Data[wid]
			if !ok {
				gm = make(map[any]any)
				w.State.Data[wid] = gm
			}
			prev := gm[groupKey]
			if prev == nil {
				prev = w.AggInit()
			}

			newVal, delta := w.AggFn.Apply(prev, td)
			gm[groupKey] = newVal

			if delta != nil {
				if delta.Tuple == nil {
					delta.Tuple = types.Tuple{}
				}
				delta.Tuple["__window_start"] = wid.Start
				delta.Tuple["__window_end"] = wid.End

				// Restore group key columns from groupKey value
				if len(w.GroupKeys) == 1 {
					// Single key case: groupKey is the value directly
					delta.Tuple[w.GroupKeys[0]] = groupKey
				}
				// TODO: Support composite keys if needed

				out = append(out, *delta)
			}
		}
	}

	return out, nil
}
