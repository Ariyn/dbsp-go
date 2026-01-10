package op

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/ariyn/dbsp/internal/dbsp/types"
)

// WindowID identifies a single window by [start, end) in millis.
type WindowID struct {
	Start int64
	End   int64
}

// WindowAggState keeps aggregate state per (window, groupKey).
type WindowAggState struct {
	Data map[WindowID]map[any]any // window → (groupKey → aggValue)
}

// WindowType defines the type of time-based window
type WindowType string

const (
	WindowTypeTumbling WindowType = "TUMBLING"
	WindowTypeSliding  WindowType = "SLIDING"
	WindowTypeSession  WindowType = "SESSION"
)

// WindowSpecLite is a minimal view of window spec used by WindowAggOp.
// We keep it local to avoid import cycles with ir.
type WindowSpecLite struct {
	TimeCol    string
	SizeMillis int64
	WindowType WindowType // TUMBLING, SLIDING, or SESSION
	SlideMillis int64      // For sliding windows (hop size)
	GapMillis   int64      // For session windows (inactivity gap)
}

// FrameSpecLite is a minimal view of frame spec to avoid import cycles.
type FrameSpecLite struct {
	Type       string // ROWS, RANGE, or GROUPS
	StartType  string // UNBOUNDED PRECEDING, CURRENT ROW, <value> PRECEDING/FOLLOWING
	StartValue string // numeric value or interval
	EndType    string // UNBOUNDED FOLLOWING, CURRENT ROW, <value> PRECEDING/FOLLOWING
	EndValue   string // numeric value or interval
}

// WindowAggOp maintains per-window aggregate state and emits deltas only
// for windows affected by each input delta.
type WindowAggOp struct {
	Spec        WindowSpecLite
	KeyFn       func(types.Tuple) any
	GroupKeys   []string // column names for group keys (empty if no grouping)
	OrderByCol  string   // ORDER BY column for frame-based windows
	FrameSpec   *FrameSpecLite
	AggInit     func() any
	AggFn       AggFunc
	State       WindowAggState
	// GroupCounts tracks raw row multiplicity per (window, groupKey).
	// It is used to evict empty groups/windows so state doesn't retain
	// entries with aggregate value 0 after deletions.
	GroupCounts map[WindowID]map[any]int64
	WatermarkFn func() int64 // optional; nil means no watermark/GC logic
	// Per-partition ordered buffers for frame-based aggregation
	PartitionBuffers map[any]*PartitionBuffer
}

// PartitionBuffer maintains ordered rows within a partition for frame-based aggregation
type PartitionBuffer struct {
	Rows []RowWithOrder // sorted by ORDER BY column
}

// RowWithOrder represents a row with its order value and multiplicity
type RowWithOrder struct {
	OrderValue any
	Tuple      types.Tuple
	Count      int64
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
		GroupCounts:      make(map[WindowID]map[any]int64),
		PartitionBuffers: make(map[any]*PartitionBuffer),
	}
}

func (w *WindowAggOp) ensureStateMaps() {
	if w.State.Data == nil {
		w.State.Data = make(map[WindowID]map[any]any)
	}
	if w.GroupCounts == nil {
		w.GroupCounts = make(map[WindowID]map[any]int64)
	}
}

func (w *WindowAggOp) injectGroupKeyColumns(outTuple types.Tuple, inTuple types.Tuple) {
	if outTuple == nil {
		return
	}
	if len(w.GroupKeys) == 0 {
		return
	}
	for _, col := range w.GroupKeys {
		outTuple[col] = inTuple[col]
	}
}

func (w *WindowAggOp) applyGroupCount(wid WindowID, groupKey any, deltaCount int64) error {
	cm, ok := w.GroupCounts[wid]
	if !ok {
		cm = make(map[any]int64)
		w.GroupCounts[wid] = cm
	}

	newCount := cm[groupKey] + deltaCount
	if newCount < 0 {
		return fmt.Errorf("window group underflow for window=%v groupKey=%v resultingCount=%d", wid, groupKey, newCount)
	}
	if newCount == 0 {
		delete(cm, groupKey)
		if len(cm) == 0 {
			delete(w.GroupCounts, wid)
		}

		if gm, ok := w.State.Data[wid]; ok {
			delete(gm, groupKey)
			if len(gm) == 0 {
				delete(w.State.Data, wid)
			}
		}
		return nil
	}
	cm[groupKey] = newCount
	return nil
}

// windowIDsForTumble returns the single tumbling window that ts belongs to.
func windowIDsForTumble(spec WindowSpecLite, ts int64) []WindowID {
	if spec.SizeMillis <= 0 {
		return nil
	}
	start := (ts / spec.SizeMillis) * spec.SizeMillis
	return []WindowID{{Start: start, End: start + spec.SizeMillis}}
}

// windowIDsForSliding returns all sliding windows that ts belongs to.
// For example, with size=10s, slide=5s:
// - ts=7 belongs to windows [0,10) and [5,15)
// - ts=12 belongs to windows [5,15) and [10,20)
func windowIDsForSliding(spec WindowSpecLite, ts int64) []WindowID {
	if spec.SizeMillis <= 0 || spec.SlideMillis <= 0 {
		return nil
	}
	
	var windows []WindowID
	
	// Find the earliest window that could contain this timestamp
	// Window starts are at 0, slide, 2*slide, 3*slide, ...
	// We need windows where: windowStart <= ts < windowStart + size
	
	// Calculate the first window start that ends after ts
	firstPossibleStart := ((ts - spec.SizeMillis + 1) / spec.SlideMillis) * spec.SlideMillis
	if firstPossibleStart < 0 {
		firstPossibleStart = 0
	}
	
	// Generate all windows that contain ts
	for start := firstPossibleStart; start <= ts; start += spec.SlideMillis {
		end := start + spec.SizeMillis
		if ts >= start && ts < end {
			windows = append(windows, WindowID{Start: start, End: end})
		}
		// Stop if we've passed the timestamp
		if start > ts {
			break
		}
	}
	
	return windows
}

// windowIDsForSession returns the session window for a given timestamp and partition.
// Session windows are event-driven and require maintaining session state.
// This is a simplified implementation that creates a new session for each event.
// A full implementation would need to merge sessions when events arrive within the gap.
func windowIDsForSession(spec WindowSpecLite, ts int64) []WindowID {
	if spec.GapMillis <= 0 {
		return nil
	}
	// For now, create a window from this timestamp extending by the gap
	// This is simplified; real session windowing requires cross-event state
	return []WindowID{{Start: ts, End: ts + spec.GapMillis}}
}

// Apply applies a delta-batch to windowed aggregates and returns the
// corresponding delta output for affected windows only.
func (w *WindowAggOp) Apply(batch types.Batch) (types.Batch, error) {
	// Choose execution path based on frame specification
	if w.FrameSpec != nil && w.OrderByCol != "" {
		return w.applyFrameBased(batch)
	}
	
	// Choose window type
	switch w.Spec.WindowType {
	case WindowTypeSliding:
		return w.applySliding(batch)
	case WindowTypeSession:
		return w.applySession(batch)
	default: // TUMBLING or empty
		return w.applyTumbling(batch)
	}
}

// applyTumbling handles tumbling windows (original implementation)
func (w *WindowAggOp) applyTumbling(batch types.Batch) (types.Batch, error) {
	var out types.Batch
	w.ensureStateMaps()
	// Compact additive deltas per (window, groupKey).
	type winGroup struct {
		wid WindowID
		key any
	}
	pending := make(map[winGroup]*types.TupleDelta)
	baseCounts := make(map[winGroup]int64)
	countDeltas := make(map[winGroup]int64)

	for _, td := range batch {
		// Extract event time
		rawTs, ok := td.Tuple[w.Spec.TimeCol]
		if !ok || rawTs == nil {
			continue
		}
		ts, ok := rawTs.(int64)
		if !ok {
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

			k := winGroup{wid: wid, key: groupKey}
			if _, ok := baseCounts[k]; !ok {
				var base int64
				if cm := w.GroupCounts[wid]; cm != nil {
					base = cm[groupKey]
				}
				baseCounts[k] = base
			}
			countDeltas[k] += td.Count

			newVal, delta := w.AggFn.Apply(prev, td)
			gm[groupKey] = newVal

			if delta != nil {
				if delta.Tuple == nil {
					delta.Tuple = types.Tuple{}
				}
				delta.Tuple["__window_start"] = wid.Start
				delta.Tuple["__window_end"] = wid.End
				w.injectGroupKeyColumns(delta.Tuple, td.Tuple)

				if delta.Count == 1 {
					k := winGroup{wid: wid, key: groupKey}
					if _, ok := delta.Tuple["agg_delta"]; ok {
						ex := pending[k]
						if ex == nil {
							pending[k] = &types.TupleDelta{Tuple: cloneTupleLocal(delta.Tuple), Count: 1}
						} else {
							ex.Tuple["agg_delta"] = toFloat64Local(ex.Tuple["agg_delta"]) + toFloat64Local(delta.Tuple["agg_delta"])
						}
						if ex := pending[k]; ex != nil && toFloat64Local(ex.Tuple["agg_delta"]) == 0 {
							delete(pending, k)
						}
						continue
					}
					if _, ok := delta.Tuple["avg_delta"]; ok {
						ex := pending[k]
						if ex == nil {
							pending[k] = &types.TupleDelta{Tuple: cloneTupleLocal(delta.Tuple), Count: 1}
						} else {
							ex.Tuple["avg_delta"] = toFloat64Local(ex.Tuple["avg_delta"]) + toFloat64Local(delta.Tuple["avg_delta"])
						}
						if ex := pending[k]; ex != nil && toFloat64Local(ex.Tuple["avg_delta"]) == 0 {
							delete(pending, k)
						}
						continue
					}
					if _, ok := delta.Tuple["count_delta"]; ok {
						ex := pending[k]
						if ex == nil {
							pending[k] = &types.TupleDelta{Tuple: cloneTupleLocal(delta.Tuple), Count: 1}
						} else {
							ex.Tuple["count_delta"] = toInt64Local(ex.Tuple["count_delta"]) + toInt64Local(delta.Tuple["count_delta"])
						}
						if ex := pending[k]; ex != nil && toInt64Local(ex.Tuple["count_delta"]) == 0 {
							delete(pending, k)
						}
						continue
					}
				}

				out = append(out, *delta)
			}
		}
	}

	// Apply raw row count changes and evict empty groups/windows.
	for k, base := range baseCounts {
		delta := countDeltas[k]
		final := base + delta
		if final < 0 {
			return nil, fmt.Errorf("window group underflow for window=%v groupKey=%v resultingCount=%d", k.wid, k.key, final)
		}
		if final == 0 {
			if cm := w.GroupCounts[k.wid]; cm != nil {
				delete(cm, k.key)
				if len(cm) == 0 {
					delete(w.GroupCounts, k.wid)
				}
			}
			if gm := w.State.Data[k.wid]; gm != nil {
				delete(gm, k.key)
				if len(gm) == 0 {
					delete(w.State.Data, k.wid)
				}
			}
			continue
		}
		cm, ok := w.GroupCounts[k.wid]
		if !ok {
			cm = make(map[any]int64)
			w.GroupCounts[k.wid] = cm
		}
		cm[k.key] = final
	}

	for _, td := range pending {
		out = append(out, *td)
	}

	return out, nil
}

// applySliding handles sliding windows
func (w *WindowAggOp) applySliding(batch types.Batch) (types.Batch, error) {
	var out types.Batch
	w.ensureStateMaps()
	// Compact additive deltas per (window, groupKey).
	type winGroup struct {
		wid WindowID
		key any
	}
	pending := make(map[winGroup]*types.TupleDelta)
	baseCounts := make(map[winGroup]int64)
	countDeltas := make(map[winGroup]int64)

	for _, td := range batch {
		// Extract event time
		rawTs, ok := td.Tuple[w.Spec.TimeCol]
		if !ok || rawTs == nil {
			continue
		}
		ts, ok := rawTs.(int64)
		if !ok {
			continue
		}

		winIDs := windowIDsForSliding(w.Spec, ts)
		if len(winIDs) == 0 {
			continue
		}

		groupKey := w.KeyFn(td.Tuple)

		// Process each window this event belongs to
		for _, wid := range winIDs {
			if w.WatermarkFn != nil {
				wm := w.WatermarkFn()
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

			k := winGroup{wid: wid, key: groupKey}
			if _, ok := baseCounts[k]; !ok {
				var base int64
				if cm := w.GroupCounts[wid]; cm != nil {
					base = cm[groupKey]
				}
				baseCounts[k] = base
			}
			countDeltas[k] += td.Count

			newVal, delta := w.AggFn.Apply(prev, td)
			gm[groupKey] = newVal

			if delta != nil {
				if delta.Tuple == nil {
					delta.Tuple = types.Tuple{}
				}
				delta.Tuple["__window_start"] = wid.Start
				delta.Tuple["__window_end"] = wid.End
				w.injectGroupKeyColumns(delta.Tuple, td.Tuple)

				if delta.Count == 1 {
					k := winGroup{wid: wid, key: groupKey}
					if _, ok := delta.Tuple["agg_delta"]; ok {
						ex := pending[k]
						if ex == nil {
							pending[k] = &types.TupleDelta{Tuple: cloneTupleLocal(delta.Tuple), Count: 1}
						} else {
							ex.Tuple["agg_delta"] = toFloat64Local(ex.Tuple["agg_delta"]) + toFloat64Local(delta.Tuple["agg_delta"])
						}
						if ex := pending[k]; ex != nil && toFloat64Local(ex.Tuple["agg_delta"]) == 0 {
							delete(pending, k)
						}
						continue
					}
					if _, ok := delta.Tuple["avg_delta"]; ok {
						ex := pending[k]
						if ex == nil {
							pending[k] = &types.TupleDelta{Tuple: cloneTupleLocal(delta.Tuple), Count: 1}
						} else {
							ex.Tuple["avg_delta"] = toFloat64Local(ex.Tuple["avg_delta"]) + toFloat64Local(delta.Tuple["avg_delta"])
						}
						if ex := pending[k]; ex != nil && toFloat64Local(ex.Tuple["avg_delta"]) == 0 {
							delete(pending, k)
						}
						continue
					}
					if _, ok := delta.Tuple["count_delta"]; ok {
						ex := pending[k]
						if ex == nil {
							pending[k] = &types.TupleDelta{Tuple: cloneTupleLocal(delta.Tuple), Count: 1}
						} else {
							ex.Tuple["count_delta"] = toInt64Local(ex.Tuple["count_delta"]) + toInt64Local(delta.Tuple["count_delta"])
						}
						if ex := pending[k]; ex != nil && toInt64Local(ex.Tuple["count_delta"]) == 0 {
							delete(pending, k)
						}
						continue
					}
				}

				out = append(out, *delta)
			}
		}
	}

	// Apply raw row count changes and evict empty groups/windows.
	for k, base := range baseCounts {
		delta := countDeltas[k]
		final := base + delta
		if final < 0 {
			return nil, fmt.Errorf("window group underflow for window=%v groupKey=%v resultingCount=%d", k.wid, k.key, final)
		}
		if final == 0 {
			if cm := w.GroupCounts[k.wid]; cm != nil {
				delete(cm, k.key)
				if len(cm) == 0 {
					delete(w.GroupCounts, k.wid)
				}
			}
			if gm := w.State.Data[k.wid]; gm != nil {
				delete(gm, k.key)
				if len(gm) == 0 {
					delete(w.State.Data, k.wid)
				}
			}
			continue
		}
		cm, ok := w.GroupCounts[k.wid]
		if !ok {
			cm = make(map[any]int64)
			w.GroupCounts[k.wid] = cm
		}
		cm[k.key] = final
	}

	for _, td := range pending {
		out = append(out, *td)
	}

	return out, nil
}

// SessionState tracks active sessions per partition
type SessionState struct {
	LastEventTime int64
	WindowStart   int64
	AggValue      any
}

// applySession handles session windows
func (w *WindowAggOp) applySession(batch types.Batch) (types.Batch, error) {
	var out types.Batch
	w.ensureStateMaps()

	// Group events by partition key
	partitionEvents := make(map[any][]types.TupleDelta)
	for _, td := range batch {
		groupKey := w.KeyFn(td.Tuple)
		partitionEvents[groupKey] = append(partitionEvents[groupKey], td)
	}

	// Process each partition separately
	for groupKey, events := range partitionEvents {
		// Sort events by timestamp
		sort.Slice(events, func(i, j int) bool {
			ti, _ := events[i].Tuple[w.Spec.TimeCol].(int64)
			tj, _ := events[j].Tuple[w.Spec.TimeCol].(int64)
			return ti < tj
		})

		var currentSession *SessionState
		
		for _, td := range events {
			rawTs, ok := td.Tuple[w.Spec.TimeCol]
			if !ok || rawTs == nil {
				continue
			}
			ts, ok := rawTs.(int64)
			if !ok {
				continue
			}

			// Check if we need to start a new session
			if currentSession == nil || (ts - currentSession.LastEventTime) > w.Spec.GapMillis {
				// Close previous session if exists
				if currentSession != nil {
					wid := WindowID{
						Start: currentSession.WindowStart,
						End:   currentSession.LastEventTime + w.Spec.GapMillis,
					}
					
					// Emit session close delta
					delta := &types.TupleDelta{
						Tuple: types.Tuple{
							"__window_start": wid.Start,
							"__window_end":   wid.End,
						},
						Count: 1,
					}
					if len(events) > 0 {
						w.injectGroupKeyColumns(delta.Tuple, events[0].Tuple)
					}
					out = append(out, *delta)
				}
				
				// Start new session
				currentSession = &SessionState{
					WindowStart:   ts,
					LastEventTime: ts,
					AggValue:      w.AggInit(),
				}
			}

			// Update session
			currentSession.LastEventTime = ts
			newVal, delta := w.AggFn.Apply(currentSession.AggValue, td)
			currentSession.AggValue = newVal

			wid := WindowID{
				Start: currentSession.WindowStart,
				End:   currentSession.LastEventTime + w.Spec.GapMillis,
			}
			// Store state and maintain raw row counts even if AggFn emits no delta.
			gm, ok := w.State.Data[wid]
			if !ok {
				gm = make(map[any]any)
				w.State.Data[wid] = gm
			}
			gm[groupKey] = newVal
			if err := w.applyGroupCount(wid, groupKey, td.Count); err != nil {
				return nil, err
			}

			// Emit delta for current session window
			if delta != nil {
				if delta.Tuple == nil {
					delta.Tuple = types.Tuple{}
				}
				delta.Tuple["__window_start"] = wid.Start
				delta.Tuple["__window_end"] = wid.End
				w.injectGroupKeyColumns(delta.Tuple, td.Tuple)

				out = append(out, *delta)
			}
		}
	}

	return out, nil
}

// applyFrameBased handles frame-based windows (RANGE/ROWS BETWEEN)
func (w *WindowAggOp) applyFrameBased(batch types.Batch) (types.Batch, error) {
	var out types.Batch

	// Group by partition
	partitionDeltas := make(map[any][]types.TupleDelta)
	for _, td := range batch {
		partitionKey := w.KeyFn(td.Tuple)
		partitionDeltas[partitionKey] = append(partitionDeltas[partitionKey], td)
	}

	// Process each partition
	for partitionKey, deltas := range partitionDeltas {
		buffer := w.getOrCreatePartitionBuffer(partitionKey)
		
		// Apply deltas to buffer
		for _, td := range deltas {
			buffer.addRow(td, w.OrderByCol)
		}

		// Compute frame-based aggregates for affected rows
		frameOut, err := w.computeFrameAggregates(buffer, partitionKey)
		if err != nil {
			return nil, err
		}
		out = append(out, frameOut...)
	}

	return out, nil
}

// getOrCreatePartitionBuffer retrieves or creates a partition buffer
func (w *WindowAggOp) getOrCreatePartitionBuffer(key any) *PartitionBuffer {
	if w.PartitionBuffers == nil {
		w.PartitionBuffers = make(map[any]*PartitionBuffer)
	}
	buffer, ok := w.PartitionBuffers[key]
	if !ok {
		buffer = &PartitionBuffer{Rows: []RowWithOrder{}}
		w.PartitionBuffers[key] = buffer
	}
	return buffer
}

// addRow adds or removes a row from the partition buffer
func (pb *PartitionBuffer) addRow(td types.TupleDelta, orderByCol string) {
	orderValue := td.Tuple[orderByCol]
	
	// Find existing row or insert position
	idx := -1
	for i, row := range pb.Rows {
		if compareValues(row.OrderValue, orderValue) == 0 && tuplesEqual(row.Tuple, td.Tuple) {
			idx = i
			break
		}
	}

	if idx >= 0 {
		// Update existing row
		pb.Rows[idx].Count += td.Count
		if pb.Rows[idx].Count == 0 {
			// Remove row
			pb.Rows = append(pb.Rows[:idx], pb.Rows[idx+1:]...)
		}
	} else if td.Count > 0 {
		// Insert new row
		newRow := RowWithOrder{
			OrderValue: orderValue,
			Tuple:      td.Tuple,
			Count:      td.Count,
		}
		pb.Rows = append(pb.Rows, newRow)
		// Sort by order value
		sort.Slice(pb.Rows, func(i, j int) bool {
			return compareValues(pb.Rows[i].OrderValue, pb.Rows[j].OrderValue) < 0
		})
	}
}

// computeFrameAggregates computes aggregates for all rows in the partition
func (w *WindowAggOp) computeFrameAggregates(buffer *PartitionBuffer, partitionKey any) (types.Batch, error) {
	var out types.Batch

	for i, row := range buffer.Rows {
		// Determine frame boundaries for this row
		frameRows, err := w.getFrameRows(buffer, i)
		if err != nil {
			return nil, err
		}

		// Compute aggregate over frame
		aggState := w.AggInit()
		for _, frameRow := range frameRows {
			for c := int64(0); c < frameRow.Count; c++ {
				td := types.TupleDelta{Tuple: frameRow.Tuple, Count: 1}
				aggState, _ = w.AggFn.Apply(aggState, td)
			}
		}

		// Extract result value
		resultTuple := make(types.Tuple)
		for k, v := range row.Tuple {
			resultTuple[k] = v
		}

		// Add aggregate result
		resultTuple = w.extractAggResult(resultTuple, aggState)

		// Add partition key columns
		if len(w.GroupKeys) > 0 {
			for _, col := range w.GroupKeys {
				resultTuple[col] = row.Tuple[col]
			}
		}

		out = append(out, types.TupleDelta{Tuple: resultTuple, Count: row.Count})
	}

	return out, nil
}

// getFrameRows returns rows within the frame for the given row index
func (w *WindowAggOp) getFrameRows(buffer *PartitionBuffer, currentIdx int) ([]RowWithOrder, error) {
	if w.FrameSpec == nil {
		// Default: entire partition
		return buffer.Rows, nil
	}

	currentRow := buffer.Rows[currentIdx]
	var frameRows []RowWithOrder

	switch w.FrameSpec.Type {
	case "ROWS":
		// Row-based frame
		start, end := w.computeRowFrame(buffer, currentIdx)
		if start < 0 {
			start = 0
		}
		if end > len(buffer.Rows) {
			end = len(buffer.Rows)
		}
		frameRows = buffer.Rows[start:end]

	case "RANGE":
		// Range-based frame (value-based)
		startVal, endVal, err := w.computeRangeFrame(currentRow.OrderValue)
		if err != nil {
			return nil, err
		}
		for _, row := range buffer.Rows {
			cmp := compareValues(row.OrderValue, currentRow.OrderValue)
			if cmp >= startVal && cmp <= endVal {
				frameRows = append(frameRows, row)
			}
		}

	default:
		// Default: entire partition
		frameRows = buffer.Rows
	}

	return frameRows, nil
}

// computeRowFrame computes row-based frame boundaries
func (w *WindowAggOp) computeRowFrame(buffer *PartitionBuffer, currentIdx int) (start, end int) {
	start = currentIdx
	end = currentIdx + 1

	// Parse start boundary
	switch w.FrameSpec.StartType {
	case "UNBOUNDED PRECEDING":
		start = 0
	case "CURRENT ROW":
		start = currentIdx
	default:
		if strings.Contains(w.FrameSpec.StartType, "PRECEDING") {
			offset, _ := strconv.Atoi(w.FrameSpec.StartValue)
			start = currentIdx - offset
		} else if strings.Contains(w.FrameSpec.StartType, "FOLLOWING") {
			offset, _ := strconv.Atoi(w.FrameSpec.StartValue)
			start = currentIdx + offset
		}
	}

	// Parse end boundary
	switch w.FrameSpec.EndType {
	case "UNBOUNDED FOLLOWING":
		end = len(buffer.Rows)
	case "CURRENT ROW":
		end = currentIdx + 1
	default:
		if strings.Contains(w.FrameSpec.EndType, "PRECEDING") {
			offset, _ := strconv.Atoi(w.FrameSpec.EndValue)
			end = currentIdx - offset + 1
		} else if strings.Contains(w.FrameSpec.EndType, "FOLLOWING") {
			offset, _ := strconv.Atoi(w.FrameSpec.EndValue)
			end = currentIdx + offset + 1
		}
	}

	return start, end
}

// computeRangeFrame computes range-based frame boundaries (returns relative positions)
func (w *WindowAggOp) computeRangeFrame(currentValue any) (startOffset, endOffset int, err error) {
	startOffset = -1000000 // effectively unbounded
	endOffset = 1000000

	// Parse start boundary
	switch w.FrameSpec.StartType {
	case "UNBOUNDED PRECEDING":
		startOffset = -1000000
	case "CURRENT ROW":
		startOffset = 0
	default:
		if strings.Contains(w.FrameSpec.StartType, "PRECEDING") {
			startOffset = -1 // preceding values
		} else if strings.Contains(w.FrameSpec.StartType, "FOLLOWING") {
			startOffset = 1 // following values
		}
	}

	// Parse end boundary
	switch w.FrameSpec.EndType {
	case "UNBOUNDED FOLLOWING":
		endOffset = 1000000
	case "CURRENT ROW":
		endOffset = 0
	default:
		if strings.Contains(w.FrameSpec.EndType, "PRECEDING") {
			endOffset = -1
		} else if strings.Contains(w.FrameSpec.EndType, "FOLLOWING") {
			endOffset = 1
		}
	}

	return startOffset, endOffset, nil
}

// extractAggResult extracts aggregate result from aggregate state
func (w *WindowAggOp) extractAggResult(tuple types.Tuple, aggState any) types.Tuple {
	switch s := aggState.(type) {
	case float64:
		tuple["agg_result"] = s
	case int64:
		tuple["agg_result"] = s
	case AvgMonoid:
		if s.Count > 0 {
			tuple["agg_result"] = s.Sum / float64(s.Count)
		} else {
			tuple["agg_result"] = nil
		}
	case SortedMultiset:
		if !s.IsEmpty() {
			tuple["min"] = s.Min()
			tuple["max"] = s.Max()
		}
	default:
		tuple["agg_result"] = aggState
	}
	return tuple
}

// tuplesEqual checks if two tuples are equal
func tuplesEqual(a, b types.Tuple) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}
