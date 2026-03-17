package op

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

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
	TimeCol     string
	SizeMillis  int64
	WindowType  WindowType // TUMBLING, SLIDING, or SESSION
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
	Spec            WindowSpecLite
	KeyFn           func(types.Tuple) any
	GroupKeys       []string // column names for group keys (empty if no grouping)
	frameKeyColumns []string
	profile         operatorApplyProfile
	KeepInput       bool
	// EmitValue switches aggregate output from delta to current value.
	// When true, the operator emits (-1 old, +1 new) value tuples per window/group.
	EmitValue  bool
	OrderByCol string // ORDER BY column for frame-based windows
	FrameSpec  *FrameSpecLite
	AggInit    func() any
	AggFn      AggFunc
	State      WindowAggState
	// GroupCounts tracks raw row multiplicity per (window, groupKey).
	// It is used to evict empty groups/windows so state doesn't retain
	// entries with aggregate value 0 after deletions.
	GroupCounts map[WindowID]map[any]int64
	WatermarkFn func() int64 // optional; nil means no watermark/GC logic
	// Per-partition ordered buffers for frame-based aggregation
	PartitionBuffers map[any]*PartitionBuffer

	// SessionBuffers maintains per-partition event buffers for session windows.
	// It stores rows ordered by event time and supports deletions.
	SessionBuffers map[any]*PartitionBuffer
	// sessionOut stores the last computed session output tuples per partition.
	// We diff against this to emit retractions/insertions when sessions merge/split/extend.
	sessionOut map[any]map[string]types.Tuple
	// frameOut stores the last computed frame-based output tuples per partition.
	frameOut map[any]map[string]types.TupleDelta
	// cumulativeFrameCache stores the current tail aggregate state for
	// cumulative ROWS frames so append-only updates can avoid full recompute.
	cumulativeFrameCache map[any]*cumulativeFramePartitionCache
	// bucketedCumulativeState stores per-partition bucket states for append-only
	// cumulative SUM windows ordered by a bucket column.
	bucketedCumulativeState map[any]*bucketedCumulativePartitionState

	// StateTTL evicts window/partition state based on processing-time inactivity.
	StateTTL             time.Duration
	lastTouchedWindow    map[WindowID]time.Time
	lastTouchedPartition map[any]time.Time
	ttlCheckInterval     time.Duration
	nextTTLCheck         time.Time
	windowTTLExpiry      ttlExpiryQueue
	partitionTTLExpiry   ttlExpiryQueue
	bucketTTLExpiry      ttlExpiryQueue

	// WatermarkGC enables automatic eviction of closed windows whose End <= watermark.
	WatermarkGC          bool
	MaxOutOfOrderness    time.Duration
	AllowedLateness      time.Duration
	LateDataPolicy       string
	observedMaxEventTime int64 // tracks max event time seen across Apply calls

	stateBackend        StateBackend
	statePrefix         string
	backendLoaded       bool
	profileAppendHits   int
	profileAppendMisses int
}

type cumulativeFramePartitionCache struct {
	rowCount        int
	beforeTailState any
	tailState       any
	tailOutput      frameOutputCacheEntry
	outputs         map[string]frameOutputCacheEntry
}

type frameOutputCacheEntry struct {
	BaseTuple  types.Tuple
	BasePacked *types.PackedTuple
	AggValues  types.Tuple
	Count      int64
}

type bucketedCumulativePartitionState struct {
	ClosedPrefixSum float64
	CurrentBucketID string
	CurrentOrder    any
	Buckets         map[string]*bucketedCumulativeBucketState
}

type bucketedCumulativeBucketState struct {
	OrderValue any
	AggState   any
	BaseTuple  types.Tuple
	BasePacked *types.PackedTuple
	Output     types.TupleDelta
}

type windowAggSnapshotV1 struct {
	Spec             WindowSpecLite
	State            WindowAggState
	GroupCounts      map[WindowID]map[any]int64
	PartitionBuffers map[any]*PartitionBuffer
	SessionBuffers   map[any]*PartitionBuffer
	SessionOut       map[any]map[string]types.Tuple
	FrameOut         map[any]map[string]types.TupleDelta
	BucketedState    map[any]*bucketedCumulativePartitionState
}

func (w *WindowAggOp) Snapshot() (any, error) {
	if w == nil {
		return windowAggSnapshotV1{}, nil
	}
	if err := w.loadBackendState(); err != nil {
		return nil, err
	}
	w.ensureStateMaps()

	snap := windowAggSnapshotV1{
		Spec:        w.Spec,
		State:       w.State,
		GroupCounts: w.GroupCounts,
	}
	if w.PartitionBuffers != nil {
		snap.PartitionBuffers = w.PartitionBuffers
	}
	if w.SessionBuffers != nil {
		snap.SessionBuffers = w.SessionBuffers
	}
	if w.sessionOut != nil {
		snap.SessionOut = w.sessionOut
	}
	if w.frameOut != nil {
		snap.FrameOut = w.frameOut
	}
	if w.bucketedCumulativeState != nil {
		snap.BucketedState = w.bucketedCumulativeState
	}
	return snap, nil
}

func (w *WindowAggOp) Restore(state any) error {
	if w == nil {
		return fmt.Errorf("WindowAggOp is nil")
	}
	s, ok := state.(windowAggSnapshotV1)
	if !ok {
		return fmt.Errorf("unexpected snapshot type %T", state)
	}
	// Spec is part of the operator definition; restore it anyway for safety.
	w.Spec = s.Spec
	w.State = s.State
	w.GroupCounts = s.GroupCounts
	w.PartitionBuffers = s.PartitionBuffers
	w.SessionBuffers = s.SessionBuffers
	w.sessionOut = s.SessionOut
	w.frameOut = s.FrameOut
	w.bucketedCumulativeState = s.BucketedState
	w.ensureStateMaps()
	w.backendLoaded = true
	if err := w.flushBackendState(); err != nil {
		return err
	}
	return nil
}

// PartitionBuffer maintains ordered rows within a partition for frame-based aggregation
type PartitionBuffer struct {
	Rows     []RowWithOrder // sorted by ORDER BY column
	rowIndex map[uint64][]int
}

// RowWithOrder represents a row with its order value and multiplicity
type RowWithOrder struct {
	OrderValue any
	Tuple      types.Tuple
	Packed     *types.PackedTuple
	RowHash    uint64
	Count      int64
}

func (r RowWithOrder) materializeTuple() types.Tuple {
	if r.Tuple != nil {
		return r.Tuple
	}
	if r.Packed != nil {
		return r.Packed.Materialize()
	}
	return nil
}

func (r RowWithOrder) get(col string) (any, bool) {
	if r.Tuple != nil {
		value, ok := r.Tuple[col]
		return value, ok
	}
	if r.Packed != nil {
		return r.Packed.Get(col)
	}
	return nil, false
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
		GroupCounts:             make(map[WindowID]map[any]int64),
		PartitionBuffers:        make(map[any]*PartitionBuffer),
		SessionBuffers:          make(map[any]*PartitionBuffer),
		sessionOut:              make(map[any]map[string]types.Tuple),
		cumulativeFrameCache:    make(map[any]*cumulativeFramePartitionCache),
		bucketedCumulativeState: make(map[any]*bucketedCumulativePartitionState),
		profile:                 newOperatorApplyProfile("WindowAggOp"),
	}
}

func (w *WindowAggOp) ensureProfiler() {
	if w.profile.label == "" {
		w.profile = newOperatorApplyProfile("WindowAggOp")
	}
}

func (w *WindowAggOp) SetStateBackend(backend StateBackend, prefix string) {
	w.stateBackend = backend
	w.statePrefix = prefix
	if w.statePrefix == "" {
		w.statePrefix = "windowagg/default"
	}
	w.backendLoaded = false
}

func (w *WindowAggOp) SetStateTTL(ttl time.Duration) {
	w.StateTTL = ttl
}

func (w *WindowAggOp) SetWatermarkGC(enabled bool) {
	w.WatermarkGC = enabled
}

func (w *WindowAggOp) SetEventTimeWatermark(cfg EventTimeWatermarkConfig) {
	w.MaxOutOfOrderness = cfg.MaxOutOfOrderness
	w.AllowedLateness = cfg.AllowedLateness
	w.LateDataPolicy = strings.ToLower(strings.TrimSpace(cfg.Policy))
}

func (w *WindowAggOp) observeEventTimeDelta(td types.TupleDelta) {
	timeCol := w.Spec.TimeCol
	if timeCol == "" {
		timeCol = w.OrderByCol
	}
	if timeCol == "" {
		return
	}
	if td.Count < 0 {
		return
	}
	rawTs, ok := td.Get(timeCol)
	if !ok {
		return
	}
	if ts, ok := rawTs.(int64); ok && ts > w.observedMaxEventTime {
		w.observedMaxEventTime = ts
	}
}

func (w *WindowAggOp) currentWatermark() (int64, bool) {
	if w.WatermarkFn != nil {
		wm := w.WatermarkFn()
		return wm, wm > 0
	}
	if w.observedMaxEventTime <= 0 {
		return 0, false
	}
	if w.MaxOutOfOrderness <= 0 {
		return w.observedMaxEventTime, true
	}
	wm := w.observedMaxEventTime - w.MaxOutOfOrderness.Milliseconds()
	return wm, wm > 0
}

func (w *WindowAggOp) windowExpiredByWatermark(wid WindowID) bool {
	wm, ok := w.currentWatermark()
	if !ok {
		return false
	}
	return wid.End+w.AllowedLateness.Milliseconds() <= wm
}

func (w *WindowAggOp) shouldDropByWatermark(wid WindowID) bool {
	if w.LateDataPolicy != "drop" {
		return false
	}
	return w.windowExpiredByWatermark(wid)
}

func (w *WindowAggOp) touchWindow(now time.Time, wid WindowID) {
	if w.StateTTL <= 0 {
		return
	}
	if w.lastTouchedWindow == nil {
		w.lastTouchedWindow = make(map[WindowID]time.Time)
	}
	w.lastTouchedWindow[wid] = now
	w.windowTTLExpiry.touch(encodeWindowIDKey(wid), now.Add(w.StateTTL))
}

func (w *WindowAggOp) touchPartition(now time.Time, key any) {
	if w.StateTTL <= 0 {
		return
	}
	if w.lastTouchedPartition == nil {
		w.lastTouchedPartition = make(map[any]time.Time)
	}
	w.lastTouchedPartition[key] = now
	w.partitionTTLExpiry.touch(stableAnyKey(key), now.Add(w.StateTTL))
}

func encodeBucketTTLKey(partitionKey any, bucketID string) string {
	partitionEnc := stableAnyKey(partitionKey)
	return strconv.Itoa(len(partitionEnc)) + ":" + partitionEnc + bucketID
}

func decodeBucketTTLKey(encoded string) (any, string, error) {
	sep := strings.IndexByte(encoded, ':')
	if sep <= 0 {
		return nil, "", fmt.Errorf("invalid bucket ttl key %q", encoded)
	}
	partitionLen, err := strconv.Atoi(encoded[:sep])
	if err != nil {
		return nil, "", err
	}
	rest := encoded[sep+1:]
	if partitionLen < 0 || partitionLen > len(rest) {
		return nil, "", fmt.Errorf("invalid bucket ttl key %q", encoded)
	}
	partitionKey, err := decodeAnyKey(rest[:partitionLen])
	if err != nil {
		return nil, "", err
	}
	return partitionKey, rest[partitionLen:], nil
}

func (w *WindowAggOp) touchBucket(now time.Time, partitionKey any, bucketID string) {
	if w.StateTTL <= 0 {
		return
	}
	w.bucketTTLExpiry.touch(encodeBucketTTLKey(partitionKey, bucketID), now.Add(w.StateTTL))
}

func (w *WindowAggOp) evictExpired(now time.Time) types.Batch {
	var out types.Batch
	if w.StateTTL <= 0 {
		return nil
	}
	_ = w.windowTTLExpiry.popExpired(now, func(id string) error {
		wid, err := decodeWindowIDKey(id)
		if err != nil {
			return nil
		}
		delete(w.lastTouchedWindow, wid)
		if w.GroupCounts != nil {
			delete(w.GroupCounts, wid)
		}
		if w.State.Data != nil {
			delete(w.State.Data, wid)
		}
		return nil
	})
	out = append(out, w.evictExpiredBucketedCumulativeBuckets(now)...)
	_ = w.partitionTTLExpiry.popExpired(now, func(id string) error {
		key, err := decodeAnyKey(id)
		if err != nil {
			key = id
		}
		delete(w.lastTouchedPartition, key)
		w.clearFramePartitionState(key)
		w.clearSessionPartitionState(key)
		return nil
	})
	return out
}

// gcByWatermark removes all window state for windows whose End <= current watermark.
// This prevents unbounded growth of State.Data and GroupCounts for closed windows.
// For frame-based windows, it trims old rows from PartitionBuffers and evicts
// closed buckets from bucketedCumulativeState.
func (w *WindowAggOp) gcByWatermark() {
	if !w.WatermarkGC {
		return
	}
	wm, ok := w.currentWatermark()
	if !ok {
		return
	}
	// Tumbling/sliding window state
	for wid := range w.State.Data {
		if w.windowExpiredByWatermark(wid) {
			delete(w.State.Data, wid)
			delete(w.GroupCounts, wid)
			delete(w.lastTouchedWindow, wid)
			w.windowTTLExpiry.remove(encodeWindowIDKey(wid))
		}
	}
	for wid := range w.GroupCounts {
		if w.windowExpiredByWatermark(wid) {
			delete(w.GroupCounts, wid)
		}
	}
	// Frame-based window: trim PartitionBuffer rows older than watermark.
	// Keep rows where OrderValue >= watermark so that frame computation
	// (e.g. LAG) still has the preceding row available for new events.
	for partitionKey, buf := range w.PartitionBuffers {
		if buf == nil || len(buf.Rows) == 0 {
			continue
		}
		// Find first row with OrderValue >= watermark
		cutoff := 0
		for i, row := range buf.Rows {
			if ts, ok := toInt64OrderValue(row.OrderValue); ok && ts+w.AllowedLateness.Milliseconds() < wm {
				cutoff = i + 1
			} else {
				break
			}
		}
		// Always keep at least the last trimmed row as the "preceding" anchor
		if cutoff > 1 {
			cutoff--
		}
		if cutoff > 0 {
			buf.Rows = buf.Rows[cutoff:]
			// Rebuild rowIndex
			buf.rowIndex = make(map[uint64][]int, len(buf.Rows))
			for i, row := range buf.Rows {
				buf.rowIndex[row.RowHash] = append(buf.rowIndex[row.RowHash], i)
			}
		}
		if len(buf.Rows) == 0 {
			delete(w.PartitionBuffers, partitionKey)
			delete(w.lastTouchedPartition, partitionKey)
		}
	}
	// Bucketed cumulative state: evict closed buckets whose OrderValue <= watermark.
	// ClosedPrefixSum already includes these buckets' contributions, so deletion
	// does not affect correctness of future cumulative values.
	for _, partition := range w.bucketedCumulativeState {
		if partition == nil {
			continue
		}
		for bucketID, bucket := range partition.Buckets {
			if bucketID == partition.CurrentBucketID {
				continue // never evict the current open bucket
			}
			if ts, ok := toInt64OrderValue(bucket.OrderValue); ok && ts+w.AllowedLateness.Milliseconds() <= wm {
				delete(partition.Buckets, bucketID)
			}
		}
	}
}

// toInt64OrderValue extracts an int64 from an order value if possible.
func toInt64OrderValue(v any) (int64, bool) {
	switch t := v.(type) {
	case int64:
		return t, true
	case int:
		return int64(t), true
	case int32:
		return int64(t), true
	}
	return 0, false
}

func (w *WindowAggOp) clearFramePartitionState(key any) {
	if w.PartitionBuffers != nil {
		delete(w.PartitionBuffers, key)
	}
	if w.frameOut != nil {
		delete(w.frameOut, key)
	}
	if w.cumulativeFrameCache != nil {
		delete(w.cumulativeFrameCache, key)
	}
	w.clearBucketedCumulativePartitionState(key)
}

func (w *WindowAggOp) clearBucketedCumulativePartitionState(key any) {
	if w.bucketedCumulativeState == nil {
		return
	}
	partition := w.bucketedCumulativeState[key]
	if partition != nil {
		for bucketID := range partition.Buckets {
			w.bucketTTLExpiry.remove(encodeBucketTTLKey(key, bucketID))
		}
	}
	delete(w.bucketedCumulativeState, key)
}

func (w *WindowAggOp) evictExpiredBucketedCumulativeBuckets(now time.Time) types.Batch {
	if w.StateTTL <= 0 || len(w.bucketedCumulativeState) == 0 {
		return nil
	}

	expired := make(map[any]map[string]struct{})
	_ = w.bucketTTLExpiry.popExpired(now, func(id string) error {
		partitionKey, bucketID, err := decodeBucketTTLKey(id)
		if err != nil {
			return nil
		}
		bucketSet := expired[partitionKey]
		if bucketSet == nil {
			bucketSet = make(map[string]struct{})
			expired[partitionKey] = bucketSet
		}
		bucketSet[bucketID] = struct{}{}
		return nil
	})

	var out types.Batch
	for partitionKey, bucketSet := range expired {
		partition := w.bucketedCumulativeState[partitionKey]
		if partition == nil {
			continue
		}
		changed := false
		for bucketID := range bucketSet {
			bucket := partition.Buckets[bucketID]
			if bucket == nil {
				continue
			}
			if bucket.Output.Tuple != nil || bucket.Output.Packed != nil {
				out = append(out, negateTupleDelta(bucket.Output))
			}
			delete(partition.Buckets, bucketID)
			changed = true
		}
		if !changed {
			continue
		}
		if len(partition.Buckets) == 0 {
			partition.ClosedPrefixSum = 0
			partition.CurrentBucketID = ""
			partition.CurrentOrder = nil
			delete(w.bucketedCumulativeState, partitionKey)
			continue
		}
		out = append(out, w.recomputeBucketedCumulativePartitionOutputs(partition)...)
	}
	return out
}

func (w *WindowAggOp) recomputeBucketedCumulativePartitionOutputs(partition *bucketedCumulativePartitionState) types.Batch {
	if partition == nil || len(partition.Buckets) == 0 {
		return nil
	}

	bucketIDs := make([]string, 0, len(partition.Buckets))
	for bucketID := range partition.Buckets {
		bucketIDs = append(bucketIDs, bucketID)
	}
	sort.Slice(bucketIDs, func(i, j int) bool {
		left := partition.Buckets[bucketIDs[i]]
		right := partition.Buckets[bucketIDs[j]]
		return compareValues(left.OrderValue, right.OrderValue) < 0
	})

	var (
		out          types.Batch
		closedPrefix float64
		currentID    string
		currentOrder any
	)
	for idx, bucketID := range bucketIDs {
		bucket := partition.Buckets[bucketID]
		currentValue := types.ToFloat64(aggValueFromState(w.AggFn, bucket.AggState))
		oldOutput := bucket.Output
		hadOldOutput := oldOutput.Tuple != nil || oldOutput.Packed != nil
		newOutput := w.buildBucketedCumulativeOutput(bucket, closedPrefix+currentValue)
		bucket.Output = newOutput
		if hadOldOutput {
			if !tupleDeltaPayloadEqual(oldOutput, newOutput) || oldOutput.Count != newOutput.Count {
				out = append(out, negateTupleDelta(oldOutput))
				out = append(out, newOutput)
			}
		} else {
			out = append(out, newOutput)
		}
		if idx == len(bucketIDs)-1 {
			currentID = bucketID
			currentOrder = bucket.OrderValue
		} else {
			closedPrefix += currentValue
		}
	}
	partition.ClosedPrefixSum = closedPrefix
	partition.CurrentBucketID = currentID
	partition.CurrentOrder = currentOrder
	return out
}

func (w *WindowAggOp) clearSessionPartitionState(key any) {
	if w.SessionBuffers != nil {
		delete(w.SessionBuffers, key)
	}
	if w.sessionOut != nil {
		delete(w.sessionOut, key)
	}
}

func (w *WindowAggOp) backendEnabled() bool {
	return w != nil && w.stateBackend != nil
}

func (w *WindowAggOp) backendSnapshotKey() []byte {
	return []byte(fmt.Sprintf("%s/snapshot", w.statePrefix))
}

func (w *WindowAggOp) backendV2BasePrefix() string {
	return fmt.Sprintf("%s/v2", w.statePrefix)
}

func (w *WindowAggOp) backendV2Prefix() []byte {
	return []byte(w.backendV2BasePrefix() + "/")
}

func (w *WindowAggOp) backendV2SpecKey() []byte {
	return []byte(fmt.Sprintf("%s/spec", w.backendV2BasePrefix()))
}

func encodeWindowIDKey(wid WindowID) string {
	return fmt.Sprintf("%d:%d", wid.Start, wid.End)
}

func decodeWindowIDKey(encoded string) (WindowID, error) {
	parts := strings.SplitN(encoded, ":", 2)
	if len(parts) != 2 {
		return WindowID{}, fmt.Errorf("invalid window id key %q", encoded)
	}
	start, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return WindowID{}, err
	}
	end, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return WindowID{}, err
	}
	return WindowID{Start: start, End: end}, nil
}

func encodeGobValue(v any) ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(v); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func decodeGobValue(payload []byte, out any) error {
	return gob.NewDecoder(bytes.NewReader(payload)).Decode(out)
}

func parseWindowAggV2TwoPartKey(fullKey, sectionPrefix string) (string, string, error) {
	rel := strings.TrimPrefix(fullKey, sectionPrefix)
	parts := strings.SplitN(rel, "/", 2)
	if len(parts) != 2 {
		return "", "", fmt.Errorf("invalid key format %q", fullKey)
	}
	return parts[0], parts[1], nil
}

func parseWindowAggV2OnePartKey(fullKey, sectionPrefix string) string {
	return strings.TrimPrefix(fullKey, sectionPrefix)
}

func encodeWindowAggSnapshot(s windowAggSnapshotV1) ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(s); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func decodeWindowAggSnapshot(payload []byte) (windowAggSnapshotV1, error) {
	var s windowAggSnapshotV1
	dec := gob.NewDecoder(bytes.NewReader(payload))
	if err := dec.Decode(&s); err != nil {
		return windowAggSnapshotV1{}, err
	}
	return s, nil
}

func (w *WindowAggOp) loadBackendState() error {
	if !w.backendEnabled() || w.backendLoaded {
		return nil
	}

	v2Prefix := w.backendV2Prefix()
	v2Seen := false
	if err := w.stateBackend.IterPrefix(v2Prefix, func(_, _ []byte) error {
		v2Seen = true
		return nil
	}); err != nil {
		return err
	}

	if v2Seen {
		w.State = WindowAggState{Data: make(map[WindowID]map[any]any)}
		w.GroupCounts = make(map[WindowID]map[any]int64)
		w.PartitionBuffers = make(map[any]*PartitionBuffer)
		w.SessionBuffers = make(map[any]*PartitionBuffer)
		w.sessionOut = make(map[any]map[string]types.Tuple)
		w.frameOut = make(map[any]map[string]types.TupleDelta)
		w.bucketedCumulativeState = make(map[any]*bucketedCumulativePartitionState)

		if payload, ok, err := w.stateBackend.Get(w.backendV2SpecKey()); err != nil {
			return err
		} else if ok {
			if err := decodeGobValue(payload, &w.Spec); err != nil {
				return err
			}
		}

		statePrefix := fmt.Sprintf("%s/state/", w.backendV2BasePrefix())
		if err := w.stateBackend.IterPrefix([]byte(statePrefix), func(key, value []byte) error {
			windowEnc, groupEnc, err := parseWindowAggV2TwoPartKey(string(key), statePrefix)
			if err != nil {
				return nil
			}
			wid, err := decodeWindowIDKey(windowEnc)
			if err != nil {
				return nil
			}
			groupKey, err := decodeAnyKey(groupEnc)
			if err != nil {
				groupKey = groupEnc
			}
			aggState, err := decodeGroupAggStateRecord(value)
			if err != nil {
				return nil
			}
			gm := w.State.Data[wid]
			if gm == nil {
				gm = make(map[any]any)
				w.State.Data[wid] = gm
			}
			gm[groupKey] = aggState
			return nil
		}); err != nil {
			return err
		}

		countPrefix := fmt.Sprintf("%s/counts/", w.backendV2BasePrefix())
		if err := w.stateBackend.IterPrefix([]byte(countPrefix), func(key, value []byte) error {
			windowEnc, groupEnc, err := parseWindowAggV2TwoPartKey(string(key), countPrefix)
			if err != nil {
				return nil
			}
			wid, err := decodeWindowIDKey(windowEnc)
			if err != nil {
				return nil
			}
			groupKey, err := decodeAnyKey(groupEnc)
			if err != nil {
				groupKey = groupEnc
			}
			count, err := strconv.ParseInt(string(value), 10, 64)
			if err != nil {
				return nil
			}
			cm := w.GroupCounts[wid]
			if cm == nil {
				cm = make(map[any]int64)
				w.GroupCounts[wid] = cm
			}
			cm[groupKey] = count
			return nil
		}); err != nil {
			return err
		}

		pbPrefix := fmt.Sprintf("%s/pbuf/", w.backendV2BasePrefix())
		if err := w.stateBackend.IterPrefix([]byte(pbPrefix), func(key, value []byte) error {
			partitionEnc := parseWindowAggV2OnePartKey(string(key), pbPrefix)
			partitionKey, err := decodeAnyKey(partitionEnc)
			if err != nil {
				partitionKey = partitionEnc
			}
			var pb PartitionBuffer
			if err := decodeGobValue(value, &pb); err != nil {
				return nil
			}
			w.PartitionBuffers[partitionKey] = &pb
			return nil
		}); err != nil {
			return err
		}

		sbPrefix := fmt.Sprintf("%s/sbuf/", w.backendV2BasePrefix())
		if err := w.stateBackend.IterPrefix([]byte(sbPrefix), func(key, value []byte) error {
			partitionEnc := parseWindowAggV2OnePartKey(string(key), sbPrefix)
			partitionKey, err := decodeAnyKey(partitionEnc)
			if err != nil {
				partitionKey = partitionEnc
			}
			var sb PartitionBuffer
			if err := decodeGobValue(value, &sb); err != nil {
				return nil
			}
			w.SessionBuffers[partitionKey] = &sb
			return nil
		}); err != nil {
			return err
		}

		soutPrefix := fmt.Sprintf("%s/sout/", w.backendV2BasePrefix())
		if err := w.stateBackend.IterPrefix([]byte(soutPrefix), func(key, value []byte) error {
			partitionEnc := parseWindowAggV2OnePartKey(string(key), soutPrefix)
			partitionKey, err := decodeAnyKey(partitionEnc)
			if err != nil {
				partitionKey = partitionEnc
			}
			var out map[string]types.Tuple
			if err := decodeGobValue(value, &out); err != nil {
				return nil
			}
			w.sessionOut[partitionKey] = out
			return nil
		}); err != nil {
			return err
		}

		foutPrefix := fmt.Sprintf("%s/fout/", w.backendV2BasePrefix())
		if err := w.stateBackend.IterPrefix([]byte(foutPrefix), func(key, value []byte) error {
			partitionEnc := parseWindowAggV2OnePartKey(string(key), foutPrefix)
			partitionKey, err := decodeAnyKey(partitionEnc)
			if err != nil {
				partitionKey = partitionEnc
			}
			var out map[string]types.TupleDelta
			if err := decodeGobValue(value, &out); err != nil {
				return nil
			}
			w.frameOut[partitionKey] = out
			return nil
		}); err != nil {
			return err
		}

		bcumPrefix := fmt.Sprintf("%s/bcum/", w.backendV2BasePrefix())
		if err := w.stateBackend.IterPrefix([]byte(bcumPrefix), func(key, value []byte) error {
			partitionEnc := parseWindowAggV2OnePartKey(string(key), bcumPrefix)
			partitionKey, err := decodeAnyKey(partitionEnc)
			if err != nil {
				partitionKey = partitionEnc
			}
			var state bucketedCumulativePartitionState
			if err := decodeGobValue(value, &state); err != nil {
				return nil
			}
			w.bucketedCumulativeState[partitionKey] = &state
			return nil
		}); err != nil {
			return err
		}

		w.ensureStateMaps()
		w.backendLoaded = true
		return nil
	}

	payload, ok, err := w.stateBackend.Get(w.backendSnapshotKey())
	if err != nil {
		return err
	}
	if !ok {
		w.ensureStateMaps()
		w.backendLoaded = true
		return nil
	}
	s, err := decodeWindowAggSnapshot(payload)
	if err != nil {
		return err
	}
	w.Spec = s.Spec
	w.State = s.State
	w.GroupCounts = s.GroupCounts
	w.PartitionBuffers = s.PartitionBuffers
	w.SessionBuffers = s.SessionBuffers
	w.sessionOut = s.SessionOut
	w.frameOut = s.FrameOut
	w.bucketedCumulativeState = s.BucketedState
	w.ensureStateMaps()
	w.backendLoaded = true
	return nil
}

func (w *WindowAggOp) flushBackendState() error {
	if !w.backendEnabled() {
		return nil
	}
	w.ensureStateMaps()

	ops := make([]StateBatchOp, 0, 64)
	if err := w.stateBackend.IterPrefix(w.backendV2Prefix(), func(key, _ []byte) error {
		ops = append(ops, StateBatchOp{Type: StateBatchDelete, Key: key})
		return nil
	}); err != nil {
		return err
	}
	ops = append(ops, StateBatchOp{Type: StateBatchDelete, Key: w.backendSnapshotKey()})

	specPayload, err := encodeGobValue(w.Spec)
	if err != nil {
		return err
	}
	ops = append(ops, StateBatchOp{Type: StateBatchPut, Key: w.backendV2SpecKey(), Value: specPayload})

	for wid, gm := range w.State.Data {
		windowEnc := encodeWindowIDKey(wid)
		for groupKey, aggState := range gm {
			aggPayload, err := encodeGroupAggStateRecord(aggState)
			if err != nil {
				return err
			}
			key := []byte(fmt.Sprintf("%s/state/%s/%s", w.backendV2BasePrefix(), windowEnc, stableAnyKey(groupKey)))
			ops = append(ops, StateBatchOp{Type: StateBatchPut, Key: key, Value: aggPayload})
		}
	}

	for wid, cm := range w.GroupCounts {
		windowEnc := encodeWindowIDKey(wid)
		for groupKey, count := range cm {
			key := []byte(fmt.Sprintf("%s/counts/%s/%s", w.backendV2BasePrefix(), windowEnc, stableAnyKey(groupKey)))
			ops = append(ops, StateBatchOp{Type: StateBatchPut, Key: key, Value: []byte(strconv.FormatInt(count, 10))})
		}
	}

	for partitionKey, pb := range w.PartitionBuffers {
		if pb == nil {
			continue
		}
		payload, err := encodeGobValue(*pb)
		if err != nil {
			return err
		}
		key := []byte(fmt.Sprintf("%s/pbuf/%s", w.backendV2BasePrefix(), stableAnyKey(partitionKey)))
		ops = append(ops, StateBatchOp{Type: StateBatchPut, Key: key, Value: payload})
	}

	for partitionKey, sb := range w.SessionBuffers {
		if sb == nil {
			continue
		}
		payload, err := encodeGobValue(*sb)
		if err != nil {
			return err
		}
		key := []byte(fmt.Sprintf("%s/sbuf/%s", w.backendV2BasePrefix(), stableAnyKey(partitionKey)))
		ops = append(ops, StateBatchOp{Type: StateBatchPut, Key: key, Value: payload})
	}

	for partitionKey, out := range w.sessionOut {
		payload, err := encodeGobValue(out)
		if err != nil {
			return err
		}
		key := []byte(fmt.Sprintf("%s/sout/%s", w.backendV2BasePrefix(), stableAnyKey(partitionKey)))
		ops = append(ops, StateBatchOp{Type: StateBatchPut, Key: key, Value: payload})
	}

	for partitionKey, out := range w.frameOut {
		payload, err := encodeGobValue(out)
		if err != nil {
			return err
		}
		key := []byte(fmt.Sprintf("%s/fout/%s", w.backendV2BasePrefix(), stableAnyKey(partitionKey)))
		ops = append(ops, StateBatchOp{Type: StateBatchPut, Key: key, Value: payload})
	}

	for partitionKey, state := range w.bucketedCumulativeState {
		if state == nil {
			continue
		}
		payload, err := encodeGobValue(*state)
		if err != nil {
			return err
		}
		key := []byte(fmt.Sprintf("%s/bcum/%s", w.backendV2BasePrefix(), stableAnyKey(partitionKey)))
		ops = append(ops, StateBatchOp{Type: StateBatchPut, Key: key, Value: payload})
	}

	return w.stateBackend.BatchWrite(ops)
}

func (w *WindowAggOp) ensureStateMaps() {
	if w.State.Data == nil {
		w.State.Data = make(map[WindowID]map[any]any)
	}
	if w.GroupCounts == nil {
		w.GroupCounts = make(map[WindowID]map[any]int64)
	}
	if w.SessionBuffers == nil {
		w.SessionBuffers = make(map[any]*PartitionBuffer)
	}
	if w.PartitionBuffers == nil {
		w.PartitionBuffers = make(map[any]*PartitionBuffer)
	}
	if w.sessionOut == nil {
		w.sessionOut = make(map[any]map[string]types.Tuple)
	}
	if w.frameOut == nil {
		w.frameOut = make(map[any]map[string]types.TupleDelta)
	}
	if w.cumulativeFrameCache == nil {
		w.cumulativeFrameCache = make(map[any]*cumulativeFramePartitionCache)
	}
	if w.bucketedCumulativeState == nil {
		w.bucketedCumulativeState = make(map[any]*bucketedCumulativePartitionState)
	}
}

func (w *WindowAggOp) injectGroupKeyColumns(outTuple types.Tuple, inTuple types.Tuple) {
	if outTuple == nil {
		return
	}
	if len(w.GroupKeys) == 0 && !w.KeepInput {
		return
	}
	if inTuple == nil {
		return
	}
	for _, col := range w.GroupKeys {
		outTuple[col] = inTuple[col]
	}
	if w.KeepInput {
		for k, v := range inTuple {
			if _, ok := outTuple[k]; ok {
				continue
			}
			outTuple[k] = v
		}
	}
}

func (w *WindowAggOp) aggValueTuple(aggState any) types.Tuple {
	vals := types.Tuple{}
	return w.extractAggResult(vals, aggState)
}

func hasAnyNonNilValue(t types.Tuple) bool {
	for _, v := range t {
		if v != nil {
			return true
		}
	}
	return false
}

func (w *WindowAggOp) buildWindowValueTuple(wid WindowID, inTuple types.Tuple, values types.Tuple) types.Tuple {
	out := types.Tuple{
		"__window_start": wid.Start,
		"__window_end":   wid.End,
	}
	w.injectGroupKeyColumns(out, inTuple)
	for k, v := range values {
		out[k] = v
	}
	return out
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
				delete(w.lastTouchedWindow, wid)
				w.windowTTLExpiry.remove(encodeWindowIDKey(wid))
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
	w.ensureProfiler()
	w.profileAppendHits = 0
	w.profileAppendMisses = 0
	if err := w.loadBackendState(); err != nil {
		return nil, err
	}
	w.ensureStateMaps()
	now := time.Now()
	var ttlOut types.Batch
	if shouldRunTTLCheck(&w.nextTTLCheck, now, w.StateTTL, w.ttlCheckInterval) {
		ttlOut = w.evictExpired(now)
	}

	var (
		out types.Batch
		err error
	)

	// Choose execution path based on frame specification
	if w.FrameSpec != nil && w.OrderByCol != "" {
		out, err = w.applyFrameBased(batch)
	} else {
		// Choose window type
		switch w.Spec.WindowType {
		case WindowTypeSliding:
			out, err = w.applySliding(batch)
		case WindowTypeSession:
			out, err = w.applySession(batch)
		default: // TUMBLING or empty
			out, err = w.applyTumbling(batch)
		}
	}
	if err != nil {
		return nil, err
	}
	w.gcByWatermark()
	if len(ttlOut) > 0 {
		out = append(ttlOut, out...)
	}
	if err := w.flushBackendState(); err != nil {
		return nil, err
	}
	w.profile.observeBatch(len(batch), out, w.profileAppendHits, w.profileAppendMisses, w.stateEntryCount())
	return out, nil
}

func (w *WindowAggOp) stateEntryCount() int {
	if w == nil {
		return 0
	}
	count := 0
	for _, groups := range w.State.Data {
		count += len(groups)
	}
	for _, buffer := range w.PartitionBuffers {
		if buffer != nil {
			count += len(buffer.Rows)
		}
	}
	for _, buffer := range w.SessionBuffers {
		if buffer != nil {
			count += len(buffer.Rows)
		}
	}
	for _, partition := range w.bucketedCumulativeState {
		if partition != nil {
			count += len(partition.Buckets)
		}
	}
	return count
}

// applyTumbling handles tumbling windows (original implementation)
func (w *WindowAggOp) applyTumbling(batch types.Batch) (types.Batch, error) {
	if w.EmitValue {
		return w.applyTumblingValue(batch)
	}
	var out types.Batch
	w.ensureStateMaps()
	now := time.Now()
	// Compact additive deltas per (window, groupKey).
	type winGroup struct {
		wid WindowID
		key any
	}
	pending := make(map[winGroup]*types.TupleDelta)
	baseCounts := make(map[winGroup]int64)
	countDeltas := make(map[winGroup]int64)

	for _, td := range batch {
		w.observeEventTimeDelta(td)
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
			if w.shouldDropByWatermark(wid) {
				continue
			}

			w.touchWindow(now, wid)

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
							pending[k] = delta
						} else {
							ex.Tuple["agg_delta"] = types.ToFloat64(ex.Tuple["agg_delta"]) + types.ToFloat64(delta.Tuple["agg_delta"])
						}
						if ex := pending[k]; ex != nil && types.ToFloat64(ex.Tuple["agg_delta"]) == 0 {
							delete(pending, k)
						}
						continue
					}
					if _, ok := delta.Tuple["avg_delta"]; ok {
						ex := pending[k]
						if ex == nil {
							pending[k] = delta
						} else {
							ex.Tuple["avg_delta"] = types.ToFloat64(ex.Tuple["avg_delta"]) + types.ToFloat64(delta.Tuple["avg_delta"])
						}
						if ex := pending[k]; ex != nil && types.ToFloat64(ex.Tuple["avg_delta"]) == 0 {
							delete(pending, k)
						}
						continue
					}
					if _, ok := delta.Tuple["count_delta"]; ok {
						ex := pending[k]
						if ex == nil {
							pending[k] = delta
						} else {
							ex.Tuple["count_delta"] = types.ToInt64(ex.Tuple["count_delta"]) + types.ToInt64(delta.Tuple["count_delta"])
						}
						if ex := pending[k]; ex != nil && types.ToInt64(ex.Tuple["count_delta"]) == 0 {
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

func (w *WindowAggOp) applyTumblingValue(batch types.Batch) (types.Batch, error) {
	var out types.Batch
	w.ensureStateMaps()
	now := time.Now()

	type winGroup struct {
		wid WindowID
		key any
	}
	type valueSnapshot struct {
		vals   types.Tuple
		exists bool
	}

	oldVals := make(map[winGroup]valueSnapshot)
	newVals := make(map[winGroup]types.Tuple)
	inputTuples := make(map[winGroup]types.Tuple)
	baseCounts := make(map[winGroup]int64)
	countDeltas := make(map[winGroup]int64)

	for _, td := range batch {
		w.observeEventTimeDelta(td)
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
			if w.shouldDropByWatermark(wid) {
				continue
			}

			w.touchWindow(now, wid)

			gm, ok := w.State.Data[wid]
			if !ok {
				gm = make(map[any]any)
				w.State.Data[wid] = gm
			}
			prev, existed := gm[groupKey]
			if !existed || prev == nil {
				prev = w.AggInit()
			}

			k := winGroup{wid: wid, key: groupKey}
			inputTuples[k] = td.Tuple
			if _, ok := baseCounts[k]; !ok {
				var base int64
				if cm := w.GroupCounts[wid]; cm != nil {
					base = cm[groupKey]
				}
				baseCounts[k] = base
			}
			countDeltas[k] += td.Count

			if _, ok := oldVals[k]; !ok {
				if existed {
					oldVals[k] = valueSnapshot{vals: w.aggValueTuple(prev), exists: true}
				} else {
					oldVals[k] = valueSnapshot{}
				}
			}

			newState, _ := w.AggFn.Apply(prev, td)
			gm[groupKey] = newState
			newVals[k] = w.aggValueTuple(newState)
		}
	}

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

	keys := make(map[winGroup]struct{})
	for k := range oldVals {
		keys[k] = struct{}{}
	}
	for k := range newVals {
		keys[k] = struct{}{}
	}

	for k := range keys {
		snap := oldVals[k]
		newVal := newVals[k]
		final := baseCounts[k] + countDeltas[k]
		if final <= 0 {
			if snap.exists && hasAnyNonNilValue(snap.vals) {
				out = append(out, types.TupleDelta{Tuple: w.buildWindowValueTuple(k.wid, inputTuples[k], snap.vals), Count: -1})
			}
			continue
		}
		if snap.exists && types.TuplesEqual(snap.vals, newVal) {
			continue
		}
		if snap.exists && hasAnyNonNilValue(snap.vals) {
			out = append(out, types.TupleDelta{Tuple: w.buildWindowValueTuple(k.wid, inputTuples[k], snap.vals), Count: -1})
		}
		if hasAnyNonNilValue(newVal) {
			out = append(out, types.TupleDelta{Tuple: w.buildWindowValueTuple(k.wid, inputTuples[k], newVal), Count: 1})
		}
	}

	return out, nil
}

// applySliding handles sliding windows
func (w *WindowAggOp) applySliding(batch types.Batch) (types.Batch, error) {
	if w.EmitValue {
		return w.applySlidingValue(batch)
	}
	var out types.Batch
	w.ensureStateMaps()
	now := time.Now()
	// Compact additive deltas per (window, groupKey).
	type winGroup struct {
		wid WindowID
		key any
	}
	pending := make(map[winGroup]*types.TupleDelta)
	baseCounts := make(map[winGroup]int64)
	countDeltas := make(map[winGroup]int64)

	for _, td := range batch {
		w.observeEventTimeDelta(td)
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
			if w.shouldDropByWatermark(wid) {
				continue
			}

			w.touchWindow(now, wid)

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
							pending[k] = delta
						} else {
							ex.Tuple["agg_delta"] = types.ToFloat64(ex.Tuple["agg_delta"]) + types.ToFloat64(delta.Tuple["agg_delta"])
						}
						if ex := pending[k]; ex != nil && types.ToFloat64(ex.Tuple["agg_delta"]) == 0 {
							delete(pending, k)
						}
						continue
					}
					if _, ok := delta.Tuple["avg_delta"]; ok {
						ex := pending[k]
						if ex == nil {
							pending[k] = delta
						} else {
							ex.Tuple["avg_delta"] = types.ToFloat64(ex.Tuple["avg_delta"]) + types.ToFloat64(delta.Tuple["avg_delta"])
						}
						if ex := pending[k]; ex != nil && types.ToFloat64(ex.Tuple["avg_delta"]) == 0 {
							delete(pending, k)
						}
						continue
					}
					if _, ok := delta.Tuple["count_delta"]; ok {
						ex := pending[k]
						if ex == nil {
							pending[k] = delta
						} else {
							ex.Tuple["count_delta"] = types.ToInt64(ex.Tuple["count_delta"]) + types.ToInt64(delta.Tuple["count_delta"])
						}
						if ex := pending[k]; ex != nil && types.ToInt64(ex.Tuple["count_delta"]) == 0 {
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

func (w *WindowAggOp) applySlidingValue(batch types.Batch) (types.Batch, error) {
	var out types.Batch
	w.ensureStateMaps()
	now := time.Now()

	type winGroup struct {
		wid WindowID
		key any
	}
	type valueSnapshot struct {
		vals   types.Tuple
		exists bool
	}

	oldVals := make(map[winGroup]valueSnapshot)
	newVals := make(map[winGroup]types.Tuple)
	inputTuples := make(map[winGroup]types.Tuple)
	baseCounts := make(map[winGroup]int64)
	countDeltas := make(map[winGroup]int64)

	for _, td := range batch {
		w.observeEventTimeDelta(td)
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

		for _, wid := range winIDs {
			if w.shouldDropByWatermark(wid) {
				continue
			}

			w.touchWindow(now, wid)

			gm, ok := w.State.Data[wid]
			if !ok {
				gm = make(map[any]any)
				w.State.Data[wid] = gm
			}
			prev, existed := gm[groupKey]
			if !existed || prev == nil {
				prev = w.AggInit()
			}

			k := winGroup{wid: wid, key: groupKey}
			inputTuples[k] = td.Tuple
			if _, ok := baseCounts[k]; !ok {
				var base int64
				if cm := w.GroupCounts[wid]; cm != nil {
					base = cm[groupKey]
				}
				baseCounts[k] = base
			}
			countDeltas[k] += td.Count

			if _, ok := oldVals[k]; !ok {
				if existed {
					oldVals[k] = valueSnapshot{vals: w.aggValueTuple(prev), exists: true}
				} else {
					oldVals[k] = valueSnapshot{}
				}
			}

			newState, _ := w.AggFn.Apply(prev, td)
			gm[groupKey] = newState
			newVals[k] = w.aggValueTuple(newState)
		}
	}

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

	keys := make(map[winGroup]struct{})
	for k := range oldVals {
		keys[k] = struct{}{}
	}
	for k := range newVals {
		keys[k] = struct{}{}
	}

	for k := range keys {
		snap := oldVals[k]
		newVal := newVals[k]
		final := baseCounts[k] + countDeltas[k]
		if final <= 0 {
			if snap.exists && hasAnyNonNilValue(snap.vals) {
				out = append(out, types.TupleDelta{Tuple: w.buildWindowValueTuple(k.wid, inputTuples[k], snap.vals), Count: -1})
			}
			continue
		}
		if snap.exists && types.TuplesEqual(snap.vals, newVal) {
			continue
		}
		if snap.exists && hasAnyNonNilValue(snap.vals) {
			out = append(out, types.TupleDelta{Tuple: w.buildWindowValueTuple(k.wid, inputTuples[k], snap.vals), Count: -1})
		}
		if hasAnyNonNilValue(newVal) {
			out = append(out, types.TupleDelta{Tuple: w.buildWindowValueTuple(k.wid, inputTuples[k], newVal), Count: 1})
		}
	}

	return out, nil
}

// applySession handles session windows.
// This implementation is stateful across batches and supports session extend/merge/split.
// For correctness and simplicity, we buffer per-partition events and recompute the
// sessionization for each touched partition, then emit output deltas as full tuple
// retractions/insertions.
func (w *WindowAggOp) applySession(batch types.Batch) (types.Batch, error) {
	var out types.Batch
	w.ensureStateMaps()
	now := time.Now()
	if w.Spec.GapMillis <= 0 {
		return nil, fmt.Errorf("session window requires GapMillis > 0")
	}

	// Track which partitions are touched by this batch.
	touched := make(map[any]struct{})

	// Update per-partition event buffers (insert/delete).
	for _, td := range batch {
		inputTuple := td.EnsureTuple()
		groupKey := w.KeyFn(inputTuple)
		touched[groupKey] = struct{}{}
		w.touchPartition(now, groupKey)
		pb := w.getOrCreateSessionBuffer(groupKey)
		if err := pb.addRowStrict(td, w.Spec.TimeCol); err != nil {
			return nil, err
		}
	}

	// Recompute sessions for each touched partition and diff against previous output.
	for groupKey := range touched {
		pb := w.getOrCreateSessionBuffer(groupKey)
		newTuples, err := w.computeSessionOutputForPartition(pb)
		if err != nil {
			return nil, err
		}
		newMap := make(map[string]types.Tuple, len(newTuples))
		for _, t := range newTuples {
			newMap[tupleKeyLocal(t)] = t
		}

		oldMap := w.sessionOut[groupKey]
		if oldMap == nil {
			oldMap = make(map[string]types.Tuple)
		}

		// Retractions
		for k, tup := range oldMap {
			if _, ok := newMap[k]; !ok {
				out = append(out, types.TupleDelta{Tuple: tup, Count: -1})
			}
		}
		// Insertions
		for k, tup := range newMap {
			if _, ok := oldMap[k]; !ok {
				out = append(out, types.TupleDelta{Tuple: tup, Count: 1})
			}
		}

		// Save
		if len(newMap) == 0 {
			w.clearSessionPartitionState(groupKey)
		} else {
			w.sessionOut[groupKey] = newMap
		}
	}

	return out, nil
}

func (w *WindowAggOp) getOrCreateSessionBuffer(key any) *PartitionBuffer {
	if w.SessionBuffers == nil {
		w.SessionBuffers = make(map[any]*PartitionBuffer)
	}
	buffer, ok := w.SessionBuffers[key]
	if !ok {
		buffer = &PartitionBuffer{Rows: []RowWithOrder{}, rowIndex: make(map[uint64][]int)}
		w.SessionBuffers[key] = buffer
	}
	return buffer
}

func (pb *PartitionBuffer) ensureRowIndex() {
	if pb == nil {
		return
	}
	if pb.rowIndex != nil {
		return
	}
	pb.rowIndex = make(map[uint64][]int, len(pb.Rows))
	for idx, row := range pb.Rows {
		hash := rowIdentityHash(row.Tuple, row.Packed)
		pb.Rows[idx].RowHash = hash
		pb.rowIndex[hash] = append(pb.rowIndex[hash], idx)
	}
}

func (pb *PartitionBuffer) invalidateRowIndex() {
	if pb == nil {
		return
	}
	pb.rowIndex = nil
}

func (pb *PartitionBuffer) lowerBound(orderValue any) int {
	return sort.Search(len(pb.Rows), func(i int) bool {
		return compareValues(pb.Rows[i].OrderValue, orderValue) >= 0
	})
}

func (pb *PartitionBuffer) upperBound(orderValue any) int {
	return sort.Search(len(pb.Rows), func(i int) bool {
		return compareValues(pb.Rows[i].OrderValue, orderValue) > 0
	})
}

func (pb *PartitionBuffer) findRow(orderValue any, tuple types.Tuple, packed *types.PackedTuple) (idx, start, end int) {
	pb.ensureRowIndex()
	start = pb.lowerBound(orderValue)
	end = pb.upperBound(orderValue)
	rowHash := rowIdentityHash(tuple, packed)
	if candidates, ok := pb.rowIndex[rowHash]; ok {
		for _, directIdx := range candidates {
			if directIdx < start || directIdx >= end || directIdx < 0 || directIdx >= len(pb.Rows) {
				continue
			}
			if compareValues(pb.Rows[directIdx].OrderValue, orderValue) == 0 && rowMatchesInput(pb.Rows[directIdx], tuple, packed) {
				return directIdx, start, end
			}
		}
	}
	for i := start; i < end; i++ {
		if pb.Rows[i].RowHash == rowHash && rowMatchesInput(pb.Rows[i], tuple, packed) {
			return i, start, end
		}
	}
	return -1, start, end
}

func (pb *PartitionBuffer) addRowStrict(td types.TupleDelta, orderByCol string) error {
	orderValue, ok := td.Get(orderByCol)
	if !ok {
		return fmt.Errorf("order by column %q not found", orderByCol)
	}
	var inputTuple types.Tuple
	if td.Packed == nil {
		inputTuple = td.EnsureTuple()
	}
	idx, _, end := pb.findRow(orderValue, inputTuple, td.Packed)

	if idx >= 0 {
		pb.Rows[idx].Count += td.Count
		if pb.Rows[idx].Count < 0 {
			return fmt.Errorf("row underflow for order=%v tuple=%v", orderValue, inputTuple)
		}
		if pb.Rows[idx].Count == 0 {
			pb.Rows = append(pb.Rows[:idx], pb.Rows[idx+1:]...)
			pb.invalidateRowIndex()
		}
		return nil
	}

	if td.Count < 0 {
		return fmt.Errorf("row not found for deletion order=%v", orderValue)
	}

	newRow := RowWithOrder{OrderValue: orderValue, Packed: types.ClonePackedTuple(td.Packed), RowHash: rowIdentityHash(inputTuple, td.Packed), Count: td.Count}
	if td.Packed == nil {
		newRow.Tuple = inputTuple
	}
	pb.Rows = append(pb.Rows, RowWithOrder{})
	copy(pb.Rows[end+1:], pb.Rows[end:])
	pb.Rows[end] = newRow
	pb.invalidateRowIndex()
	return nil
}

func (w *WindowAggOp) computeSessionOutputForPartition(buffer *PartitionBuffer) ([]types.Tuple, error) {
	if buffer == nil || len(buffer.Rows) == 0 {
		return nil, nil
	}
	// Ensure time column values are int64.
	getTS := func(row RowWithOrder) (int64, bool) {
		if row.OrderValue == nil {
			return 0, false
		}
		v, ok := row.OrderValue.(int64)
		return v, ok
	}

	var out []types.Tuple

	var sessionRows []RowWithOrder
	var startTS int64
	var lastTS int64
	var haveSession bool

	flush := func() error {
		if !haveSession || len(sessionRows) == 0 {
			return nil
		}
		aggState := w.AggInit()
		for _, r := range sessionRows {
			td := types.TupleDelta{Tuple: r.materializeTuple(), Count: r.Count}
			newVal, _ := w.AggFn.Apply(aggState, td)
			aggState = newVal
		}
		wid := WindowID{Start: startTS, End: lastTS + w.Spec.GapMillis}
		tup := types.Tuple{
			"__window_start": wid.Start,
			"__window_end":   wid.End,
		}
		w.injectGroupKeyColumns(tup, sessionRows[0].materializeTuple())
		tup = w.extractAggResult(tup, aggState)
		out = append(out, tup)
		return nil
	}

	for _, r := range buffer.Rows {
		ts, ok := getTS(r)
		if !ok {
			continue
		}
		if !haveSession {
			haveSession = true
			startTS = ts
			lastTS = ts
			sessionRows = sessionRows[:0]
			sessionRows = append(sessionRows, r)
			continue
		}
		if (ts - lastTS) > w.Spec.GapMillis {
			if err := flush(); err != nil {
				return nil, err
			}
			startTS = ts
			lastTS = ts
			sessionRows = sessionRows[:0]
			sessionRows = append(sessionRows, r)
			continue
		}
		// Extend current session.
		if ts > lastTS {
			lastTS = ts
		}
		sessionRows = append(sessionRows, r)
	}
	if err := flush(); err != nil {
		return nil, err
	}
	return out, nil
}

func tupleKeyLocal(tup types.Tuple) string {
	return stableTupleKeyCanonical(tup)
}

func (w *WindowAggOp) frameKeyColumnsForRows() []string {
	if len(w.frameKeyColumns) > 0 {
		return w.frameKeyColumns
	}
	cols := make([]string, 0, len(w.GroupKeys)+1)
	if w.OrderByCol != "" {
		cols = append(cols, w.OrderByCol)
	}
	for _, key := range w.GroupKeys {
		if key == w.OrderByCol {
			continue
		}
		cols = append(cols, key)
	}
	w.frameKeyColumns = cols
	return w.frameKeyColumns
}

func (w *WindowAggOp) frameRowKey(tup types.Tuple) string {
	if tup == nil {
		return ""
	}
	return stableTupleKeyForColumns(tup, w.frameKeyColumnsForRows())
}

func (w *WindowAggOp) frameRowKeyForRow(row RowWithOrder) string {
	cols := w.frameKeyColumnsForRows()
	if len(cols) == 0 {
		return ""
	}
	keyTuple := make(types.Tuple, len(cols))
	for _, col := range cols {
		if value, ok := row.get(col); ok {
			keyTuple[col] = value
		}
	}
	return stableTupleKeyForColumns(keyTuple, cols)
}

// applyFrameBased handles frame-based windows (RANGE/ROWS BETWEEN)
func (w *WindowAggOp) applyFrameBased(batch types.Batch) (types.Batch, error) {
	if w.EmitValue {
		return w.applyFrameBasedValue(batch)
	}
	var out types.Batch
	now := time.Now()

	// Group by partition
	partitionDeltas := make(map[any][]types.TupleDelta)
	for _, td := range batch {
		inputTuple := td.EnsureTuple()
		partitionKey := w.KeyFn(inputTuple)
		partitionDeltas[partitionKey] = append(partitionDeltas[partitionKey], td)
	}

	// Process each partition
	for partitionKey, deltas := range partitionDeltas {
		w.touchPartition(now, partitionKey)
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

func (w *WindowAggOp) applyFrameBasedValue(batch types.Batch) (types.Batch, error) {
	if w.isCumulativeRowsFrame() {
		return w.applyCumulativeFrameBasedValue(batch)
	}

	var out types.Batch
	now := time.Now()

	partitionDeltas := make(map[any][]types.TupleDelta)
	for _, td := range batch {
		inputTuple := td.EnsureTuple()
		partitionKey := w.KeyFn(inputTuple)
		partitionDeltas[partitionKey] = append(partitionDeltas[partitionKey], td)
	}

	for partitionKey, deltas := range partitionDeltas {
		w.touchPartition(now, partitionKey)
		buffer := w.getOrCreatePartitionBuffer(partitionKey)
		for _, td := range deltas {
			buffer.addRow(td, w.OrderByCol)
		}

		frameOut, err := w.computeFrameAggregates(buffer, partitionKey)
		if err != nil {
			return nil, err
		}

		newMap := make(map[string]types.TupleDelta, len(frameOut))
		for _, td := range frameOut {
			key := w.frameTupleDeltaKey(td)
			newMap[key] = td
		}
		oldMap := w.frameOut[partitionKey]
		if oldMap == nil {
			oldMap = make(map[string]types.TupleDelta)
		}

		for k, oldTd := range oldMap {
			newTd, ok := newMap[k]
			if !ok {
				out = append(out, negateTupleDelta(oldTd))
				continue
			}
			if !tupleDeltaPayloadEqual(oldTd, newTd) {
				out = append(out, negateTupleDelta(oldTd))
				out = append(out, newTd)
				continue
			}
			if newTd.Count != oldTd.Count {
				diff := newTd.Count - oldTd.Count
				if diff != 0 {
					out = append(out, types.TupleDelta{Tuple: newTd.Tuple, Packed: newTd.Packed, Count: diff})
				}
			}
		}

		for k, newTd := range newMap {
			if _, ok := oldMap[k]; !ok {
				out = append(out, newTd)
			}
		}

		if len(newMap) == 0 {
			w.clearFramePartitionState(partitionKey)
		} else {
			w.frameOut[partitionKey] = newMap
		}
	}

	return out, nil
}

func (w *WindowAggOp) getOrCreateCumulativeFrameCache(key any) *cumulativeFramePartitionCache {
	if w.cumulativeFrameCache == nil {
		w.cumulativeFrameCache = make(map[any]*cumulativeFramePartitionCache)
	}
	cache, ok := w.cumulativeFrameCache[key]
	if !ok {
		cache = &cumulativeFramePartitionCache{}
		w.cumulativeFrameCache[key] = cache
	}
	return cache
}

// getOrCreatePartitionBuffer retrieves or creates a partition buffer
func (w *WindowAggOp) getOrCreatePartitionBuffer(key any) *PartitionBuffer {
	if w.PartitionBuffers == nil {
		w.PartitionBuffers = make(map[any]*PartitionBuffer)
	}
	buffer, ok := w.PartitionBuffers[key]
	if !ok {
		buffer = &PartitionBuffer{Rows: []RowWithOrder{}, rowIndex: make(map[uint64][]int)}
		w.PartitionBuffers[key] = buffer
	}
	return buffer
}

// addRow adds or removes a row from the partition buffer
func (pb *PartitionBuffer) addRow(td types.TupleDelta, orderByCol string) {
	_, _ = pb.addRowTracked(td, orderByCol)
}

func (pb *PartitionBuffer) addRowTracked(td types.TupleDelta, orderByCol string) (affectedIndex int, appended bool) {
	orderValue, ok := td.Get(orderByCol)
	if !ok {
		return len(pb.Rows), false
	}
	var inputTuple types.Tuple
	if td.Packed == nil {
		inputTuple = td.EnsureTuple()
	}

	idx, start, end := pb.findRow(orderValue, inputTuple, td.Packed)

	if idx >= 0 {
		// Update existing row
		pb.Rows[idx].Count += td.Count
		if pb.Rows[idx].Count == 0 {
			// Remove row
			pb.Rows = append(pb.Rows[:idx], pb.Rows[idx+1:]...)
			pb.invalidateRowIndex()
		}
		return idx, false
	} else if td.Count > 0 {
		canAppend := len(pb.Rows) == 0 || compareValues(pb.Rows[len(pb.Rows)-1].OrderValue, orderValue) < 0
		// Insert new row
		newRow := RowWithOrder{
			OrderValue: orderValue,
			Packed:     types.ClonePackedTuple(td.Packed),
			RowHash:    rowIdentityHash(inputTuple, td.Packed),
			Count:      td.Count,
		}
		if td.Packed == nil {
			newRow.Tuple = inputTuple
		}
		if canAppend {
			pb.Rows = append(pb.Rows, newRow)
			if pb.rowIndex != nil {
				pb.rowIndex[newRow.RowHash] = append(pb.rowIndex[newRow.RowHash], len(pb.Rows)-1)
			}
			return len(pb.Rows) - 1, true
		}
		insertAt := end
		if start == end {
			insertAt = start
		}
		pb.Rows = append(pb.Rows, RowWithOrder{})
		copy(pb.Rows[insertAt+1:], pb.Rows[insertAt:])
		pb.Rows[insertAt] = newRow
		pb.invalidateRowIndex()
		return insertAt, false
	}
	return len(pb.Rows), false
}

func rowIdentityHash(tuple types.Tuple, packed *types.PackedTuple) uint64 {
	if packed != nil {
		return stablePackedTupleOrderHash(packed)
	}
	return stableTupleOrderHash(tuple)
}

func rowMatchesInput(row RowWithOrder, tuple types.Tuple, packed *types.PackedTuple) bool {
	if row.Packed != nil && packed != nil {
		return packedTuplesEqual(row.Packed, packed)
	}
	return types.TuplesEqual(row.materializeTuple(), tuple)
}

// computeFrameAggregates computes aggregates for all rows in the partition
func (w *WindowAggOp) computeFrameAggregates(buffer *PartitionBuffer, partitionKey any) (types.Batch, error) {
	if w.isCumulativeRowsFrame() {
		return w.computeCumulativeFrameAggregates(buffer, partitionKey)
	}

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
			frameTuple := frameRow.materializeTuple()
			for c := int64(0); c < frameRow.Count; c++ {
				td := types.TupleDelta{Tuple: frameTuple, Count: 1}
				aggState, _ = w.AggFn.Apply(aggState, td)
			}
		}

		// Extract result value
		rowTuple := row.materializeTuple()
		resultTuple := make(types.Tuple)
		for k, v := range rowTuple {
			resultTuple[k] = v
		}

		// Add aggregate result
		resultTuple = w.extractAggResult(resultTuple, aggState)

		// Add partition key columns
		if len(w.GroupKeys) > 0 {
			for _, col := range w.GroupKeys {
				resultTuple[col] = rowTuple[col]
			}
		}

		out = append(out, types.TupleDelta{Tuple: resultTuple, Count: row.Count})
	}

	return out, nil
}

func (w *WindowAggOp) isCumulativeRowsFrame() bool {
	if w.FrameSpec == nil {
		return false
	}
	return strings.EqualFold(w.FrameSpec.Type, "ROWS") &&
		strings.EqualFold(w.FrameSpec.StartType, "UNBOUNDED PRECEDING") &&
		strings.EqualFold(w.FrameSpec.EndType, "CURRENT ROW")
}

func (w *WindowAggOp) computeCumulativeFrameAggregates(buffer *PartitionBuffer, partitionKey any) (types.Batch, error) {
	var out types.Batch
	if buffer == nil || len(buffer.Rows) == 0 {
		return out, nil
	}

	aggState := w.AggInit()
	for _, row := range buffer.Rows {
		rowTuple := row.materializeTuple()
		for c := int64(0); c < row.Count; c++ {
			var outDelta *types.TupleDelta
			aggState, outDelta = w.AggFn.Apply(aggState, types.TupleDelta{Tuple: rowTuple, Count: 1})
			_ = outDelta
		}
		out = append(out, w.buildFrameResultDelta(row, aggState))
	}

	return out, nil
}

func (w *WindowAggOp) buildFrameResultDelta(row RowWithOrder, aggState any) types.TupleDelta {
	if row.Packed != nil {
		return types.TupleDelta{Packed: row.Packed.WithExtras(w.aggValueTuple(aggState)), Count: row.Count}
	}
	rowTuple := row.materializeTuple()
	resultTuple := make(types.Tuple, len(rowTuple)+len(w.GroupKeys)+1)
	for k, v := range rowTuple {
		resultTuple[k] = v
	}
	resultTuple = w.extractAggResult(resultTuple, aggState)
	if len(w.GroupKeys) > 0 {
		for _, col := range w.GroupKeys {
			resultTuple[col] = rowTuple[col]
		}
	}
	return types.TupleDelta{Tuple: resultTuple, Count: row.Count}
}

func cloneCumulativeAggState(state any) any {
	switch v := state.(type) {
	case nil:
		return nil
	case SortedMultiset:
		values := make(map[string]int64, len(v.values))
		for key, count := range v.values {
			values[key] = count
		}
		return SortedMultiset{values: values, sorted: append([]string(nil), v.sorted...)}
	case OrderedBuffer:
		return OrderedBuffer{entries: append([]BufferEntry(nil), v.entries...), orderByCol: v.orderByCol}
	default:
		return v
	}
}

func (w *WindowAggOp) applyCumulativeFrameBasedValue(batch types.Batch) (types.Batch, error) {
	if !w.canUseBucketedCumulativeSumFastPath() {
		return w.applyCumulativeFrameBasedValueGeneric(batch)
	}
	if !w.bucketedCumulativeBatchCompatible(batch) {
		w.promoteBucketedCumulativeStateToFrameBuffers()
		return w.applyCumulativeFrameBasedValueGeneric(batch)
	}
	return w.applyBucketedCumulativeSumValue(batch)
}

func (w *WindowAggOp) applyCumulativeFrameBasedValueGeneric(batch types.Batch) (types.Batch, error) {
	var out types.Batch
	now := time.Now()

	partitionDeltas := make(map[any][]types.TupleDelta)
	for _, td := range batch {
		partitionKey := w.KeyFn(td.EnsureTuple())
		partitionDeltas[partitionKey] = append(partitionDeltas[partitionKey], td)
	}

	for partitionKey, deltas := range partitionDeltas {
		w.touchPartition(now, partitionKey)
		buffer := w.getOrCreatePartitionBuffer(partitionKey)
		cache := w.getOrCreateCumulativeFrameCache(partitionKey)
		oldLen := len(buffer.Rows)
		affectedStart := oldLen
		appendOnly := true

		for _, td := range deltas {
			idx, appended := buffer.addRowTracked(td, w.OrderByCol)
			if idx < affectedStart {
				affectedStart = idx
			}
			if !appended {
				appendOnly = false
			}
		}
		appendFastPath := appendOnly && affectedStart == oldLen

		frameOut, err := w.recomputeCumulativeFrameSuffix(partitionKey, buffer, cache, affectedStart, appendOnly)
		if err != nil {
			return nil, err
		}
		if appendFastPath {
			w.profileAppendHits++
		} else {
			w.profileAppendMisses++
		}
		out = append(out, frameOut...)
	}

	return out, nil
}

func (w *WindowAggOp) canUseBucketedCumulativeSumFastPath() bool {
	if w == nil || !w.EmitValue || !w.isCumulativeRowsFrame() || strings.TrimSpace(w.OrderByCol) == "" {
		return false
	}
	if _, ok := w.AggFn.(*SumAgg); !ok {
		return false
	}
	for _, buffer := range w.PartitionBuffers {
		if buffer != nil && len(buffer.Rows) > 0 {
			return false
		}
	}
	for _, out := range w.frameOut {
		if len(out) > 0 {
			return false
		}
	}
	for _, cache := range w.cumulativeFrameCache {
		if cache != nil && cache.rowCount > 0 {
			return false
		}
	}
	return true
}

func (w *WindowAggOp) bucketedCumulativeBatchCompatible(batch types.Batch) bool {
	if len(batch) == 0 {
		return true
	}
	type partitionOrderState struct {
		lastOrder any
		seen      bool
	}
	seen := make(map[any]partitionOrderState)
	for _, td := range batch {
		inputTuple := td.EnsureTuple()
		if inputTuple == nil {
			return false
		}
		partitionKey := w.KeyFn(inputTuple)
		orderValue := inputTuple[w.OrderByCol]
		partition := w.bucketedCumulativeState[partitionKey]
		if partition != nil && partition.CurrentBucketID != "" && compareValues(orderValue, partition.CurrentOrder) < 0 {
			return false
		}
		state := seen[partitionKey]
		if state.seen && compareValues(orderValue, state.lastOrder) < 0 {
			return false
		}
		state.lastOrder = orderValue
		state.seen = true
		seen[partitionKey] = state
	}
	return true
}

func (w *WindowAggOp) getOrCreateBucketedCumulativePartitionState(key any) *bucketedCumulativePartitionState {
	if w.bucketedCumulativeState == nil {
		w.bucketedCumulativeState = make(map[any]*bucketedCumulativePartitionState)
	}
	partition, ok := w.bucketedCumulativeState[key]
	if !ok {
		partition = &bucketedCumulativePartitionState{Buckets: make(map[string]*bucketedCumulativeBucketState)}
		w.bucketedCumulativeState[key] = partition
	}
	if partition.Buckets == nil {
		partition.Buckets = make(map[string]*bucketedCumulativeBucketState)
	}
	return partition
}

func (w *WindowAggOp) applyBucketedCumulativeSumValue(batch types.Batch) (types.Batch, error) {
	var out types.Batch
	now := time.Now()

	partitionDeltas := make(map[any][]types.TupleDelta)
	for _, td := range batch {
		inputTuple := td.EnsureTuple()
		partitionKey := w.KeyFn(inputTuple)
		partitionDeltas[partitionKey] = append(partitionDeltas[partitionKey], td)
	}

	for partitionKey, deltas := range partitionDeltas {
		w.touchPartition(now, partitionKey)
		partition := w.getOrCreateBucketedCumulativePartitionState(partitionKey)

		for start := 0; start < len(deltas); {
			inputTuple := deltas[start].EnsureTuple()
			orderValue := inputTuple[w.OrderByCol]
			bucketID := stableAnyKey(orderValue)

			if partition.CurrentBucketID == "" {
				partition.CurrentBucketID = bucketID
				partition.CurrentOrder = orderValue
			} else if bucketID != partition.CurrentBucketID {
				cmp := compareValues(orderValue, partition.CurrentOrder)
				if cmp < 0 {
					return nil, fmt.Errorf("bucketed cumulative sum requires monotonic %s within partition", w.OrderByCol)
				}
				if cmp > 0 {
					if current := partition.Buckets[partition.CurrentBucketID]; current != nil {
						partition.ClosedPrefixSum += types.ToFloat64(aggValueFromState(w.AggFn, current.AggState))
					}
					partition.CurrentBucketID = bucketID
					partition.CurrentOrder = orderValue
				}
			}

			bucket := partition.Buckets[bucketID]
			if bucket == nil {
				bucket = &bucketedCumulativeBucketState{OrderValue: orderValue}
				partition.Buckets[bucketID] = bucket
			}
			oldOutput := bucket.Output
			hadOldOutput := oldOutput.Tuple != nil || oldOutput.Packed != nil

			end := start
			for end < len(deltas) {
				orderVal, _ := deltas[end].Get(w.OrderByCol)
				if stableAnyKey(orderVal) != bucketID {
					break
				}
				newState, _ := w.AggFn.Apply(bucket.AggState, deltas[end])
				bucket.AggState = newState
				if deltas[end].Count > 0 {
					if deltas[end].Packed != nil {
						bucket.BasePacked = types.ClonePackedTuple(deltas[end].Packed)
						bucket.BaseTuple = nil
					} else {
						bucket.BaseTuple = types.CloneTuple(deltas[end].EnsureTuple())
						bucket.BasePacked = nil
					}
				}
				end++
			}

			currentValue := types.ToFloat64(aggValueFromState(w.AggFn, bucket.AggState))
			if bucket.BaseTuple == nil && bucket.BasePacked == nil {
				if hadOldOutput {
					out = append(out, negateTupleDelta(oldOutput))
				}
				w.bucketTTLExpiry.remove(encodeBucketTTLKey(partitionKey, bucketID))
				delete(partition.Buckets, bucketID)
				if bucketID == partition.CurrentBucketID {
					partition.CurrentBucketID = ""
					partition.CurrentOrder = nil
				}
				start = end
				continue
			}

			newOutput := w.buildBucketedCumulativeOutput(bucket, partition.ClosedPrefixSum+currentValue)
			bucket.Output = newOutput
			w.touchBucket(now, partitionKey, bucketID)
			if hadOldOutput {
				if !tupleDeltaPayloadEqual(oldOutput, newOutput) || oldOutput.Count != newOutput.Count {
					out = append(out, negateTupleDelta(oldOutput))
					out = append(out, newOutput)
				}
			} else {
				out = append(out, newOutput)
			}

			start = end
		}
	}

	return out, nil
}

func (w *WindowAggOp) buildBucketedCumulativeOutput(bucket *bucketedCumulativeBucketState, cumulative float64) types.TupleDelta {
	return w.buildFrameResultDelta(RowWithOrder{
		Tuple:  bucket.BaseTuple,
		Packed: bucket.BasePacked,
		Count:  1,
	}, cumulative)
}

func (w *WindowAggOp) promoteBucketedCumulativeStateToFrameBuffers() {
	if len(w.bucketedCumulativeState) == 0 {
		return
	}
	w.ensureStateMaps()
	for partitionKey, partition := range w.bucketedCumulativeState {
		if partition == nil || len(partition.Buckets) == 0 {
			continue
		}
		bucketIDs := make([]string, 0, len(partition.Buckets))
		for bucketID := range partition.Buckets {
			bucketIDs = append(bucketIDs, bucketID)
		}
		sort.Slice(bucketIDs, func(i, j int) bool {
			left := partition.Buckets[bucketIDs[i]]
			right := partition.Buckets[bucketIDs[j]]
			return compareValues(left.OrderValue, right.OrderValue) < 0
		})

		pb := &PartitionBuffer{Rows: []RowWithOrder{}, rowIndex: make(map[uint64][]int)}
		cachedOut := make(map[string]frameOutputCacheEntry, len(bucketIDs))
		for _, bucketID := range bucketIDs {
			bucket := partition.Buckets[bucketID]
			if bucket == nil || (bucket.BaseTuple == nil && bucket.BasePacked == nil) {
				continue
			}
			w.bucketTTLExpiry.remove(encodeBucketTTLKey(partitionKey, bucketID))
			td := types.TupleDelta{Tuple: types.CloneTuple(bucket.BaseTuple), Packed: types.ClonePackedTuple(bucket.BasePacked), Count: 1}
			pb.addRow(td, w.OrderByCol)
			if bucket.Output.Tuple != nil || bucket.Output.Packed != nil {
				cachedOut[w.frameTupleDeltaKey(bucket.Output)] = frameOutputCacheEntryFromTupleDelta(bucket.Output)
			}
		}
		if len(pb.Rows) > 0 {
			w.PartitionBuffers[partitionKey] = pb
		}
		if len(cachedOut) > 0 {
			cache := w.getOrCreateCumulativeFrameCache(partitionKey)
			cache.outputs = cachedOut
		}
	}
	w.bucketedCumulativeState = make(map[any]*bucketedCumulativePartitionState)
}

func (w *WindowAggOp) recomputeCumulativeFrameSuffix(partitionKey any, buffer *PartitionBuffer, cache *cumulativeFramePartitionCache, affectedStart int, appendOnly bool) (types.Batch, error) {
	if cache == nil {
		cache = &cumulativeFramePartitionCache{}
		w.cumulativeFrameCache[partitionKey] = cache
	}
	if affectedStart < 0 {
		affectedStart = 0
	}
	if affectedStart > len(buffer.Rows) {
		affectedStart = len(buffer.Rows)
	}

	partitionOut := cache.outputs
	if partitionOut == nil {
		partitionOut = make(map[string]frameOutputCacheEntry)
	}
	if buffer == nil || len(buffer.Rows) == 0 {
		out := make(types.Batch, 0, len(partitionOut))
		for _, oldEntry := range partitionOut {
			out = append(out, negateTupleDelta(frameOutputCacheEntryToTupleDelta(oldEntry)))
		}
		w.clearFramePartitionState(partitionKey)
		return out, nil
	}
	canTailReplace := !appendOnly && len(buffer.Rows) > 0 && len(buffer.Rows) == cache.rowCount && affectedStart == len(buffer.Rows)-1

	if appendOnly && affectedStart == cache.rowCount && affectedStart <= len(buffer.Rows) {
		currentState := w.AggInit()
		if affectedStart > 0 {
			currentState = cloneCumulativeAggState(cache.tailState)
		}

		out := make(types.Batch, 0, len(buffer.Rows)-affectedStart)
		previousState := cloneCumulativeAggState(currentState)
		var tailOutput frameOutputCacheEntry
		for idx := affectedStart; idx < len(buffer.Rows); idx++ {
			row := buffer.Rows[idx]
			previousState = cloneCumulativeAggState(currentState)
			rowTuple := row.materializeTuple()
			for c := int64(0); c < row.Count; c++ {
				var outDelta *types.TupleDelta
				currentState, outDelta = w.AggFn.Apply(currentState, types.TupleDelta{Tuple: rowTuple, Count: 1})
				_ = outDelta
			}
			entry := w.buildFrameOutputCacheEntry(row, currentState)
			partitionOut[w.frameRowKeyForRow(row)] = entry
			tailOutput = entry
			out = append(out, frameOutputCacheEntryToTupleDelta(entry))
		}
		cache.outputs = partitionOut
		cache.rowCount = len(buffer.Rows)
		if len(buffer.Rows) > 1 {
			cache.beforeTailState = previousState
		} else {
			cache.beforeTailState = nil
		}
		cache.tailState = cloneCumulativeAggState(currentState)
		cache.tailOutput = tailOutput
		return out, nil
	}

	if canTailReplace {
		row := buffer.Rows[len(buffer.Rows)-1]
		currentState := w.AggInit()
		if len(buffer.Rows) > 1 {
			currentState = cloneCumulativeAggState(cache.beforeTailState)
		}
		for c := int64(0); c < row.Count; c++ {
			var outDelta *types.TupleDelta
			currentState, outDelta = w.AggFn.Apply(currentState, types.TupleDelta{Tuple: row.materializeTuple(), Count: 1})
			_ = outDelta
		}
		newTail := w.buildFrameResultDelta(row, currentState)
		oldTail := frameOutputCacheEntryToTupleDelta(cache.tailOutput)
		oldKey := w.frameRowKeyForRow(row)
		newKey := w.frameRowKeyForRow(row)
		var out types.Batch
		if frameOutputCacheEntryValid(cache.tailOutput) {
			if oldKey != newKey {
				delete(partitionOut, oldKey)
				out = append(out, negateTupleDelta(oldTail))
				partitionOut[newKey] = w.buildFrameOutputCacheEntry(row, currentState)
				out = append(out, newTail)
			} else if !tupleDeltaPayloadEqual(oldTail, newTail) {
				partitionOut[newKey] = w.buildFrameOutputCacheEntry(row, currentState)
				out = append(out, negateTupleDelta(oldTail))
				out = append(out, newTail)
			} else if newTail.Count != oldTail.Count {
				partitionOut[newKey] = w.buildFrameOutputCacheEntry(row, currentState)
				diff := newTail.Count - oldTail.Count
				if diff != 0 {
					out = append(out, types.TupleDelta{Tuple: newTail.Tuple, Packed: newTail.Packed, Count: diff})
				}
			}
		} else {
			partitionOut[newKey] = w.buildFrameOutputCacheEntry(row, currentState)
			out = append(out, newTail)
		}
		cache.outputs = partitionOut
		cache.tailState = cloneCumulativeAggState(currentState)
		cache.tailOutput = w.buildFrameOutputCacheEntry(row, currentState)
		return out, nil
	}

	oldMap := make(map[string]frameOutputCacheEntry, len(partitionOut))
	for key, entry := range partitionOut {
		oldMap[key] = entry
	}

	newMap := make(map[string]frameOutputCacheEntry, len(buffer.Rows))
	currentState := w.AggInit()
	previousState := cloneCumulativeAggState(currentState)
	var tailOutput frameOutputCacheEntry
	for _, row := range buffer.Rows {
		previousState = cloneCumulativeAggState(currentState)
		rowTuple := row.materializeTuple()
		for c := int64(0); c < row.Count; c++ {
			var outDelta *types.TupleDelta
			currentState, outDelta = w.AggFn.Apply(currentState, types.TupleDelta{Tuple: rowTuple, Count: 1})
			_ = outDelta
		}
		entry := w.buildFrameOutputCacheEntry(row, currentState)
		newMap[w.frameRowKeyForRow(row)] = entry
		tailOutput = entry
	}

	var out types.Batch
	for key, oldEntry := range oldMap {
		newEntry, ok := newMap[key]
		oldTd := frameOutputCacheEntryToTupleDelta(oldEntry)
		if !ok {
			out = append(out, negateTupleDelta(oldTd))
			continue
		}
		newTd := frameOutputCacheEntryToTupleDelta(newEntry)
		if !tupleDeltaPayloadEqual(oldTd, newTd) {
			out = append(out, negateTupleDelta(oldTd))
			out = append(out, newTd)
			continue
		}
		if newTd.Count != oldTd.Count {
			diff := newTd.Count - oldTd.Count
			if diff != 0 {
				out = append(out, types.TupleDelta{Tuple: newTd.Tuple, Packed: newTd.Packed, Count: diff})
			}
		}
	}
	for key, newEntry := range newMap {
		if _, ok := oldMap[key]; !ok {
			out = append(out, frameOutputCacheEntryToTupleDelta(newEntry))
		}
	}

	if len(newMap) == 0 {
		w.clearFramePartitionState(partitionKey)
	} else {
		cache.outputs = newMap
	}
	cache.rowCount = len(buffer.Rows)
	if len(buffer.Rows) > 1 {
		cache.beforeTailState = previousState
	} else {
		cache.beforeTailState = nil
	}
	cache.tailState = cloneCumulativeAggState(currentState)
	cache.tailOutput = tailOutput
	return out, nil
}

func (w *WindowAggOp) buildFrameOutputCacheEntry(row RowWithOrder, aggState any) frameOutputCacheEntry {
	entry := frameOutputCacheEntry{AggValues: w.aggValueTuple(aggState), Count: row.Count}
	if row.Packed != nil {
		entry.BasePacked = types.ClonePackedTuple(row.Packed)
		return entry
	}
	entry.BaseTuple = types.CloneTuple(row.materializeTuple())
	return entry
}

func frameOutputCacheEntryFromTupleDelta(td types.TupleDelta) frameOutputCacheEntry {
	entry := frameOutputCacheEntry{Count: td.Count}
	if td.Packed != nil {
		entry.BasePacked = types.ClonePackedTuple(td.Packed)
		return entry
	}
	entry.BaseTuple = types.CloneTuple(td.Tuple)
	return entry
}

func frameOutputCacheEntryToTupleDelta(entry frameOutputCacheEntry) types.TupleDelta {
	if entry.BasePacked != nil {
		return types.TupleDelta{Packed: entry.BasePacked.WithExtras(entry.AggValues), Count: entry.Count}
	}
	tuple := make(types.Tuple, len(entry.BaseTuple)+len(entry.AggValues))
	for key, value := range entry.BaseTuple {
		tuple[key] = value
	}
	for key, value := range entry.AggValues {
		tuple[key] = value
	}
	return types.TupleDelta{Tuple: tuple, Count: entry.Count}
}

func frameOutputCacheEntryValid(entry frameOutputCacheEntry) bool {
	return entry.BaseTuple != nil || entry.BasePacked != nil
}

func (w *WindowAggOp) frameTupleDeltaKey(td types.TupleDelta) string {
	if len(w.frameKeyColumnsForRows()) == 0 {
		return ""
	}
	keyTuple := make(types.Tuple, len(w.frameKeyColumnsForRows()))
	for _, col := range w.frameKeyColumnsForRows() {
		if value, ok := td.Get(col); ok {
			keyTuple[col] = value
		}
	}
	return stableTupleKeyForColumns(keyTuple, w.frameKeyColumnsForRows())
}

func negateTupleDelta(td types.TupleDelta) types.TupleDelta {
	return types.TupleDelta{Tuple: td.Tuple, Packed: td.Packed, Count: -td.Count}
}

func tupleDeltaPayloadEqual(left, right types.TupleDelta) bool {
	if left.Packed != nil || right.Packed != nil {
		return packedTuplesEqual(left.Packed, right.Packed)
	}
	return types.TuplesEqual(left.Tuple, right.Tuple)
}

func packedTuplesEqual(left, right *types.PackedTuple) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	if !packedRowsEqualIgnoringExtras(left, right) {
		return false
	}
	if len(left.Extras) != len(right.Extras) {
		return false
	}
	for key, value := range left.Extras {
		other, ok := right.Extras[key]
		if !ok || !types.EqualAny(value, other) {
			return false
		}
	}
	return true
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
	return types.TuplesEqual(a, b)
}
