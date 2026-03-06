package op

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/ariyn/dbsp/internal/dbsp/types"
)

type orderedWindowSnapshotV1 struct {
	Partitions  map[any]*orderedWindowPartition
	StateTTL    time.Duration
	OnlyLastLag bool
	LastTouched map[any]time.Time
}

type orderedWindowPartition struct {
	Rows         []orderedWindowRow
	NextSeq      int64
	MinExpiresAt time.Time
}

type orderedWindowRow struct {
	OrderValue any
	Tuple      types.Tuple
	Packed     *types.PackedTuple
	TieBreaker string
	Seq        int64
	ExpiresAt  time.Time
}

type orderedWindowMutation struct {
	oldPos int
	newPos int
	row    orderedWindowRow
	append bool
}

type orderedWindowOutput struct {
	row       orderedWindowRow
	lagValues []any
	count     int64
}

type OrderedWindowLag struct {
	LagCol    string
	LagExpr   func(types.Tuple) (any, error)
	OutputCol string
}

type OrderedWindowOp struct {
	KeyFn         func(types.Tuple) any
	PartitionCols []string
	OrderByCol    string
	LagCol        string
	LagExpr       func(types.Tuple) (any, error)
	Offset        int
	OutputCol     string
	LagOutputs    []OrderedWindowLag
	profile       operatorApplyProfile

	Partitions       map[any]*orderedWindowPartition
	StateTTL         time.Duration
	OnlyLastLag      bool
	lastTouched      map[any]time.Time
	ttlCheckInterval time.Duration
	nextTTLCheck     time.Time
	ttlExpiry        ttlExpiryQueue
	stateBackend     StateBackend
	statePrefix      string
	backendLoaded    bool
}

func (w *OrderedWindowOp) SupportsPackedBatch() bool { return true }

func NewOrderedWindowOp(keyFn func(types.Tuple) any, orderByCol, lagCol string, offset int, outputCol string) *OrderedWindowOp {
	if keyFn == nil {
		keyFn = func(types.Tuple) any { return nil }
	}
	if offset <= 0 {
		offset = 1
	}
	return &OrderedWindowOp{
		KeyFn:       keyFn,
		OrderByCol:  orderByCol,
		LagCol:      lagCol,
		Offset:      offset,
		OutputCol:   outputCol,
		LagOutputs:  []OrderedWindowLag{{LagCol: lagCol, OutputCol: outputCol}},
		profile:     newOperatorApplyProfile("OrderedWindowOp"),
		Partitions:  make(map[any]*orderedWindowPartition),
		lastTouched: make(map[any]time.Time),
	}
}

func (w *OrderedWindowOp) ensureProfiler() {
	if w.profile.label == "" {
		w.profile = newOperatorApplyProfile("OrderedWindowOp")
	}
}

func (w *OrderedWindowOp) ensureLagOutputs() {
	if w == nil {
		return
	}
	if len(w.LagOutputs) > 0 {
		for idx := range w.LagOutputs {
			if strings.TrimSpace(w.LagOutputs[idx].OutputCol) == "" {
				w.LagOutputs[idx].OutputCol = w.defaultOutputColFor(w.LagOutputs[idx].LagCol)
			}
		}
		return
	}
	w.LagOutputs = []OrderedWindowLag{{
		LagCol:    w.LagCol,
		LagExpr:   w.LagExpr,
		OutputCol: w.outputCol(),
	}}
}

func (w *OrderedWindowOp) AddLagOutput(lagCol string, lagExpr func(types.Tuple) (any, error), outputCol string) {
	w.ensureLagOutputs()
	if strings.TrimSpace(outputCol) == "" {
		outputCol = w.defaultOutputColFor(lagCol)
	}
	w.LagOutputs = append(w.LagOutputs, OrderedWindowLag{LagCol: lagCol, LagExpr: lagExpr, OutputCol: outputCol})
}

func (w *OrderedWindowOp) Snapshot() (any, error) {
	if w == nil {
		return orderedWindowSnapshotV1{}, nil
	}
	if err := w.loadBackendState(); err != nil {
		return nil, err
	}
	return orderedWindowSnapshotV1{
		Partitions:  cloneOrderedWindowPartitions(w.Partitions),
		StateTTL:    w.StateTTL,
		OnlyLastLag: w.OnlyLastLag,
		LastTouched: cloneOrderedWindowTouchMap(w.lastTouched),
	}, nil
}

func (w *OrderedWindowOp) Restore(state any) error {
	snap, ok := state.(orderedWindowSnapshotV1)
	if !ok {
		return fmt.Errorf("unexpected snapshot type %T", state)
	}
	if w.Partitions == nil {
		w.Partitions = make(map[any]*orderedWindowPartition)
	}
	w.Partitions = cloneOrderedWindowPartitions(snap.Partitions)
	w.StateTTL = snap.StateTTL
	w.OnlyLastLag = snap.OnlyLastLag
	w.lastTouched = cloneOrderedWindowTouchMap(snap.LastTouched)
	if w.lastTouched == nil {
		w.lastTouched = make(map[any]time.Time)
	}
	w.backendLoaded = true
	if err := w.flushBackendState(); err != nil {
		return err
	}
	return nil
}

func (w *OrderedWindowOp) SetStateBackend(backend StateBackend, prefix string) {
	w.stateBackend = backend
	w.statePrefix = prefix
	if w.statePrefix == "" {
		w.statePrefix = "orderedwindow/default"
	}
	w.backendLoaded = false
}

func (w *OrderedWindowOp) SetStateTTL(ttl time.Duration) {
	w.StateTTL = ttl
}

func (w *OrderedWindowOp) SetOnlyLastLag(enabled bool) {
	w.OnlyLastLag = enabled
}

func (w *OrderedWindowOp) touchPartition(now time.Time, key any) {
	if w.lastTouched == nil {
		w.lastTouched = make(map[any]time.Time)
	}
	w.lastTouched[key] = now
	if w.StateTTL > 0 {
		w.ttlExpiry.touch(stableAnyKey(key), now.Add(w.StateTTL))
	}
}

func (w *OrderedWindowOp) Apply(batch types.Batch) (types.Batch, error) {
	w.ensureProfiler()
	w.ensureLagOutputs()
	if err := w.loadBackendState(); err != nil {
		return nil, err
	}
	if w.Partitions == nil {
		w.Partitions = make(map[any]*orderedWindowPartition)
	}
	if w.lastTouched == nil {
		w.lastTouched = make(map[any]time.Time)
	}
	now := time.Now()
	shouldCheckTTL := shouldRunTTLCheck(&w.nextTTLCheck, now, w.StateTTL, w.ttlCheckInterval)
	if shouldCheckTTL {
		w.evictExpired(now)
	}
	if len(batch) == 0 {
		if err := w.flushBackendState(); err != nil {
			return nil, err
		}
		w.profile.observeBatch(0, nil, 0, 0, w.stateEntryCount())
		return nil, nil
	}

	var out types.Batch
	appendHits := 0
	appendMisses := 0
	for _, td := range batch {
		if td.Count == 0 {
			continue
		}
		partitionKey := w.partitionKeyForDelta(td)
		w.touchPartition(now, partitionKey)
		partition := w.getOrCreatePartition(partitionKey)
		if shouldCheckTTL {
			w.pruneExpiredRows(partition, now)
		}
		count := td.Count
		step := int64(1)
		if count < 0 {
			step = -1
			count = -count
		}
		for i := int64(0); i < count; i++ {
			stepOut, appended, err := w.applyUnit(partition, types.TupleDelta{Tuple: td.Tuple, Packed: td.Packed, Count: step})
			if err != nil {
				return nil, err
			}
			if w.shouldRetainOnlyLastLag() {
				partition.retainNewestRow()
			}
			if appended {
				appendHits++
			} else {
				appendMisses++
			}
			out = append(out, stepOut...)
		}
	}
	if err := w.flushBackendState(); err != nil {
		return nil, err
	}
	w.profile.observeBatch(len(batch), out, appendHits, appendMisses, w.stateEntryCount())
	return out, nil
}

func (w *OrderedWindowOp) stateEntryCount() int {
	if w == nil {
		return 0
	}
	count := 0
	for _, partition := range w.Partitions {
		if partition != nil {
			count += len(partition.Rows)
		}
	}
	return count
}

func (w *OrderedWindowOp) shouldRetainOnlyLastLag() bool {
	return w != nil && w.OnlyLastLag && w.offset() == 1
}

func (w *OrderedWindowOp) applyUnit(partition *orderedWindowPartition, td types.TupleDelta) (types.Batch, bool, error) {
	mutation, err := w.planMutation(partition, td)
	if err != nil {
		return nil, false, err
	}
	if td.Count > 0 && mutation.append {
		w.applyMutation(partition, td, mutation)
		row := partition.Rows[mutation.newPos]
		return types.Batch{w.materializeOutput(orderedWindowOutput{
			row:       row,
			lagValues: w.getLagValues(partition.Rows, mutation.newPos),
			count:     1,
		})}, true, nil
	}

	oldStart, oldEnd := oldBandRange(td.Count, mutation.oldPos, w.Offset, len(partition.Rows))
	oldMap := w.buildOutputMap(partition.Rows, oldStart, oldEnd)

	w.applyMutation(partition, td, mutation)

	newStart, newEnd := newBandRange(td.Count, mutation.newPos, w.Offset, len(partition.Rows))
	newMap := w.buildOutputMap(partition.Rows, newStart, newEnd)

	return diffOrderedOutputMaps(oldMap, newMap, w.materializeOutput), false, nil
}

func (w *OrderedWindowOp) planMutation(partition *orderedWindowPartition, td types.TupleDelta) (orderedWindowMutation, error) {
	orderValue, ok := td.Get(w.OrderByCol)
	if !ok {
		return orderedWindowMutation{}, fmt.Errorf("order by column %q not found in tuple", w.OrderByCol)
	}
	if td.Count > 0 {
		row := orderedWindowRow{
			OrderValue: orderValue,
			Tuple:      td.Tuple,
			Packed:     types.ClonePackedTuple(td.Packed),
			TieBreaker: w.tieBreakerForDelta(td),
			Seq:        partition.NextSeq,
		}
		if w.StateTTL > 0 {
			row.ExpiresAt = time.Now().Add(w.StateTTL)
		}
		if len(partition.Rows) == 0 {
			return orderedWindowMutation{oldPos: 0, newPos: 0, row: row, append: true}, nil
		}
		if compareOrderedWindowRows(partition.Rows[len(partition.Rows)-1], row) <= 0 {
			pos := len(partition.Rows)
			return orderedWindowMutation{oldPos: pos, newPos: pos, row: row, append: true}, nil
		}
		pos := findOrderedInsertPos(partition.Rows, row)
		return orderedWindowMutation{oldPos: pos, newPos: pos, row: row}, nil
	}

	oldPos := findOrderedMatchPos(partition.Rows, td, orderValue, w.tieBreakerForDelta(td))
	if oldPos < 0 {
		return orderedWindowMutation{}, fmt.Errorf("row not found for deletion")
	}
	return orderedWindowMutation{oldPos: oldPos, newPos: oldPos}, nil
}

func (w *OrderedWindowOp) applyMutation(partition *orderedWindowPartition, td types.TupleDelta, mutation orderedWindowMutation) {
	if td.Count > 0 {
		if mutation.append {
			partition.Rows = append(partition.Rows, mutation.row)
			partition.NextSeq++
			partition.observeExpiry(mutation.row.ExpiresAt)
			return
		}
		partition.Rows = append(partition.Rows, orderedWindowRow{})
		copy(partition.Rows[mutation.newPos+1:], partition.Rows[mutation.newPos:])
		partition.Rows[mutation.newPos] = mutation.row
		partition.NextSeq++
		partition.observeExpiry(mutation.row.ExpiresAt)
		return
	}
	removed := partition.Rows[mutation.oldPos]
	partition.Rows = append(partition.Rows[:mutation.oldPos], partition.Rows[mutation.oldPos+1:]...)
	if !removed.ExpiresAt.IsZero() && removed.ExpiresAt.Equal(partition.MinExpiresAt) {
		partition.recomputeMinExpiry()
	}
}

func (w *OrderedWindowOp) buildOutputMap(rows []orderedWindowRow, start int, end int) map[string]orderedWindowOutput {
	out := make(map[string]orderedWindowOutput)
	if start < 0 {
		start = 0
	}
	if end > len(rows) {
		end = len(rows)
	}
	for idx := start; idx < end; idx++ {
		row := rows[idx]
		out[orderedRowIdentity(row)] = orderedWindowOutput{
			row:       row,
			lagValues: w.getLagValues(rows, idx),
			count:     1,
		}
	}
	return out
}

func (w *OrderedWindowOp) materializeOutput(output orderedWindowOutput) types.TupleDelta {
	outputs := w.lagOutputs()
	if output.row.Packed != nil {
		packed := output.row.Packed.WithExtras(w.buildLagExtras(outputs, output.lagValues))
		return types.TupleDelta{Packed: packed, Count: output.count}
	}
	tuple := make(types.Tuple, len(output.row.Tuple)+len(outputs))
	for key, value := range output.row.Tuple {
		tuple[key] = value
	}
	for idx, cfg := range outputs {
		tuple[cfg.OutputCol] = output.lagValues[idx]
	}
	return types.TupleDelta{Tuple: tuple, Count: output.count}
}

func (w *OrderedWindowOp) buildLagExtras(outputs []OrderedWindowLag, lagValues []any) types.Tuple {
	if len(outputs) == 0 {
		return nil
	}
	extras := make(types.Tuple, len(outputs))
	for idx, cfg := range outputs {
		extras[cfg.OutputCol] = lagValues[idx]
	}
	return extras
}

func (w *OrderedWindowOp) getLagValues(rows []orderedWindowRow, idx int) []any {
	outputs := w.lagOutputs()
	values := make([]any, len(outputs))
	for outputIdx, cfg := range outputs {
		values[outputIdx] = w.getLagValueFor(rows, idx, cfg)
	}
	return values
}

func (w *OrderedWindowOp) getLagValueFor(rows []orderedWindowRow, idx int, cfg OrderedWindowLag) any {
	lagIdx := idx - w.offset()
	if lagIdx < 0 || lagIdx >= len(rows) {
		return nil
	}
	if cfg.LagExpr != nil {
		value, _ := cfg.LagExpr(rows[lagIdx].materializeTuple())
		return value
	}
	value, _ := rows[lagIdx].get(cfg.LagCol)
	return value
}

func (w *OrderedWindowOp) getOrCreatePartition(key any) *orderedWindowPartition {
	if w.Partitions == nil {
		w.Partitions = make(map[any]*orderedWindowPartition)
	}
	partition, ok := w.Partitions[key]
	if !ok {
		partition = &orderedWindowPartition{}
		w.Partitions[key] = partition
	}
	return partition
}

func (w *OrderedWindowOp) evictExpired(now time.Time) {
	if w.StateTTL <= 0 {
		return
	}
	_ = w.ttlExpiry.popExpired(now, func(id string) error {
		key, err := decodeAnyKey(id)
		if err != nil {
			key = id
		}
		delete(w.lastTouched, key)
		delete(w.Partitions, key)
		return nil
	})
}

func (w *OrderedWindowOp) pruneExpiredRows(partition *orderedWindowPartition, now time.Time) {
	if w.StateTTL <= 0 || partition == nil || len(partition.Rows) == 0 {
		return
	}
	if partition.MinExpiresAt.IsZero() || now.Before(partition.MinExpiresAt) {
		return
	}
	keep := partition.Rows[:0]
	var nextMin time.Time
	for _, row := range partition.Rows {
		if !row.ExpiresAt.IsZero() && !now.Before(row.ExpiresAt) {
			continue
		}
		if !row.ExpiresAt.IsZero() && (nextMin.IsZero() || row.ExpiresAt.Before(nextMin)) {
			nextMin = row.ExpiresAt
		}
		keep = append(keep, row)
	}
	partition.Rows = keep
	partition.MinExpiresAt = nextMin
}

func (p *orderedWindowPartition) retainNewestRow() {
	if p == nil || len(p.Rows) <= 1 {
		return
	}
	newest := p.Rows[len(p.Rows)-1]
	p.Rows = []orderedWindowRow{newest}
	p.MinExpiresAt = newest.ExpiresAt
	if p.MinExpiresAt.IsZero() {
		p.recomputeMinExpiry()
	}
}

func (w *OrderedWindowOp) backendEnabled() bool {
	return w != nil && w.stateBackend != nil
}

func (w *OrderedWindowOp) backendSnapshotKey() []byte {
	return []byte(fmt.Sprintf("%s/snapshot", w.statePrefix))
}

func (w *OrderedWindowOp) loadBackendState() error {
	if !w.backendEnabled() || w.backendLoaded {
		return nil
	}
	payload, ok, err := w.stateBackend.Get(w.backendSnapshotKey())
	if err != nil {
		return err
	}
	if !ok {
		if w.Partitions == nil {
			w.Partitions = make(map[any]*orderedWindowPartition)
		}
		if w.lastTouched == nil {
			w.lastTouched = make(map[any]time.Time)
		}
		w.backendLoaded = true
		return nil
	}
	var snap orderedWindowSnapshotV1
	if err := decodeGobValue(payload, &snap); err != nil {
		return err
	}
	w.Partitions = cloneOrderedWindowPartitions(snap.Partitions)
	w.StateTTL = snap.StateTTL
	w.lastTouched = cloneOrderedWindowTouchMap(snap.LastTouched)
	if w.Partitions == nil {
		w.Partitions = make(map[any]*orderedWindowPartition)
	}
	if w.lastTouched == nil {
		w.lastTouched = make(map[any]time.Time)
	}
	w.backendLoaded = true
	return nil
}

func (w *OrderedWindowOp) flushBackendState() error {
	if !w.backendEnabled() {
		return nil
	}
	snap := orderedWindowSnapshotV1{
		Partitions:  cloneOrderedWindowPartitions(w.Partitions),
		StateTTL:    w.StateTTL,
		LastTouched: cloneOrderedWindowTouchMap(w.lastTouched),
	}
	payload, err := encodeGobValue(snap)
	if err != nil {
		return err
	}
	return w.stateBackend.Put(w.backendSnapshotKey(), payload)
}

func (w *OrderedWindowOp) outputCol() string {
	if w.OutputCol != "" {
		return w.OutputCol
	}
	return w.defaultOutputColFor(w.LagCol)
}

func (w *OrderedWindowOp) defaultOutputColFor(lagCol string) string {
	return "lag_" + lagCol
}

func (w *OrderedWindowOp) lagOutputs() []OrderedWindowLag {
	w.ensureLagOutputs()
	return w.LagOutputs
}

func (w *OrderedWindowOp) offset() int {
	if w.Offset <= 0 {
		return 1
	}
	return w.Offset
}

func cloneOrderedWindowPartitions(src map[any]*orderedWindowPartition) map[any]*orderedWindowPartition {
	if len(src) == 0 {
		return make(map[any]*orderedWindowPartition)
	}
	out := make(map[any]*orderedWindowPartition, len(src))
	for key, partition := range src {
		if partition == nil {
			out[key] = &orderedWindowPartition{}
			continue
		}
		out[key] = &orderedWindowPartition{Rows: cloneOrderedRows(partition.Rows), NextSeq: partition.NextSeq, MinExpiresAt: partition.MinExpiresAt}
	}
	return out
}

func (p *orderedWindowPartition) observeExpiry(expiresAt time.Time) {
	if expiresAt.IsZero() {
		return
	}
	if p.MinExpiresAt.IsZero() || expiresAt.Before(p.MinExpiresAt) {
		p.MinExpiresAt = expiresAt
	}
}

func (p *orderedWindowPartition) recomputeMinExpiry() {
	var nextMin time.Time
	for _, row := range p.Rows {
		if row.ExpiresAt.IsZero() {
			continue
		}
		if nextMin.IsZero() || row.ExpiresAt.Before(nextMin) {
			nextMin = row.ExpiresAt
		}
	}
	p.MinExpiresAt = nextMin
}

func cloneOrderedWindowTouchMap(src map[any]time.Time) map[any]time.Time {
	if len(src) == 0 {
		return make(map[any]time.Time)
	}
	out := make(map[any]time.Time, len(src))
	for key, value := range src {
		out[key] = value
	}
	return out
}

func cloneOrderedRows(rows []orderedWindowRow) []orderedWindowRow {
	if len(rows) == 0 {
		return nil
	}
	out := make([]orderedWindowRow, len(rows))
	for i, row := range rows {
		out[i] = orderedWindowRow{
			OrderValue: row.OrderValue,
			Tuple:      types.CloneTuple(row.Tuple),
			Packed:     types.ClonePackedTuple(row.Packed),
			TieBreaker: row.TieBreaker,
			Seq:        row.Seq,
			ExpiresAt:  row.ExpiresAt,
		}
	}
	return out
}

func findOrderedInsertPos(rows []orderedWindowRow, row orderedWindowRow) int {
	return sort.Search(len(rows), func(i int) bool {
		return compareOrderedWindowRows(rows[i], row) >= 0
	})
}

func findOrderedMatchPos(rows []orderedWindowRow, td types.TupleDelta, orderValue any, tieBreaker string) int {
	for i, row := range rows {
		if row.TieBreaker == tieBreaker && types.EqualAny(row.OrderValue, orderValue) {
			return i
		}
	}
	for i, row := range rows {
		if row.matchesDelta(td) {
			return i
		}
	}
	return -1
}

func compareOrderedWindowRows(a, b orderedWindowRow) int {
	if cmp := compareValues(a.OrderValue, b.OrderValue); cmp != 0 {
		return cmp
	}
	if a.TieBreaker < b.TieBreaker {
		return -1
	}
	if a.TieBreaker > b.TieBreaker {
		return 1
	}
	if a.Seq < b.Seq {
		return -1
	}
	if a.Seq > b.Seq {
		return 1
	}
	return 0
}

func orderedRowIdentity(row orderedWindowRow) string {
	return fmt.Sprintf("%d:%s", row.Seq, row.TieBreaker)
}

func (w *OrderedWindowOp) partitionKeyForDelta(td types.TupleDelta) any {
	if td.Tuple != nil {
		return w.KeyFn(td.Tuple)
	}
	switch len(w.PartitionCols) {
	case 0:
		return nil
	case 1:
		value, _ := td.Get(w.PartitionCols[0])
		return value
	default:
		parts := make([]any, len(w.PartitionCols))
		for idx, col := range w.PartitionCols {
			parts[idx], _ = td.Get(col)
		}
		return fmt.Sprintf("%v", parts)
	}
}

func (w *OrderedWindowOp) tieBreakerForDelta(td types.TupleDelta) string {
	if td.Packed != nil {
		return compactAnyOrderKey(td.Packed)
	}
	return compactAnyOrderKey(td.Tuple)
}

func (r orderedWindowRow) get(col string) (any, bool) {
	if r.Tuple != nil {
		value, ok := r.Tuple[col]
		return value, ok
	}
	if r.Packed != nil {
		return r.Packed.Get(col)
	}
	return nil, false
}

func (r orderedWindowRow) materializeTuple() types.Tuple {
	if r.Tuple != nil {
		return r.Tuple
	}
	if r.Packed != nil {
		return r.Packed.Materialize()
	}
	return nil
}

func (r orderedWindowRow) matchesDelta(td types.TupleDelta) bool {
	if packedRowsEqualIgnoringExtras(r.Packed, td.Packed) {
		return true
	}
	left := r.materializeTuple()
	right := td.EnsureTuple()
	return types.TuplesEqual(left, right)
}

func packedRowsEqualIgnoringExtras(left, right *types.PackedTuple) bool {
	if left == nil || right == nil {
		return false
	}
	if left.Schema == nil || right.Schema == nil {
		return false
	}
	if len(left.Schema.Columns) != len(right.Schema.Columns) {
		return false
	}
	for idx, col := range left.Schema.Columns {
		if right.Schema.Columns[idx] != col {
			return false
		}
		leftPresent := idx < len(left.Present) && left.Present[idx]
		rightPresent := idx < len(right.Present) && right.Present[idx]
		if len(left.Present) == 0 {
			leftPresent = idx < len(left.Values)
		}
		if len(right.Present) == 0 {
			rightPresent = idx < len(right.Values)
		}
		if leftPresent != rightPresent {
			return false
		}
		if !leftPresent {
			continue
		}
		if idx >= len(left.Values) || idx >= len(right.Values) {
			return false
		}
		if !types.EqualAny(left.Values[idx], right.Values[idx]) {
			return false
		}
	}
	return true
}

func oldBandRange(deltaCount int64, pos int, offset int, length int) (int, int) {
	if pos < 0 {
		return 0, 0
	}
	if deltaCount > 0 {
		end := pos + offset
		if end > length {
			end = length
		}
		return pos, end
	}
	end := pos + offset + 1
	if end > length {
		end = length
	}
	return pos, end
}

func newBandRange(deltaCount int64, pos int, offset int, length int) (int, int) {
	if pos < 0 {
		return 0, 0
	}
	if deltaCount > 0 {
		end := pos + offset + 1
		if end > length {
			end = length
		}
		return pos, end
	}
	end := pos + offset
	if end > length {
		end = length
	}
	return pos, end
}

func diffOrderedOutputMaps(oldMap, newMap map[string]orderedWindowOutput, materialize func(orderedWindowOutput) types.TupleDelta) types.Batch {
	var out types.Batch
	for key, oldOutput := range oldMap {
		newOutput, ok := newMap[key]
		if !ok {
			oldTd := materialize(oldOutput)
			out = append(out, types.TupleDelta{Tuple: oldTd.Tuple, Packed: oldTd.Packed, Count: -oldTd.Count})
			continue
		}
		if !types.EqualAny(oldOutput.lagValues, newOutput.lagValues) {
			oldTd := materialize(oldOutput)
			newTd := materialize(newOutput)
			out = append(out, types.TupleDelta{Tuple: oldTd.Tuple, Packed: oldTd.Packed, Count: -oldTd.Count})
			out = append(out, newTd)
			continue
		}
		if newOutput.count != oldOutput.count {
			diff := newOutput.count - oldOutput.count
			if diff != 0 {
				newTd := materialize(newOutput)
				out = append(out, types.TupleDelta{Tuple: newTd.Tuple, Count: diff})
			}
		}
	}
	for key, newOutput := range newMap {
		if _, ok := oldMap[key]; !ok {
			newTd := materialize(newOutput)
			out = append(out, newTd)
		}
	}
	return out
}
