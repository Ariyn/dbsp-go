package op

import (
	"fmt"
	"sort"
	"time"

	"github.com/ariyn/dbsp/internal/dbsp/types"
)

type orderedWindowSnapshotV1 struct {
	Partitions  map[any]*orderedWindowPartition
	StateTTL    time.Duration
	LastTouched map[any]time.Time
}

type orderedWindowPartition struct {
	Rows    []orderedWindowRow
	NextSeq int64
}

type orderedWindowRow struct {
	OrderValue any
	Tuple      types.Tuple
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
	row      orderedWindowRow
	lagValue any
	count    int64
}

type OrderedWindowOp struct {
	KeyFn      func(types.Tuple) any
	OrderByCol string
	LagCol     string
	LagExpr    func(types.Tuple) (any, error)
	Offset     int
	OutputCol  string
	profile    operatorApplyProfile

	Partitions    map[any]*orderedWindowPartition
	StateTTL      time.Duration
	lastTouched   map[any]time.Time
	stateBackend  StateBackend
	statePrefix   string
	backendLoaded bool
}

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

func (w *OrderedWindowOp) Apply(batch types.Batch) (types.Batch, error) {
	w.ensureProfiler()
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
	w.evictExpired(now)
	if len(batch) == 0 {
		if err := w.flushBackendState(); err != nil {
			return nil, err
		}
		w.profile.observeBatch(0, nil, 0, 0)
		return nil, nil
	}

	var out types.Batch
	appendHits := 0
	appendMisses := 0
	for _, td := range batch {
		if td.Count == 0 {
			continue
		}
		partitionKey := w.KeyFn(td.Tuple)
		w.lastTouched[partitionKey] = now
		partition := w.getOrCreatePartition(partitionKey)
		w.pruneExpiredRows(partition, now)
		count := td.Count
		step := int64(1)
		if count < 0 {
			step = -1
			count = -count
		}
		for i := int64(0); i < count; i++ {
			stepOut, appended, err := w.applyUnit(partition, types.TupleDelta{Tuple: td.Tuple, Count: step})
			if err != nil {
				return nil, err
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
	w.profile.observeBatch(len(batch), out, appendHits, appendMisses)
	return out, nil
}

func (w *OrderedWindowOp) applyUnit(partition *orderedWindowPartition, td types.TupleDelta) (types.Batch, bool, error) {
	mutation, err := w.planMutation(partition, td)
	if err != nil {
		return nil, false, err
	}
	if td.Count > 0 && mutation.append {
		w.applyMutation(partition, td, mutation)
		row := partition.Rows[mutation.newPos]
		outTuple := types.CloneTuple(row.Tuple)
		if outTuple == nil {
			outTuple = types.Tuple{}
		}
		outTuple[w.outputCol()] = w.getLagValue(partition.Rows, mutation.newPos)
		return types.Batch{{Tuple: outTuple, Count: 1}}, true, nil
	}

	oldStart, oldEnd := oldBandRange(td.Count, mutation.oldPos, w.Offset, len(partition.Rows))
	oldMap := w.buildOutputMap(partition.Rows, oldStart, oldEnd)

	w.applyMutation(partition, td, mutation)

	newStart, newEnd := newBandRange(td.Count, mutation.newPos, w.Offset, len(partition.Rows))
	newMap := w.buildOutputMap(partition.Rows, newStart, newEnd)

	return diffOrderedOutputMaps(oldMap, newMap, w.materializeOutput), false, nil
}

func (w *OrderedWindowOp) planMutation(partition *orderedWindowPartition, td types.TupleDelta) (orderedWindowMutation, error) {
	orderValue, ok := td.Tuple[w.OrderByCol]
	if !ok {
		return orderedWindowMutation{}, fmt.Errorf("order by column %q not found in tuple", w.OrderByCol)
	}
	if td.Count > 0 {
		row := orderedWindowRow{
			OrderValue: orderValue,
			Tuple:      td.Tuple,
			TieBreaker: stableAnyKey(td.Tuple),
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

	oldPos := findOrderedMatchPos(partition.Rows, td.Tuple)
	if oldPos < 0 {
		return orderedWindowMutation{}, fmt.Errorf("row not found for deletion: %v", td.Tuple)
	}
	return orderedWindowMutation{oldPos: oldPos, newPos: oldPos}, nil
}

func (w *OrderedWindowOp) applyMutation(partition *orderedWindowPartition, td types.TupleDelta, mutation orderedWindowMutation) {
	if td.Count > 0 {
		if mutation.append {
			partition.Rows = append(partition.Rows, mutation.row)
			partition.NextSeq++
			return
		}
		partition.Rows = append(partition.Rows, orderedWindowRow{})
		copy(partition.Rows[mutation.newPos+1:], partition.Rows[mutation.newPos:])
		partition.Rows[mutation.newPos] = mutation.row
		partition.NextSeq++
		return
	}
	partition.Rows = append(partition.Rows[:mutation.oldPos], partition.Rows[mutation.oldPos+1:]...)
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
			row:      row,
			lagValue: w.getLagValue(rows, idx),
			count:    1,
		}
	}
	return out
}

func (w *OrderedWindowOp) materializeOutput(output orderedWindowOutput) types.TupleDelta {
	tuple := make(types.Tuple, len(output.row.Tuple)+1)
	for key, value := range output.row.Tuple {
		tuple[key] = value
	}
	tuple[w.outputCol()] = output.lagValue
	return types.TupleDelta{Tuple: tuple, Count: output.count}
}

func (w *OrderedWindowOp) getLagValue(rows []orderedWindowRow, idx int) any {
	lagIdx := idx - w.offset()
	if lagIdx < 0 || lagIdx >= len(rows) {
		return nil
	}
	if w.LagExpr != nil {
		value, _ := w.LagExpr(rows[lagIdx].Tuple)
		return value
	}
	return rows[lagIdx].Tuple[w.LagCol]
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
	if w.StateTTL <= 0 || len(w.lastTouched) == 0 {
		return
	}
	for key, touched := range w.lastTouched {
		if now.Sub(touched) <= w.StateTTL {
			continue
		}
		delete(w.lastTouched, key)
		delete(w.Partitions, key)
	}
}

func (w *OrderedWindowOp) pruneExpiredRows(partition *orderedWindowPartition, now time.Time) {
	if w.StateTTL <= 0 || partition == nil || len(partition.Rows) == 0 {
		return
	}
	keep := partition.Rows[:0]
	for _, row := range partition.Rows {
		if !row.ExpiresAt.IsZero() && !now.Before(row.ExpiresAt) {
			continue
		}
		keep = append(keep, row)
	}
	partition.Rows = keep
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
	return "lag_" + w.LagCol
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
		out[key] = &orderedWindowPartition{Rows: cloneOrderedRows(partition.Rows), NextSeq: partition.NextSeq}
	}
	return out
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

func findOrderedMatchPos(rows []orderedWindowRow, tuple types.Tuple) int {
	for i, row := range rows {
		if types.TuplesEqual(row.Tuple, tuple) {
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
			out = append(out, types.TupleDelta{Tuple: oldTd.Tuple, Count: -oldTd.Count})
			continue
		}
		if !types.EqualAny(oldOutput.lagValue, newOutput.lagValue) {
			oldTd := materialize(oldOutput)
			newTd := materialize(newOutput)
			out = append(out, types.TupleDelta{Tuple: oldTd.Tuple, Count: -oldTd.Count})
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
