package op

import (
	"testing"

	"github.com/ariyn/dbsp/internal/dbsp/types"
)

func TestBoundedOutOfOrdernessWatermark(t *testing.T) {
	// Allow 5 seconds of out-of-orderness
	wm := NewBoundedOutOfOrdernessWatermark(5000)

	// Process events
	wm.Update(10000) // 10s
	if wm.GetWatermark() != 5000 {
		t.Errorf("expected watermark=5000, got %d", wm.GetWatermark())
	}

	wm.Update(15000) // 15s
	if wm.GetWatermark() != 10000 {
		t.Errorf("expected watermark=10000, got %d", wm.GetWatermark())
	}

	// Out of order event (doesn't advance watermark)
	wm.Update(12000) // 12s
	if wm.GetWatermark() != 10000 {
		t.Errorf("expected watermark=10000 (unchanged), got %d", wm.GetWatermark())
	}
}

func TestLateEventBuffer(t *testing.T) {
	buffer := NewLateEventBuffer(3, 5000)

	// Add events
	td1 := types.TupleDelta{Tuple: types.Tuple{"ts": int64(1000)}, Count: 1}
	td2 := types.TupleDelta{Tuple: types.Tuple{"ts": int64(2000)}, Count: 1}
	td3 := types.TupleDelta{Tuple: types.Tuple{"ts": int64(3000)}, Count: 1}
	td4 := types.TupleDelta{Tuple: types.Tuple{"ts": int64(4000)}, Count: 1}

	if !buffer.Add(td1) {
		t.Error("failed to add event 1")
	}
	if !buffer.Add(td2) {
		t.Error("failed to add event 2")
	}
	if !buffer.Add(td3) {
		t.Error("failed to add event 3")
	}

	// Buffer should be full
	if buffer.Add(td4) {
		t.Error("should not add event 4 (buffer full)")
	}

	if buffer.Len() != 3 {
		t.Errorf("expected buffer len=3, got %d", buffer.Len())
	}

	// Get and clear
	events := buffer.GetAndClear()
	if len(events) != 3 {
		t.Errorf("expected 3 events, got %d", len(events))
	}

	if buffer.Len() != 0 {
		t.Errorf("expected empty buffer after clear, got %d", buffer.Len())
	}
}

func TestWatermarkAwareWindowOp_DropLateEvents(t *testing.T) {
	spec := WindowSpecLite{
		TimeCol:     "ts",
		SizeMillis:  10000, // 10 seconds
		WindowType:  WindowTypeTumbling,
	}

	config := WatermarkConfig{
		Enabled:           true,
		MaxOutOfOrderness: 2000, // 2 seconds
		AllowedLateness:   1000, // 1 second
		Policy:            DropLateEvents,
	}

	keyFn := func(t types.Tuple) any { return nil }
	aggInit := func() any { return int64(0) }
	agg := &CountAgg{}

	op := NewWatermarkAwareWindowOp(spec, keyFn, []string{}, aggInit, agg, config)

	// Process events
	batch := types.Batch{
		{Tuple: types.Tuple{"ts": int64(5000)}, Count: 1},  // On-time
		{Tuple: types.Tuple{"ts": int64(8000)}, Count: 1},  // On-time
		{Tuple: types.Tuple{"ts": int64(2000)}, Count: 1},  // Late (< watermark)
		{Tuple: types.Tuple{"ts": int64(10000)}, Count: 1}, // On-time
	}

	out, err := op.Apply(batch)
	if err != nil {
		t.Fatalf("Apply failed: %v", err)
	}

	// Late event should be dropped
	// Should process 3 on-time events
	t.Logf("Output: %d deltas", len(out))
	for i, td := range out {
		t.Logf("  [%d] %+v", i, td.Tuple)
	}

	// Watermark should be maxTimestamp - maxOutOfOrderness = 10000 - 2000 = 8000
	watermark := op.GetCurrentWatermark()
	if watermark != 8000 {
		t.Errorf("expected watermark=8000, got %d", watermark)
	}
}

func TestWatermarkAwareWindowOp_EmitLateEvents(t *testing.T) {
	spec := WindowSpecLite{
		TimeCol:     "ts",
		SizeMillis:  10000,
		WindowType:  WindowTypeTumbling,
	}

	config := WatermarkConfig{
		Enabled:           true,
		MaxOutOfOrderness: 3000,
		AllowedLateness:   2000,
		Policy:            EmitLateEvents,
	}

	keyFn := func(t types.Tuple) any { return nil }
	aggInit := func() any { return int64(0) }
	agg := &CountAgg{}

	op := NewWatermarkAwareWindowOp(spec, keyFn, []string{}, aggInit, agg, config)

	// First batch to establish watermark
	batch1 := types.Batch{
		{Tuple: types.Tuple{"ts": int64(10000)}, Count: 1},
	}
	out1, err := op.Apply(batch1)
	if err != nil {
		t.Fatalf("Apply batch1 failed: %v", err)
	}

	t.Logf("Batch 1 output: %d deltas", len(out1))

	// Second batch with late event
	batch2 := types.Batch{
		{Tuple: types.Tuple{"ts": int64(5000)}, Count: 1}, // Late event
		{Tuple: types.Tuple{"ts": int64(12000)}, Count: 1}, // On-time
	}

	out2, err := op.Apply(batch2)
	if err != nil {
		t.Fatalf("Apply batch2 failed: %v", err)
	}

	t.Logf("Batch 2 output: %d deltas", len(out2))
	
	// Check for __late marker
	foundLate := false
	for _, td := range out2 {
		if late, ok := td.Tuple["__late"].(bool); ok && late {
			foundLate = true
			t.Logf("Found late event: %+v", td.Tuple)
		}
	}

	if !foundLate {
		t.Error("expected to find late event with __late marker")
	}
}

func TestWatermarkAwareWindowOp_BufferLateEvents(t *testing.T) {
	spec := WindowSpecLite{
		TimeCol:     "ts",
		SizeMillis:  10000,
		WindowType:  WindowTypeTumbling,
	}

	config := WatermarkConfig{
		Enabled:           true,
		MaxOutOfOrderness: 2000,
		AllowedLateness:   3000,
		Policy:            BufferLateEvents,
		MaxBufferSize:     10,
	}

	keyFn := func(t types.Tuple) any { return nil }
	aggInit := func() any { return int64(0) }
	agg := &CountAgg{}

	op := NewWatermarkAwareWindowOp(spec, keyFn, []string{}, aggInit, agg, config)

	// Establish watermark
	batch1 := types.Batch{
		{Tuple: types.Tuple{"ts": int64(10000)}, Count: 1},
	}
	_, err := op.Apply(batch1)
	if err != nil {
		t.Fatalf("Apply failed: %v", err)
	}

	// Send late events
	batch2 := types.Batch{
		{Tuple: types.Tuple{"ts": int64(5000)}, Count: 1}, // Late
		{Tuple: types.Tuple{"ts": int64(6000)}, Count: 1}, // Late
	}

	out2, err := op.Apply(batch2)
	if err != nil {
		t.Fatalf("Apply failed: %v", err)
	}

	// Late events should be buffered, not in output
	t.Logf("Output after late events: %d deltas", len(out2))

	// Check buffer has late events
	if op.lateBuffer.Len() != 2 {
		t.Errorf("expected 2 buffered events, got %d", op.lateBuffer.Len())
	}

	// Process buffered events
	bufferedOut, err := op.ProcessBufferedLateEvents()
	if err != nil {
		t.Fatalf("ProcessBufferedLateEvents failed: %v", err)
	}

	t.Logf("Buffered events output: %d deltas", len(bufferedOut))

	// Buffer should be empty now
	if op.lateBuffer.Len() != 0 {
		t.Errorf("expected empty buffer after processing, got %d", op.lateBuffer.Len())
	}
}

func TestWatermarkAwareWindowOp_WindowCleanup(t *testing.T) {
	spec := WindowSpecLite{
		TimeCol:     "ts",
		SizeMillis:  10000,
		WindowType:  WindowTypeTumbling,
	}

	config := WatermarkConfig{
		Enabled:           true,
		MaxOutOfOrderness: 1000,
		AllowedLateness:   2000,
		Policy:            DropLateEvents,
	}

	keyFn := func(t types.Tuple) any { return nil }
	aggInit := func() any { return int64(0) }
	agg := &CountAgg{}

	op := NewWatermarkAwareWindowOp(spec, keyFn, []string{}, aggInit, agg, config)

	// Create windows at different times
	batches := []types.Batch{
		{{Tuple: types.Tuple{"ts": int64(5000)}, Count: 1}},   // Window [0, 10000)
		{{Tuple: types.Tuple{"ts": int64(15000)}, Count: 1}},  // Window [10000, 20000)
		{{Tuple: types.Tuple{"ts": int64(25000)}, Count: 1}},  // Window [20000, 30000)
		{{Tuple: types.Tuple{"ts": int64(35000)}, Count: 1}},  // Window [30000, 40000)
	}

	for i, batch := range batches {
		_, err := op.Apply(batch)
		if err != nil {
			t.Fatalf("Apply batch %d failed: %v", i, err)
		}
	}

	// Check how many windows remain in state
	// Watermark after last event: 35000 - 1000 = 34000
	// Cleanup cutoff: 34000 - 2000 = 32000
	// Windows with end < 32000 should be cleaned up
	// That means windows [0, 10000), [10000, 20000), [20000, 30000) should be removed
	
	t.Logf("Remaining windows in state: %d", len(op.State.Data))
	
	// At least the oldest window should be cleaned up
	if len(op.State.Data) >= 4 {
		t.Errorf("expected some windows to be cleaned up, but found %d windows", len(op.State.Data))
	}
}
