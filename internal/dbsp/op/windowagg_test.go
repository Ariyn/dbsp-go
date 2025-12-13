package op

import (
	"testing"

	"github.com/ariyn/dbsp/internal/dbsp/types"
)

func TestWindowAggOp_RowsFrame(t *testing.T) {
	// Test ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
	frameSpec := &FrameSpecLite{
		Type:       "ROWS",
		StartType:  "1 PRECEDING",
		StartValue: "1",
		EndType:    "CURRENT ROW",
		EndValue:   "",
	}

	keyFn := func(t types.Tuple) any { return t["region"] }
	aggInit := func() any { return int64(0) }
	agg := &CountAgg{}

	op := NewWindowAggOp(WindowSpecLite{}, keyFn, []string{"region"}, aggInit, agg)
	op.OrderByCol = "ts"
	op.FrameSpec = frameSpec

	// Input batch
	batch := types.Batch{
		{Tuple: types.Tuple{"region": "East", "ts": int64(1), "value": 10}, Count: 1},
		{Tuple: types.Tuple{"region": "East", "ts": int64(2), "value": 20}, Count: 1},
		{Tuple: types.Tuple{"region": "East", "ts": int64(3), "value": 30}, Count: 1},
	}

	out, err := op.Apply(batch)
	if err != nil {
		t.Fatalf("Apply failed: %v", err)
	}

	if len(out) == 0 {
		t.Fatal("expected non-empty output")
	}

	t.Logf("Output batch:")
	for i, td := range out {
		t.Logf("  [%d] count=%d, tuple=%#v", i, td.Count, td.Tuple)
	}

	// First row: only current row in frame (count=1)
	// Second row: 2 rows in frame (count=2)
	// Third row: 2 rows in frame (count=2)
	if len(out) != 3 {
		t.Errorf("expected 3 output rows, got %d", len(out))
	}
}

func TestWindowAggOp_RangeFrame(t *testing.T) {
	// Test RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
	frameSpec := &FrameSpecLite{
		Type:      "RANGE",
		StartType: "UNBOUNDED PRECEDING",
		EndType:   "CURRENT ROW",
	}

	keyFn := func(t types.Tuple) any { return t["user"] }
	aggInit := func() any { return float64(0) }
	agg := &SumAgg{ColName: "amount"}

	op := NewWindowAggOp(WindowSpecLite{}, keyFn, []string{"user"}, aggInit, agg)
	op.OrderByCol = "ts"
	op.FrameSpec = frameSpec

	// Input batch
	batch := types.Batch{
		{Tuple: types.Tuple{"user": "Alice", "ts": int64(1), "amount": 100.0}, Count: 1},
		{Tuple: types.Tuple{"user": "Alice", "ts": int64(2), "amount": 50.0}, Count: 1},
		{Tuple: types.Tuple{"user": "Alice", "ts": int64(3), "amount": 75.0}, Count: 1},
	}

	out, err := op.Apply(batch)
	if err != nil {
		t.Fatalf("Apply failed: %v", err)
	}

	if len(out) == 0 {
		t.Fatal("expected non-empty output")
	}

	t.Logf("Output batch:")
	for i, td := range out {
		t.Logf("  [%d] count=%d, tuple=%#v", i, td.Count, td.Tuple)
	}

	// Running sum: 100, 150, 225
	if len(out) != 3 {
		t.Errorf("expected 3 output rows, got %d", len(out))
	}
}

func TestWindowAggOp_MultiplePartitions(t *testing.T) {
	// Test with multiple partitions
	frameSpec := &FrameSpecLite{
		Type:       "ROWS",
		StartType:  "UNBOUNDED PRECEDING",
		StartValue: "",
		EndType:    "CURRENT ROW",
		EndValue:   "",
	}

	keyFn := func(t types.Tuple) any { return t["category"] }
	aggInit := func() any { return float64(0) }
	agg := &SumAgg{ColName: "sales"}

	op := NewWindowAggOp(WindowSpecLite{}, keyFn, []string{"category"}, aggInit, agg)
	op.OrderByCol = "date"
	op.FrameSpec = frameSpec

	// Input batch with two partitions
	batch := types.Batch{
		{Tuple: types.Tuple{"category": "A", "date": int64(1), "sales": 100.0}, Count: 1},
		{Tuple: types.Tuple{"category": "A", "date": int64(2), "sales": 200.0}, Count: 1},
		{Tuple: types.Tuple{"category": "B", "date": int64(1), "sales": 50.0}, Count: 1},
		{Tuple: types.Tuple{"category": "B", "date": int64(2), "sales": 75.0}, Count: 1},
	}

	out, err := op.Apply(batch)
	if err != nil {
		t.Fatalf("Apply failed: %v", err)
	}

	if len(out) == 0 {
		t.Fatal("expected non-empty output")
	}

	t.Logf("Output batch:")
	for i, td := range out {
		t.Logf("  [%d] count=%d, tuple=%#v", i, td.Count, td.Tuple)
	}

	// Should have 4 rows (2 per partition)
	if len(out) != 4 {
		t.Errorf("expected 4 output rows, got %d", len(out))
	}
}

func TestWindowAggOp_AvgWithFrame(t *testing.T) {
	// Test AVG with frame
	frameSpec := &FrameSpecLite{
		Type:       "ROWS",
		StartType:  "1 PRECEDING",
		StartValue: "1",
		EndType:    "1 FOLLOWING",
		EndValue:   "1",
	}

	keyFn := func(t types.Tuple) any { return nil } // No partition
	aggInit := func() any { return AvgMonoid{} }
	agg := &AvgAgg{ColName: "value"}

	op := NewWindowAggOp(WindowSpecLite{}, keyFn, []string{}, aggInit, agg)
	op.OrderByCol = "id"
	op.FrameSpec = frameSpec

	// Input batch
	batch := types.Batch{
		{Tuple: types.Tuple{"id": int64(1), "value": 10.0}, Count: 1},
		{Tuple: types.Tuple{"id": int64(2), "value": 20.0}, Count: 1},
		{Tuple: types.Tuple{"id": int64(3), "value": 30.0}, Count: 1},
		{Tuple: types.Tuple{"id": int64(4), "value": 40.0}, Count: 1},
	}

	out, err := op.Apply(batch)
	if err != nil {
		t.Fatalf("Apply failed: %v", err)
	}

	if len(out) == 0 {
		t.Fatal("expected non-empty output")
	}

	t.Logf("Output batch:")
	for i, td := range out {
		t.Logf("  [%d] count=%d, tuple=%#v", i, td.Count, td.Tuple)
	}

	// Moving average with 3-row window
	// Row 1: avg(10, 20) = 15
	// Row 2: avg(10, 20, 30) = 20
	// Row 3: avg(20, 30, 40) = 30
	// Row 4: avg(30, 40) = 35
}

func TestWindowAggOp_CountWithDeletions(t *testing.T) {
	// Test COUNT with deletions
	frameSpec := &FrameSpecLite{
		Type:      "ROWS",
		StartType: "UNBOUNDED PRECEDING",
		EndType:   "CURRENT ROW",
	}

	keyFn := func(t types.Tuple) any { return nil }
	aggInit := func() any { return int64(0) }
	agg := &CountAgg{}

	op := NewWindowAggOp(WindowSpecLite{}, keyFn, []string{}, aggInit, agg)
	op.OrderByCol = "ts"
	op.FrameSpec = frameSpec

	// Insert batch
	batch1 := types.Batch{
		{Tuple: types.Tuple{"ts": int64(1), "value": 10}, Count: 1},
		{Tuple: types.Tuple{"ts": int64(2), "value": 20}, Count: 1},
		{Tuple: types.Tuple{"ts": int64(3), "value": 30}, Count: 1},
	}

	out1, err := op.Apply(batch1)
	if err != nil {
		t.Fatalf("Apply batch1 failed: %v", err)
	}

	t.Logf("After insert - Output batch:")
	for i, td := range out1 {
		t.Logf("  [%d] count=%d, tuple=%#v", i, td.Count, td.Tuple)
	}

	// Delete batch
	batch2 := types.Batch{
		{Tuple: types.Tuple{"ts": int64(2), "value": 20}, Count: -1},
	}

	out2, err := op.Apply(batch2)
	if err != nil {
		t.Fatalf("Apply batch2 failed: %v", err)
	}

	t.Logf("After delete - Output batch:")
	for i, td := range out2 {
		t.Logf("  [%d] count=%d, tuple=%#v", i, td.Count, td.Tuple)
	}
}

func TestWindowAggOp_MinMaxWithFrame(t *testing.T) {
	// Test MIN/MAX with frame
	frameSpec := &FrameSpecLite{
		Type:       "ROWS",
		StartType:  "2 PRECEDING",
		StartValue: "2",
		EndType:    "CURRENT ROW",
		EndValue:   "",
	}

	keyFn := func(t types.Tuple) any { return nil }
	aggInit := func() any { return NewSortedMultiset() }
	agg := &MinAgg{ColName: "price"}

	op := NewWindowAggOp(WindowSpecLite{}, keyFn, []string{}, aggInit, agg)
	op.OrderByCol = "ts"
	op.FrameSpec = frameSpec

	// Input batch
	batch := types.Batch{
		{Tuple: types.Tuple{"ts": int64(1), "price": 100}, Count: 1},
		{Tuple: types.Tuple{"ts": int64(2), "price": 50}, Count: 1},
		{Tuple: types.Tuple{"ts": int64(3), "price": 75}, Count: 1},
		{Tuple: types.Tuple{"ts": int64(4), "price": 90}, Count: 1},
	}

	out, err := op.Apply(batch)
	if err != nil {
		t.Fatalf("Apply failed: %v", err)
	}

	if len(out) == 0 {
		t.Fatal("expected non-empty output")
	}

	t.Logf("Output batch:")
	for i, td := range out {
		t.Logf("  [%d] count=%d, tuple=%#v", i, td.Count, td.Tuple)
	}

	// Sliding window MIN with 3-row window
	// Row 1: min(100) = 100
	// Row 2: min(100, 50) = 50
	// Row 3: min(100, 50, 75) = 50
	// Row 4: min(50, 75, 90) = 50
}

func TestWindowAggOp_SlidingWindow(t *testing.T) {
	// Test sliding window: size=10s, slide=5s
	spec := WindowSpecLite{
		TimeCol:     "ts",
		SizeMillis:  10000, // 10 seconds
		WindowType:  WindowTypeSliding,
		SlideMillis: 5000, // 5 seconds
	}

	keyFn := func(t types.Tuple) any { return t["region"] }
	aggInit := func() any { return int64(0) }
	agg := &CountAgg{}

	op := NewWindowAggOp(spec, keyFn, []string{"region"}, aggInit, agg)

	// Input batch: events at ts=2, 7, 12
	batch := types.Batch{
		{Tuple: types.Tuple{"region": "East", "ts": int64(2000), "value": 10}, Count: 1},
		{Tuple: types.Tuple{"region": "East", "ts": int64(7000), "value": 20}, Count: 1},
		{Tuple: types.Tuple{"region": "East", "ts": int64(12000), "value": 30}, Count: 1},
	}

	out, err := op.Apply(batch)
	if err != nil {
		t.Fatalf("Apply failed: %v", err)
	}

	if len(out) == 0 {
		t.Fatal("expected non-empty output")
	}

	t.Logf("Sliding Window Output (%d deltas):", len(out))
	for i, td := range out {
		start := td.Tuple["__window_start"]
		end := td.Tuple["__window_end"]
		t.Logf("  [%d] window=[%v, %v) count=%d region=%v", 
			i, start, end, td.Count, td.Tuple["region"])
	}

	// Expected windows:
	// ts=2000 (2s) -> windows [0,10000)
	// ts=7000 (7s) -> windows [0,10000), [5000,15000)
	// ts=12000 (12s) -> windows [5000,15000), [10000,20000)
	// Total: 5 window updates expected
	if len(out) < 3 {
		t.Errorf("expected at least 3 output deltas, got %d", len(out))
	}
}

func TestWindowAggOp_SlidingWindowSum(t *testing.T) {
	// Test sliding window with SUM aggregation
	spec := WindowSpecLite{
		TimeCol:     "ts",
		SizeMillis:  15000, // 15 seconds
		WindowType:  WindowTypeSliding,
		SlideMillis: 5000, // 5 seconds
	}

	keyFn := func(t types.Tuple) any { return nil } // No grouping
	aggInit := func() any { return float64(0) }
	agg := &SumAgg{ColName: "amount"}

	op := NewWindowAggOp(spec, keyFn, []string{}, aggInit, agg)

	// Input batch
	batch := types.Batch{
		{Tuple: types.Tuple{"ts": int64(3000), "amount": 100.0}, Count: 1},
		{Tuple: types.Tuple{"ts": int64(8000), "amount": 50.0}, Count: 1},
		{Tuple: types.Tuple{"ts": int64(13000), "amount": 75.0}, Count: 1},
	}

	out, err := op.Apply(batch)
	if err != nil {
		t.Fatalf("Apply failed: %v", err)
	}

	t.Logf("Sliding Window SUM Output (%d deltas):", len(out))
	for i, td := range out {
		start := td.Tuple["__window_start"]
		end := td.Tuple["__window_end"]
		sum := td.Tuple["sum"]
		t.Logf("  [%d] window=[%v, %v) sum=%v", i, start, end, sum)
	}

	// With 15s window and 5s slide:
	// ts=3000 -> [0,15000)
	// ts=8000 -> [0,15000), [5000,20000)
	// ts=13000 -> [0,15000), [5000,20000), [10000,25000)
}

func TestWindowAggOp_SessionWindow(t *testing.T) {
	// Test session window with 5 second gap
	spec := WindowSpecLite{
		TimeCol:    "ts",
		WindowType: WindowTypeSession,
		GapMillis:  5000, // 5 seconds inactivity gap
	}

	keyFn := func(t types.Tuple) any { return t["user"] }
	aggInit := func() any { return int64(0) }
	agg := &CountAgg{}

	op := NewWindowAggOp(spec, keyFn, []string{"user"}, aggInit, agg)

	// Input: user has events at 1s, 3s, 10s (gap between 3s and 10s > 5s)
	batch := types.Batch{
		{Tuple: types.Tuple{"user": "Alice", "ts": int64(1000)}, Count: 1},
		{Tuple: types.Tuple{"user": "Alice", "ts": int64(3000)}, Count: 1},
		{Tuple: types.Tuple{"user": "Alice", "ts": int64(10000)}, Count: 1}, // New session
	}

	out, err := op.Apply(batch)
	if err != nil {
		t.Fatalf("Apply failed: %v", err)
	}

	t.Logf("Session Window Output (%d deltas):", len(out))
	for i, td := range out {
		start := td.Tuple["__window_start"]
		end := td.Tuple["__window_end"]
		t.Logf("  [%d] session=[%v, %v) count=%d user=%v", 
			i, start, end, td.Count, td.Tuple["user"])
	}

	// Expected: 2 sessions
	// Session 1: [1000, 8000) - events at 1s, 3s
	// Session 2: [10000, 15000) - event at 10s
	if len(out) < 2 {
		t.Errorf("expected at least 2 session windows, got %d", len(out))
	}
}

func TestWindowAggOp_SessionWindowMultipleUsers(t *testing.T) {
	// Test session window with multiple partitions
	spec := WindowSpecLite{
		TimeCol:    "ts",
		WindowType: WindowTypeSession,
		GapMillis:  3000, // 3 seconds gap
	}

	keyFn := func(t types.Tuple) any { return t["user"] }
	aggInit := func() any { return int64(0) }
	agg := &CountAgg{}

	op := NewWindowAggOp(spec, keyFn, []string{"user"}, aggInit, agg)

	// Multiple users with different activity patterns
	batch := types.Batch{
		{Tuple: types.Tuple{"user": "Alice", "ts": int64(1000)}, Count: 1},
		{Tuple: types.Tuple{"user": "Bob", "ts": int64(1500)}, Count: 1},
		{Tuple: types.Tuple{"user": "Alice", "ts": int64(2000)}, Count: 1},
		{Tuple: types.Tuple{"user": "Bob", "ts": int64(6000)}, Count: 1}, // Bob's new session
		{Tuple: types.Tuple{"user": "Alice", "ts": int64(7000)}, Count: 1}, // Alice's new session
	}

	out, err := op.Apply(batch)
	if err != nil {
		t.Fatalf("Apply failed: %v", err)
	}

	t.Logf("Multi-user Session Output (%d deltas):", len(out))
	for i, td := range out {
		start := td.Tuple["__window_start"]
		end := td.Tuple["__window_end"]
		user := td.Tuple["user"]
		t.Logf("  [%d] user=%v session=[%v, %v) count=%d", 
			i, user, start, end, td.Count)
	}

	// Each user should have separate sessions
	if len(out) == 0 {
		t.Error("expected session windows for multiple users")
	}
}

func TestInterval_Parse(t *testing.T) {
	tests := []struct {
		input    string
		expected int64 // expected milliseconds
		wantErr  bool
	}{
		{"5 minutes", 5 * 60 * 1000, false},
		{"1 hour", 60 * 60 * 1000, false},
		{"30 seconds", 30 * 1000, false},
		{"2 days", 2 * 24 * 60 * 60 * 1000, false},
		{"500 milliseconds", 500, false},
		{"invalid", 0, true},
		{"5", 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			interval, err := types.ParseInterval(tt.input)
			if tt.wantErr {
				if err == nil {
					t.Errorf("expected error for input %q", tt.input)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if interval.Millis != tt.expected {
				t.Errorf("expected %d millis, got %d", tt.expected, interval.Millis)
			}
		})
	}
}

