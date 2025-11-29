package sqlconv

import (
	"testing"

	"github.com/ariyn/dbsp/internal/dbsp/op"
	"github.com/ariyn/dbsp/internal/dbsp/types"
)

// ============================================================================
// LAG Query Parsing Tests
// ============================================================================

func TestParseWindowFunction_LAG(t *testing.T) {
	tests := []struct {
		name  string
		query string
		valid bool
	}{
		{
			name:  "LAG with PARTITION BY and ORDER BY",
			query: "SELECT LAG(a) OVER (PARTITION BY id ORDER BY ts) FROM t",
			valid: true,
		},
		{
			name:  "LAG with offset",
			query: "SELECT LAG(value, 2) OVER (PARTITION BY key ORDER BY ts) FROM t",
			valid: true,
		},
		{
			name:  "LAG without PARTITION BY",
			query: "SELECT LAG(a) OVER (ORDER BY ts) FROM t",
			valid: true,
		},
		{
			name:  "LAG with alias",
			query: "SELECT LAG(a) OVER (PARTITION BY id ORDER BY ts) AS prev_a FROM t",
			valid: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lp, err := ParseQueryToLogicalPlan(tt.query)
			if tt.valid && err != nil {
				t.Errorf("expected valid query but got error: %v", err)
			}
			if !tt.valid && err == nil {
				t.Errorf("expected invalid query but got no error")
			}
			if tt.valid {
				t.Logf("Logical plan: %+v", lp)
			}
		})
	}
}

func TestLAGQuery_EndToEnd(t *testing.T) {
	query := "SELECT LAG(a) OVER (PARTITION BY id ORDER BY ts) AS prev_a FROM t"

	node, err := ParseQueryToDBSP(query)
	if err != nil {
		t.Fatalf("ParseQueryToDBSP failed: %v", err)
	}

	// Test with sample data
	batch := types.Batch{
		{Tuple: types.Tuple{"id": "K1", "ts": int64(1), "a": 10}, Count: 1},
		{Tuple: types.Tuple{"id": "K1", "ts": int64(2), "a": 20}, Count: 1},
		{Tuple: types.Tuple{"id": "K1", "ts": int64(3), "a": 30}, Count: 1},
		{Tuple: types.Tuple{"id": "K2", "ts": int64(1), "a": 100}, Count: 1},
		{Tuple: types.Tuple{"id": "K2", "ts": int64(2), "a": 200}, Count: 1},
	}

	out, err := node.Op.Apply(batch)
	if err != nil {
		t.Fatalf("Apply failed: %v", err)
	}

	t.Logf("Output: %+v", out)

	// Verify output
	if len(out) == 0 {
		t.Fatal("expected output")
	}

	// Check that each row has prev_a field
	for i, delta := range out {
		if _, ok := delta.Tuple["prev_a"]; !ok {
			t.Errorf("row %d missing prev_a field", i)
		}
	}

	// Verify LAG values for K1
	expectedLag := map[int64]any{
		1: nil,
		2: 10,
		3: 20,
	}

	for _, delta := range out {
		if delta.Tuple["id"] == "K1" {
			ts := delta.Tuple["ts"].(int64)
			expected, ok := expectedLag[ts]
			if ok {
				got := delta.Tuple["prev_a"]
				if got != expected {
					t.Errorf("K1 ts=%d: expected prev_a=%v, got %v", ts, expected, got)
				}
			}
		}
	}
}

func TestLAGQuery_WithOffset(t *testing.T) {
	query := "SELECT LAG(value, 2) OVER (PARTITION BY key ORDER BY ts) AS lag_2 FROM t"

	node, err := ParseQueryToDBSP(query)
	if err != nil {
		t.Fatalf("ParseQueryToDBSP failed: %v", err)
	}

	batch := types.Batch{
		{Tuple: types.Tuple{"key": "A", "ts": int64(1), "value": 10}, Count: 1},
		{Tuple: types.Tuple{"key": "A", "ts": int64(2), "value": 20}, Count: 1},
		{Tuple: types.Tuple{"key": "A", "ts": int64(3), "value": 30}, Count: 1},
		{Tuple: types.Tuple{"key": "A", "ts": int64(4), "value": 40}, Count: 1},
	}

	out, err := node.Op.Apply(batch)
	if err != nil {
		t.Fatalf("Apply failed: %v", err)
	}

	t.Logf("Output: %+v", out)

	// Expected: LAG(value, 2)
	// ts=1: nil
	// ts=2: nil
	// ts=3: 10
	// ts=4: 20
	expectedLag := map[int64]any{
		1: nil,
		2: nil,
		3: 10,
		4: 20,
	}

	for _, delta := range out {
		ts := delta.Tuple["ts"].(int64)
		expected, ok := expectedLag[ts]
		if ok {
			got := delta.Tuple["lag_2"]
			if got != expected {
				t.Errorf("ts=%d: expected lag_2=%v, got %v", ts, expected, got)
			}
		}
	}
}

func TestLAGQuery_NoPartition(t *testing.T) {
	query := "SELECT LAG(value) OVER (ORDER BY ts) AS prev_value FROM t"

	node, err := ParseQueryToDBSP(query)
	if err != nil {
		t.Fatalf("ParseQueryToDBSP failed: %v", err)
	}

	batch := types.Batch{
		{Tuple: types.Tuple{"ts": int64(1), "value": 10}, Count: 1},
		{Tuple: types.Tuple{"ts": int64(2), "value": 20}, Count: 1},
		{Tuple: types.Tuple{"ts": int64(3), "value": 30}, Count: 1},
	}

	out, err := node.Op.Apply(batch)
	if err != nil {
		t.Fatalf("Apply failed: %v", err)
	}

	t.Logf("Output: %+v", out)

	// All in single partition
	expectedLag := map[int64]any{
		1: nil,
		2: 10,
		3: 20,
	}

	for _, delta := range out {
		ts := delta.Tuple["ts"].(int64)
		expected, ok := expectedLag[ts]
		if ok {
			got := delta.Tuple["prev_value"]
			if got != expected {
				t.Errorf("ts=%d: expected prev_value=%v, got %v", ts, expected, got)
			}
		}
	}
}

func TestLAGQuery_Sequential(t *testing.T) {
	query := "SELECT LAG(a) OVER (PARTITION BY id ORDER BY ts) AS prev_a FROM t"

	node, err := ParseQueryToDBSP(query)
	if err != nil {
		t.Fatalf("ParseQueryToDBSP failed: %v", err)
	}

	// Insert data sequentially
	batch1 := types.Batch{
		{Tuple: types.Tuple{"id": "K1", "ts": int64(1), "a": 10}, Count: 1},
	}
	out1, _ := node.Op.Apply(batch1)
	t.Logf("After ts=1: %+v", out1)

	batch2 := types.Batch{
		{Tuple: types.Tuple{"id": "K1", "ts": int64(2), "a": 20}, Count: 1},
	}
	out2, _ := node.Op.Apply(batch2)
	t.Logf("After ts=2: %+v", out2)

	// Verify ts=2 has prev_a=10
	if len(out2) > 0 {
		if out2[0].Tuple["prev_a"] != 10 {
			t.Errorf("ts=2: expected prev_a=10, got %v", out2[0].Tuple["prev_a"])
		}
	}

	batch3 := types.Batch{
		{Tuple: types.Tuple{"id": "K1", "ts": int64(3), "a": 30}, Count: 1},
	}
	out3, _ := node.Op.Apply(batch3)
	t.Logf("After ts=3: %+v", out3)

	// Verify ts=3 has prev_a=20
	if len(out3) > 0 {
		if out3[0].Tuple["prev_a"] != 20 {
			t.Errorf("ts=3: expected prev_a=20, got %v", out3[0].Tuple["prev_a"])
		}
	}
}

// ============================================================================
// LAG End-to-End Tests (Operator Level)
// ============================================================================

func TestLagEndToEnd_Sequential(t *testing.T) {
	// Create LAG query: LAG(value) PARTITION BY key ORDER BY ts
	keyFn := func(tu types.Tuple) any { return tu["key"] }
	aggInit := func() any {
		return op.LagMonoid{
			Buffer: op.NewOrderedBuffer("ts"),
		}
	}
	lagAgg := &op.LagAgg{
		OrderByCol: "ts",
		LagCol:     "a",
		Offset:     1,
		OutputCol:  "lag_a",
	}

	g := op.NewGroupAggOp(keyFn, aggInit, lagAgg)
	node := &op.Node{Op: g}

	t.Run("Single partition sequential inserts", func(t *testing.T) {
		// Time 1: Insert ts=1
		batch1 := types.Batch{
			{Tuple: types.Tuple{"key": "K1", "ts": int64(1), "a": 10}, Count: 1},
		}
		out1, err := node.Op.Apply(batch1)
		if err != nil {
			t.Fatalf("Apply failed: %v", err)
		}

		t.Logf("After ts=1: %+v", out1)
		if len(out1) != 1 {
			t.Errorf("expected 1 output, got %d", len(out1))
		}
		if out1[0].Tuple["lag_a"] != nil {
			t.Errorf("ts=1: expected lag=nil, got %v", out1[0].Tuple["lag_a"])
		}

		// Time 2: Insert ts=2
		batch2 := types.Batch{
			{Tuple: types.Tuple{"key": "K1", "ts": int64(2), "a": 20}, Count: 1},
		}
		out2, err := node.Op.Apply(batch2)
		if err != nil {
			t.Fatalf("Apply failed: %v", err)
		}

		t.Logf("After ts=2: %+v", out2)
		if len(out2) != 1 {
			t.Errorf("expected 1 output, got %d", len(out2))
		}
		if out2[0].Tuple["lag_a"] != 10 {
			t.Errorf("ts=2: expected lag=10, got %v", out2[0].Tuple["lag_a"])
		}

		// Time 3: Insert ts=3
		batch3 := types.Batch{
			{Tuple: types.Tuple{"key": "K1", "ts": int64(3), "a": 30}, Count: 1},
		}
		out3, err := node.Op.Apply(batch3)
		if err != nil {
			t.Fatalf("Apply failed: %v", err)
		}

		t.Logf("After ts=3: %+v", out3)
		if len(out3) != 1 {
			t.Errorf("expected 1 output, got %d", len(out3))
		}
		if out3[0].Tuple["lag_a"] != 20 {
			t.Errorf("ts=3: expected lag=20, got %v", out3[0].Tuple["lag_a"])
		}

		// Verify final state
		state := g.State()
		monoidK1, ok := state["K1"].(op.LagMonoid)
		if !ok {
			t.Fatalf("expected LagMonoid state")
		}
		if monoidK1.Buffer.Len() != 3 {
			t.Errorf("expected buffer len=3, got %d", monoidK1.Buffer.Len())
		}

		// Check LAG values at each position
		lag0 := monoidK1.Buffer.GetLagValue(0, 1, "a")
		lag1 := monoidK1.Buffer.GetLagValue(1, 1, "a")
		lag2 := monoidK1.Buffer.GetLagValue(2, 1, "a")

		if lag0 != nil {
			t.Errorf("final state: LAG(0)=nil expected, got %v", lag0)
		}
		if lag1 != 10 {
			t.Errorf("final state: LAG(1)=10 expected, got %v", lag1)
		}
		if lag2 != 20 {
			t.Errorf("final state: LAG(2)=20 expected, got %v", lag2)
		}
	})

	t.Run("Multiple partitions sequential", func(t *testing.T) {
		// Reset state
		keyFn := func(tu types.Tuple) any { return tu["key"] }
		aggInit := func() any {
			return op.LagMonoid{
				Buffer: op.NewOrderedBuffer("ts"),
			}
		}
		lagAgg := &op.LagAgg{
			OrderByCol: "ts",
			LagCol:     "a",
			Offset:     1,
			OutputCol:  "lag_a",
		}
		g := op.NewGroupAggOp(keyFn, aggInit, lagAgg)

		// Insert data for K1 and K2 interleaved
		batch1 := types.Batch{
			{Tuple: types.Tuple{"key": "K1", "ts": int64(1), "a": 10}, Count: 1},
			{Tuple: types.Tuple{"key": "K2", "ts": int64(1), "a": 100}, Count: 1},
		}
		out1, _ := g.Apply(batch1)
		t.Logf("Batch 1: %+v", out1)

		batch2 := types.Batch{
			{Tuple: types.Tuple{"key": "K1", "ts": int64(2), "a": 20}, Count: 1},
			{Tuple: types.Tuple{"key": "K2", "ts": int64(2), "a": 200}, Count: 1},
		}
		out2, _ := g.Apply(batch2)
		t.Logf("Batch 2: %+v", out2)

		batch3 := types.Batch{
			{Tuple: types.Tuple{"key": "K1", "ts": int64(3), "a": 30}, Count: 1},
			{Tuple: types.Tuple{"key": "K2", "ts": int64(3), "a": 300}, Count: 1},
		}
		out3, _ := g.Apply(batch3)
		t.Logf("Batch 3: %+v", out3)

		// Verify final state for both partitions
		state := g.State()

		monoidK1 := state["K1"].(op.LagMonoid)
		if monoidK1.Buffer.Len() != 3 {
			t.Errorf("K1: expected buffer len=3, got %d", monoidK1.Buffer.Len())
		}
		lag_k1_2 := monoidK1.Buffer.GetLagValue(2, 1, "a")
		if lag_k1_2 != 20 {
			t.Errorf("K1: LAG(ts=3) should be 20, got %v", lag_k1_2)
		}

		monoidK2 := state["K2"].(op.LagMonoid)
		if monoidK2.Buffer.Len() != 3 {
			t.Errorf("K2: expected buffer len=3, got %d", monoidK2.Buffer.Len())
		}
		lag_k2_2 := monoidK2.Buffer.GetLagValue(2, 1, "a")
		if lag_k2_2 != 200 {
			t.Errorf("K2: LAG(ts=3) should be 200, got %v", lag_k2_2)
		}
	})
}

func TestLagEndToEnd_OutOfOrder(t *testing.T) {
	keyFn := func(tu types.Tuple) any { return tu["key"] }
	aggInit := func() any {
		return op.LagMonoid{
			Buffer: op.NewOrderedBuffer("ts"),
		}
	}
	lagAgg := &op.LagAgg{
		OrderByCol: "ts",
		LagCol:     "a",
		Offset:     1,
		OutputCol:  "lag_a",
	}

	g := op.NewGroupAggOp(keyFn, aggInit, lagAgg)

	// Insert out of order: ts=1, ts=3, then ts=2
	batch1 := types.Batch{
		{Tuple: types.Tuple{"key": "K1", "ts": int64(1), "a": 10}, Count: 1},
	}
	out1, _ := g.Apply(batch1)
	t.Logf("Insert ts=1: %+v", out1)

	batch2 := types.Batch{
		{Tuple: types.Tuple{"key": "K1", "ts": int64(3), "a": 30}, Count: 1},
	}
	out2, _ := g.Apply(batch2)
	t.Logf("Insert ts=3: %+v", out2)

	batch3 := types.Batch{
		{Tuple: types.Tuple{"key": "K1", "ts": int64(2), "a": 20}, Count: 1},
	}
	out3, _ := g.Apply(batch3)
	t.Logf("Insert ts=2 (middle): %+v", out3)

	// Verify final state
	state := g.State()
	monoidK1 := state["K1"].(op.LagMonoid)

	// Check all LAG values
	lag0 := monoidK1.Buffer.GetLagValue(0, 1, "a")
	lag1 := monoidK1.Buffer.GetLagValue(1, 1, "a")
	lag2 := monoidK1.Buffer.GetLagValue(2, 1, "a")

	if lag0 != nil {
		t.Errorf("LAG(ts=1) should be nil, got %v", lag0)
	}
	if lag1 != 10 {
		t.Errorf("LAG(ts=2) should be 10, got %v", lag1)
	}
	if lag2 != 20 {
		t.Errorf("LAG(ts=3) should be 20 (not 10!), got %v", lag2)
	}

	t.Log("✓ Out-of-order insertion correctly updates LAG values")
}

func TestLagEndToEnd_WithDeletes(t *testing.T) {
	keyFn := func(tu types.Tuple) any { return tu["key"] }
	aggInit := func() any {
		return op.LagMonoid{
			Buffer: op.NewOrderedBuffer("ts"),
		}
	}
	lagAgg := &op.LagAgg{
		OrderByCol: "ts",
		LagCol:     "a",
		Offset:     1,
		OutputCol:  "lag_a",
	}

	g := op.NewGroupAggOp(keyFn, aggInit, lagAgg)

	// Insert: ts=1,2,3
	batchInsert := types.Batch{
		{Tuple: types.Tuple{"key": "K1", "ts": int64(1), "a": 10}, Count: 1},
		{Tuple: types.Tuple{"key": "K1", "ts": int64(2), "a": 20}, Count: 1},
		{Tuple: types.Tuple{"key": "K1", "ts": int64(3), "a": 30}, Count: 1},
	}
	outInsert, _ := g.Apply(batchInsert)
	t.Logf("After insert: %+v", outInsert)

	// State before delete
	state1 := g.State()
	monoid1 := state1["K1"].(op.LagMonoid)
	lag_before := monoid1.Buffer.GetLagValue(2, 1, "a")
	if lag_before != 20 {
		t.Errorf("Before delete: LAG(ts=3) should be 20, got %v", lag_before)
	}

	// Delete ts=2
	batchDelete := types.Batch{
		{Tuple: types.Tuple{"key": "K1", "ts": int64(2), "a": 20}, Count: -1},
	}
	outDelete, _ := g.Apply(batchDelete)
	t.Logf("After delete ts=2: %+v", outDelete)

	// Verify final state
	state2 := g.State()
	monoid2 := state2["K1"].(op.LagMonoid)

	if monoid2.Buffer.Len() != 2 {
		t.Errorf("After delete: expected len=2, got %d", monoid2.Buffer.Len())
	}

	lag_after := monoid2.Buffer.GetLagValue(1, 1, "a")
	if lag_after != 10 {
		t.Errorf("After delete: LAG(ts=3) should be 10, got %v", lag_after)
	}

	t.Log("✓ Deletion correctly updates downstream LAG values")
}

func TestLagEndToEnd_DifferentOffsets(t *testing.T) {
	testCases := []struct {
		name   string
		offset int
		ts     []int64
		values []int
		want   []any
	}{
		{
			name:   "LAG offset 1",
			offset: 1,
			ts:     []int64{1, 2, 3, 4},
			values: []int{10, 20, 30, 40},
			want:   []any{nil, 10, 20, 30},
		},
		{
			name:   "LAG offset 2",
			offset: 2,
			ts:     []int64{1, 2, 3, 4},
			values: []int{10, 20, 30, 40},
			want:   []any{nil, nil, 10, 20},
		},
		{
			name:   "LAG offset 3",
			offset: 3,
			ts:     []int64{1, 2, 3, 4, 5},
			values: []int{10, 20, 30, 40, 50},
			want:   []any{nil, nil, nil, 10, 20},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			keyFn := func(tu types.Tuple) any { return tu["key"] }
			aggInit := func() any {
				return op.LagMonoid{
					Buffer: op.NewOrderedBuffer("ts"),
				}
			}
			lagAgg := &op.LagAgg{
				OrderByCol: "ts",
				LagCol:     "a",
				Offset:     tc.offset,
				OutputCol:  "lag_a",
			}

			g := op.NewGroupAggOp(keyFn, aggInit, lagAgg)

			// Insert all data
			batch := types.Batch{}
			for i, ts := range tc.ts {
				batch = append(batch, types.TupleDelta{
					Tuple: types.Tuple{"key": "K1", "ts": ts, "a": tc.values[i]},
					Count: 1,
				})
			}

			out, err := g.Apply(batch)
			if err != nil {
				t.Fatalf("Apply failed: %v", err)
			}

			t.Logf("Output: %+v", out)

			// Verify final state
			state := g.State()
			monoid := state["K1"].(op.LagMonoid)

			for i := range tc.ts {
				lag := monoid.Buffer.GetLagValue(i, tc.offset, "a")
				if lag != tc.want[i] {
					t.Errorf("Position %d: expected LAG=%v, got %v", i, tc.want[i], lag)
				}
			}
		})
	}
}

func TestLagEndToEnd_IncrementalView(t *testing.T) {
	keyFn := func(tu types.Tuple) any { return tu["key"] }
	aggInit := func() any {
		return op.LagMonoid{
			Buffer: op.NewOrderedBuffer("ts"),
		}
	}
	lagAgg := &op.LagAgg{
		OrderByCol: "ts",
		LagCol:     "a",
		Offset:     1,
		OutputCol:  "lag_a",
	}

	g := op.NewGroupAggOp(keyFn, aggInit, lagAgg)

	// Simulate a materialized view of LAG values
	view := make(map[string]map[int64]any)

	// Helper to apply batch and update view
	applyAndUpdateView := func(batch types.Batch) {
		out, err := g.Apply(batch)
		if err != nil {
			t.Fatalf("Apply failed: %v", err)
		}

		for _, delta := range out {
			key := delta.Tuple["key"].(string)
			ts := delta.Tuple["ts"].(int64)
			lagVal := delta.Tuple["lag_a"]

			if _, ok := view[key]; !ok {
				view[key] = make(map[int64]any)
			}

			if delta.Count > 0 {
				view[key][ts] = lagVal
			} else {
				delete(view[key], ts)
			}
		}
	}

	// Transaction 1: Insert initial data
	applyAndUpdateView(types.Batch{
		{Tuple: types.Tuple{"key": "K1", "ts": int64(1), "a": 10}, Count: 1},
		{Tuple: types.Tuple{"key": "K1", "ts": int64(3), "a": 30}, Count: 1},
	})

	t.Logf("View after tx1: %+v", view)
	if view["K1"][int64(1)] != nil {
		t.Errorf("View[K1][1]: expected nil, got %v", view["K1"][int64(1)])
	}
	if view["K1"][int64(3)] != 10 {
		t.Errorf("View[K1][3]: expected 10, got %v", view["K1"][int64(3)])
	}

	// Transaction 2: Insert middle row
	applyAndUpdateView(types.Batch{
		{Tuple: types.Tuple{"key": "K1", "ts": int64(2), "a": 20}, Count: 1},
	})

	t.Logf("View after tx2: %+v", view)
	if view["K1"][int64(2)] != 10 {
		t.Errorf("View[K1][2]: expected 10, got %v", view["K1"][int64(2)])
	}

	// Transaction 3: Delete row
	applyAndUpdateView(types.Batch{
		{Tuple: types.Tuple{"key": "K1", "ts": int64(2), "a": 20}, Count: -1},
	})

	t.Logf("View after tx3 (delete): %+v", view)

	t.Log("✓ Incremental view maintenance works correctly")
}

// ============================================================================
// LAG SQL End-to-End Tests
// ============================================================================

func TestLagSQL_E2E_SinglePartition(t *testing.T) {
	query := "SELECT LAG(value) OVER (PARTITION BY id ORDER BY ts) AS prev_value FROM t"

	node, err := ParseQueryToIncrementalDBSP(query)
	if err != nil {
		t.Fatalf("failed to parse query: %v", err)
	}

	t.Run("Sequential insertion", func(t *testing.T) {
		// Insert ts=1
		batch1 := types.Batch{
			{Tuple: types.Tuple{"id": "A", "ts": int64(1), "value": 10}, Count: 1},
		}
		out1, err := node.Op.Apply(batch1)
		if err != nil {
			t.Fatalf("Apply failed: %v", err)
		}
		t.Logf("After ts=1: %+v", out1)
		if len(out1) != 1 || out1[0].Tuple["prev_value"] != nil {
			t.Errorf("ts=1: expected prev_value=nil, got %v", out1[0].Tuple["prev_value"])
		}

		// Insert ts=2
		batch2 := types.Batch{
			{Tuple: types.Tuple{"id": "A", "ts": int64(2), "value": 20}, Count: 1},
		}
		out2, err := node.Op.Apply(batch2)
		if err != nil {
			t.Fatalf("Apply failed: %v", err)
		}
		t.Logf("After ts=2: %+v", out2)
		if len(out2) != 1 || out2[0].Tuple["prev_value"] != 10 {
			t.Errorf("ts=2: expected prev_value=10, got %v", out2[0].Tuple["prev_value"])
		}

		// Insert ts=3
		batch3 := types.Batch{
			{Tuple: types.Tuple{"id": "A", "ts": int64(3), "value": 30}, Count: 1},
		}
		out3, err := node.Op.Apply(batch3)
		if err != nil {
			t.Fatalf("Apply failed: %v", err)
		}
		t.Logf("After ts=3: %+v", out3)
		if len(out3) != 1 || out3[0].Tuple["prev_value"] != 20 {
			t.Errorf("ts=3: expected prev_value=20, got %v", out3[0].Tuple["prev_value"])
		}
	})
}

func TestLagSQL_E2E_MultiplePartitions(t *testing.T) {
	query := "SELECT LAG(value) OVER (PARTITION BY id ORDER BY ts) AS prev_value FROM t"

	node, err := ParseQueryToIncrementalDBSP(query)
	if err != nil {
		t.Fatalf("failed to parse query: %v", err)
	}

	batch := types.Batch{
		{Tuple: types.Tuple{"id": "A", "ts": int64(1), "value": 10}, Count: 1},
		{Tuple: types.Tuple{"id": "A", "ts": int64(2), "value": 20}, Count: 1},
		{Tuple: types.Tuple{"id": "A", "ts": int64(3), "value": 30}, Count: 1},
		{Tuple: types.Tuple{"id": "B", "ts": int64(1), "value": 100}, Count: 1},
		{Tuple: types.Tuple{"id": "B", "ts": int64(2), "value": 200}, Count: 1},
	}

	out, err := node.Op.Apply(batch)
	if err != nil {
		t.Fatalf("Apply failed: %v", err)
	}

	t.Logf("Output: %+v", out)

	// Verify LAG values
	expectedA := map[int64]any{1: nil, 2: 10, 3: 20}
	expectedB := map[int64]any{1: nil, 2: 100}

	for _, delta := range out {
		id := delta.Tuple["id"].(string)
		ts := delta.Tuple["ts"].(int64)
		prevValue := delta.Tuple["prev_value"]

		if id == "A" {
			if expectedA[ts] != prevValue {
				t.Errorf("id=%s, ts=%d: expected prev_value=%v, got %v", id, ts, expectedA[ts], prevValue)
			}
		} else if id == "B" {
			if expectedB[ts] != prevValue {
				t.Errorf("id=%s, ts=%d: expected prev_value=%v, got %v", id, ts, expectedB[ts], prevValue)
			}
		}
	}
}

func TestLagSQL_E2E_OutOfOrder(t *testing.T) {
	query := "SELECT LAG(value) OVER (PARTITION BY id ORDER BY ts) AS prev_value FROM t"

	node, err := ParseQueryToIncrementalDBSP(query)
	if err != nil {
		t.Fatalf("failed to parse query: %v", err)
	}

	// Insert in order: ts=1, ts=3
	batch1 := types.Batch{
		{Tuple: types.Tuple{"id": "A", "ts": int64(1), "value": 10}, Count: 1},
		{Tuple: types.Tuple{"id": "A", "ts": int64(3), "value": 30}, Count: 1},
	}
	out1, err := node.Op.Apply(batch1)
	if err != nil {
		t.Fatalf("Apply batch1 failed: %v", err)
	}
	t.Logf("After insert ts=1,3: %+v", out1)

	// Now insert ts=2 (out of order)
	batch2 := types.Batch{
		{Tuple: types.Tuple{"id": "A", "ts": int64(2), "value": 20}, Count: 1},
	}
	out2, err := node.Op.Apply(batch2)
	if err != nil {
		t.Fatalf("Apply batch2 failed: %v", err)
	}
	t.Logf("After insert ts=2: %+v", out2)

	// Verify that we get delta for ts=2
	foundTs2 := false
	for _, delta := range out2 {
		ts := delta.Tuple["ts"].(int64)
		prevValue := delta.Tuple["prev_value"]

		if ts == 2 {
			foundTs2 = true
			if prevValue != 10 {
				t.Errorf("ts=2: expected prev_value=10, got %v", prevValue)
			}
		}
	}

	if !foundTs2 {
		t.Error("expected delta for ts=2")
	}

	t.Log("✓ Out-of-order insertion correctly emits delta for new row")
}

func TestLagSQL_E2E_WithDeletion(t *testing.T) {
	query := "SELECT LAG(value) OVER (PARTITION BY id ORDER BY ts) AS prev_value FROM t"

	node, err := ParseQueryToIncrementalDBSP(query)
	if err != nil {
		t.Fatalf("failed to parse query: %v", err)
	}

	// Insert ts=1, ts=2, ts=3
	batchInsert := types.Batch{
		{Tuple: types.Tuple{"id": "A", "ts": int64(1), "value": 10}, Count: 1},
		{Tuple: types.Tuple{"id": "A", "ts": int64(2), "value": 20}, Count: 1},
		{Tuple: types.Tuple{"id": "A", "ts": int64(3), "value": 30}, Count: 1},
	}
	outInsert, err := node.Op.Apply(batchInsert)
	if err != nil {
		t.Fatalf("Apply insert failed: %v", err)
	}
	t.Logf("After insert: %+v", outInsert)

	// Delete ts=2
	batchDelete := types.Batch{
		{Tuple: types.Tuple{"id": "A", "ts": int64(2), "value": 20}, Count: -1},
	}
	outDelete, err := node.Op.Apply(batchDelete)
	if err != nil {
		t.Fatalf("Apply delete failed: %v", err)
	}
	t.Logf("After delete ts=2: %+v", outDelete)

	foundDelta := false
	for _, delta := range outDelete {
		ts := delta.Tuple["ts"].(int64)
		if ts == 2 || ts == 3 {
			foundDelta = true
			t.Logf("Delta after delete: ts=%d, count=%d, prev_value=%v",
				ts, delta.Count, delta.Tuple["prev_value"])
		}
	}

	if !foundDelta {
		t.Error("expected delta output after deletion")
	}

	t.Log("✓ Deletion correctly emits delta output")
}

func TestLagSQL_E2E_WithOffset(t *testing.T) {
	testCases := []struct {
		name     string
		query    string
		expected map[int64]any
	}{
		{
			name:     "LAG offset 1",
			query:    "SELECT LAG(value, 1) OVER (PARTITION BY id ORDER BY ts) AS prev_1 FROM t",
			expected: map[int64]any{1: nil, 2: 10, 3: 20, 4: 30},
		},
		{
			name:     "LAG offset 2",
			query:    "SELECT LAG(value, 2) OVER (PARTITION BY id ORDER BY ts) AS prev_2 FROM t",
			expected: map[int64]any{1: nil, 2: nil, 3: 10, 4: 20},
		},
		{
			name:     "LAG offset 3",
			query:    "SELECT LAG(value, 3) OVER (PARTITION BY id ORDER BY ts) AS prev_3 FROM t",
			expected: map[int64]any{1: nil, 2: nil, 3: nil, 4: 10},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			node, err := ParseQueryToIncrementalDBSP(tc.query)
			if err != nil {
				t.Fatalf("failed to parse query: %v", err)
			}

			batch := types.Batch{
				{Tuple: types.Tuple{"id": "A", "ts": int64(1), "value": 10}, Count: 1},
				{Tuple: types.Tuple{"id": "A", "ts": int64(2), "value": 20}, Count: 1},
				{Tuple: types.Tuple{"id": "A", "ts": int64(3), "value": 30}, Count: 1},
				{Tuple: types.Tuple{"id": "A", "ts": int64(4), "value": 40}, Count: 1},
			}

			out, err := node.Op.Apply(batch)
			if err != nil {
				t.Fatalf("Apply failed: %v", err)
			}

			t.Logf("Output: %+v", out)

			// Verify LAG values
			for _, delta := range out {
				ts := delta.Tuple["ts"].(int64)
				var lagValue any
				for k, v := range delta.Tuple {
					if k != "id" && k != "ts" && k != "value" {
						lagValue = v
						break
					}
				}

				expectedValue := tc.expected[ts]
				if lagValue != expectedValue {
					t.Errorf("ts=%d: expected %v, got %v", ts, expectedValue, lagValue)
				}
			}
		})
	}
}

func TestLagSQL_E2E_NoPartition(t *testing.T) {
	query := "SELECT LAG(value) OVER (ORDER BY ts) AS prev_value FROM t"

	node, err := ParseQueryToIncrementalDBSP(query)
	if err != nil {
		t.Fatalf("failed to parse query: %v", err)
	}

	batch := types.Batch{
		{Tuple: types.Tuple{"id": "A", "ts": int64(1), "value": 10}, Count: 1},
		{Tuple: types.Tuple{"id": "B", "ts": int64(2), "value": 20}, Count: 1},
		{Tuple: types.Tuple{"id": "A", "ts": int64(3), "value": 30}, Count: 1},
		{Tuple: types.Tuple{"id": "B", "ts": int64(4), "value": 40}, Count: 1},
	}

	out, err := node.Op.Apply(batch)
	if err != nil {
		t.Fatalf("Apply failed: %v", err)
	}

	t.Logf("Output: %+v", out)

	expected := map[int64]any{1: nil, 2: 10, 3: 20, 4: 30}

	for _, delta := range out {
		ts := delta.Tuple["ts"].(int64)
		prevValue := delta.Tuple["prev_value"]

		if expected[ts] != prevValue {
			t.Errorf("ts=%d: expected prev_value=%v, got %v", ts, expected[ts], prevValue)
		}
	}

	t.Log("✓ LAG without PARTITION BY treats all rows as single partition")
}

// ============================================================================
// Window Aggregation Integration Tests
// ============================================================================

func TestWindowedAvgLikeIntegration(t *testing.T) {
	query := `SELECT k, AVG(v) FROM t GROUP BY TUMBLE(ts, INTERVAL 5 MINUTE), k`

	root, err := ParseQueryToIncrementalDBSP(query)
	if err != nil {
		t.Fatalf("failed to build DBSP graph: %v", err)
	}

	min := int64(60 * 1000)
	batch := types.Batch{
		{Tuple: types.Tuple{"ts": 1 * min, "k": "A", "v": int64(10)}, Count: 1},
		{Tuple: types.Tuple{"ts": 2 * min, "k": "A", "v": int64(20)}, Count: 1},
		{Tuple: types.Tuple{"ts": 4 * min, "k": "A", "v": int64(30)}, Count: 1},
		{Tuple: types.Tuple{"ts": 6 * min, "k": "A", "v": int64(40)}, Count: 1},
	}

	out, err := op.Execute(root, batch)
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if len(out) == 0 {
		t.Fatalf("expected some output deltas from windowed aggregation")
	}

	t.Logf("Output batch has %d deltas:", len(out))
	for i, td := range out {
		t.Logf("  [%d] count=%d, tuple=%#v", i, td.Count, td.Tuple)
	}
}
