package op

import (
	"testing"

	"github.com/ariyn/dbsp/internal/dbsp/types"
)

// ============================================================================
// SUM Aggregation Tests
// ============================================================================

func TestSumAggBasic(t *testing.T) {
	keyFn := func(tu types.Tuple) any { return tu["k"] }
	aggInit := func() any { return float64(0) }
	sumAgg := &SumAgg{}

	g := NewGroupAggOp(keyFn, aggInit, sumAgg)

	batch := types.Batch{
		{Tuple: types.Tuple{"k": "A", "v": 2}, Count: 1},
		{Tuple: types.Tuple{"k": "B", "v": 3}, Count: 1},
		{Tuple: types.Tuple{"k": "A", "v": 4}, Count: 1},
	}
	_, err := g.Apply(batch)
	if err != nil {
		t.Fatalf("Apply failed: %v", err)
	}
	st := g.State()
	if st["A"] != 6.0 {
		t.Fatalf("expected A=6 got %v", st["A"])
	}
	if st["B"] != 3.0 {
		t.Fatalf("expected B=3 got %v", st["B"])
	}

	// delete one of A's value
	del := types.Batch{{Tuple: types.Tuple{"k": "A", "v": 2}, Count: -1}}
	_, err = g.Apply(del)
	if err != nil {
		t.Fatalf("Apply(delete) failed: %v", err)
	}
	st2 := g.State()
	if st2["A"] != 4.0 {
		t.Fatalf("expected A=4 after delete got %v", st2["A"])
	}
}

func TestSumAgg_ToleratesNumericStringAndNull(t *testing.T) {
	keyFn := func(tu types.Tuple) any { return tu["k"] }
	aggInit := func() any { return float64(0) }
	sumAgg := &SumAgg{ColName: "v"}

	g := NewGroupAggOp(keyFn, aggInit, sumAgg)

	_, err := g.Apply(types.Batch{
		{Tuple: types.Tuple{"k": "A", "v": "10"}, Count: 1},
		{Tuple: types.Tuple{"k": "A", "v": nil}, Count: 1},
		{Tuple: types.Tuple{"k": "A", "v": "not-a-number"}, Count: 1},
	})
	if err != nil {
		t.Fatalf("Apply failed: %v", err)
	}

	st := g.State()
	if st["A"] != 10.0 {
		t.Fatalf("expected A=10 got %v", st["A"])
	}
}

// ============================================================================
// COUNT Aggregation Tests
// ============================================================================

func TestCountAggBasic(t *testing.T) {
	keyFn := func(tu types.Tuple) any { return tu["k"] }
	aggInit := func() any { return int64(0) }
	countAgg := &CountAgg{}

	g := NewGroupAggOp(keyFn, aggInit, countAgg)

	batch := types.Batch{
		{Tuple: types.Tuple{"k": "A"}, Count: 1},
		{Tuple: types.Tuple{"k": "B"}, Count: 1},
		{Tuple: types.Tuple{"k": "A"}, Count: 1},
	}
	_, err := g.Apply(batch)
	if err != nil {
		t.Fatalf("Apply failed: %v", err)
	}
	st := g.State()
	if st["A"] != int64(2) {
		t.Fatalf("expected A=2 got %v", st["A"])
	}
	if st["B"] != int64(1) {
		t.Fatalf("expected B=1 got %v", st["B"])
	}

	// delete one of A's value
	del := types.Batch{{Tuple: types.Tuple{"k": "A"}, Count: -1}}
	_, err = g.Apply(del)
	if err != nil {
		t.Fatalf("Apply(delete) failed: %v", err)
	}
	st2 := g.State()
	if st2["A"] != int64(1) {
		t.Fatalf("expected A=1 after delete got %v", st2["A"])
	}
}

func TestCountAgg_IgnoresNullWhenColSpecified(t *testing.T) {
	keyFn := func(tu types.Tuple) any { return tu["k"] }
	aggInit := func() any { return int64(0) }
	countAgg := &CountAgg{ColName: "v"}

	g := NewGroupAggOp(keyFn, aggInit, countAgg)

	_, err := g.Apply(types.Batch{
		{Tuple: types.Tuple{"k": "A", "v": nil}, Count: 1},
		{Tuple: types.Tuple{"k": "A"}, Count: 1},
		{Tuple: types.Tuple{"k": "A", "v": 123}, Count: 1},
	})
	if err != nil {
		t.Fatalf("Apply failed: %v", err)
	}
	st := g.State()
	if st["A"] != int64(1) {
		t.Fatalf("expected A=1 got %v", st["A"])
	}

	// delete the only counted row
	_, err = g.Apply(types.Batch{{Tuple: types.Tuple{"k": "A", "v": 123}, Count: -1}})
	if err != nil {
		t.Fatalf("Apply(delete) failed: %v", err)
	}
	st2 := g.State()
	if st2["A"] != int64(0) {
		t.Fatalf("expected A=0 got %v", st2["A"])
	}
}

func TestCountAgg_ColNameStar_TreatedAsCountStar(t *testing.T) {
	keyFn := func(tu types.Tuple) any { return tu["k"] }
	aggInit := func() any { return int64(0) }
	countAgg := &CountAgg{ColName: "*"}

	g := NewGroupAggOp(keyFn, aggInit, countAgg)

	// COUNT(*) must count rows regardless of NULLs / missing columns.
	_, err := g.Apply(types.Batch{
		{Tuple: types.Tuple{"k": "A", "x": nil}, Count: 1},
		{Tuple: types.Tuple{"k": "A"}, Count: 1},
	})
	if err != nil {
		t.Fatalf("Apply failed: %v", err)
	}
	st := g.State()
	if st["A"] != int64(2) {
		t.Fatalf("expected A=2 got %v", st["A"])
	}
}

func TestGroupAggOp_MultiKey_OutputIncludesAllKeys(t *testing.T) {
	// Simulate a composite-key grouping: the internal key can be an encoded string,
	// but output should carry the original group key columns.
	keyFn := func(tu types.Tuple) any { return "{\"k1\":\"A\",\"k2\":1}" }
	aggInit := func() any { return float64(0) }
	sumAgg := &SumAgg{ColName: "v"}

	g := NewGroupAggOp(keyFn, aggInit, sumAgg)
	g.SetGroupKeyColNames([]string{"k1", "k2"})

	out, err := g.Apply(types.Batch{
		{Tuple: types.Tuple{"k1": "A", "k2": int64(1), "v": 2}, Count: 1},
	})
	if err != nil {
		t.Fatalf("Apply failed: %v", err)
	}
	if len(out) != 1 {
		t.Fatalf("expected 1 output delta, got %d", len(out))
	}
	if out[0].Tuple["k1"] != "A" {
		t.Fatalf("expected k1=A, got %v", out[0].Tuple["k1"])
	}
	if out[0].Tuple["k2"] != int64(1) {
		t.Fatalf("expected k2=1, got %v", out[0].Tuple["k2"])
	}
}

func TestGroupAggOp_MultiAgg_SumAndCount_WithDelete(t *testing.T) {
	keyFn := func(tu types.Tuple) any { return tu["k"] }
	g := NewGroupAggMultiOp(keyFn, []AggSlot{
		{Init: func() any { return float64(0) }, Fn: &SumAgg{ColName: "v", DeltaCol: "agg_delta"}},
		{Init: func() any { return int64(0) }, Fn: &CountAgg{ColName: "id", DeltaCol: "count_delta"}},
	})
	g.SetGroupKeyColNames([]string{"k"})

	// Insert two rows.
	out1, err := g.Apply(types.Batch{
		{Tuple: types.Tuple{"k": "A", "id": int64(1), "v": int64(10)}, Count: 1},
		{Tuple: types.Tuple{"k": "A", "id": int64(2), "v": int64(5)}, Count: 1},
	})
	if err != nil {
		t.Fatalf("Apply(insert) failed: %v", err)
	}
	if len(out1) == 0 {
		t.Fatalf("expected output deltas")
	}

	// Delete one row.
	out2, err := g.Apply(types.Batch{
		{Tuple: types.Tuple{"k": "A", "id": int64(1), "v": int64(10)}, Count: -1},
	})
	if err != nil {
		t.Fatalf("Apply(delete) failed: %v", err)
	}
	if len(out2) != 1 {
		t.Fatalf("expected 1 compacted output delta, got %d (%v)", len(out2), out2)
	}
	if out2[0].Tuple["k"] != "A" {
		t.Fatalf("expected k=A in output, got %v", out2[0].Tuple["k"])
	}
	if out2[0].Tuple["agg_delta"] != float64(-10) {
		t.Fatalf("expected agg_delta=-10, got %v", out2[0].Tuple["agg_delta"])
	}
	if out2[0].Tuple["count_delta"] != int64(-1) {
		t.Fatalf("expected count_delta=-1, got %v", out2[0].Tuple["count_delta"])
	}

	st := g.State()
	states, ok := st["A"].([]any)
	if !ok || len(states) != 2 {
		t.Fatalf("expected state slice of len 2, got %T (%v)", st["A"], st["A"])
	}
	if states[0] != float64(5) {
		t.Fatalf("expected sum state=5, got %v", states[0])
	}
	if states[1] != int64(1) {
		t.Fatalf("expected count state=1, got %v", states[1])
	}
}

// ============================================================================
// AVG Aggregation Tests
// ============================================================================

func TestAvgAggMonoid(t *testing.T) {
	keyFn := func(tu types.Tuple) any { return tu["k"] }
	aggInit := func() any { return AvgMonoid{} }
	avgAgg := &AvgAgg{ColName: "v"}

	g := NewGroupAggOp(keyFn, aggInit, avgAgg)

	// Insert: A: [10, 20, 30] → avg = 20
	batch := types.Batch{
		{Tuple: types.Tuple{"k": "A", "v": int64(10)}, Count: 1},
		{Tuple: types.Tuple{"k": "A", "v": int64(20)}, Count: 1},
		{Tuple: types.Tuple{"k": "A", "v": int64(30)}, Count: 1},
		{Tuple: types.Tuple{"k": "B", "v": int64(100)}, Count: 1},
	}
	out, err := g.Apply(batch)
	if err != nil {
		t.Fatalf("Apply failed: %v", err)
	}
	if len(out) == 0 {
		t.Fatalf("expected output deltas")
	}

	st := g.State()
	monoidA, ok := st["A"].(AvgMonoid)
	if !ok {
		t.Fatalf("expected AvgMonoid state for A, got %T", st["A"])
	}
	if monoidA.Sum != 60.0 {
		t.Errorf("expected A.Sum=60, got %v", monoidA.Sum)
	}
	if monoidA.Count != 3.0 {
		t.Errorf("expected A.Count=3, got %v", monoidA.Count)
	}
	if avg := monoidA.Value(); avg != 20.0 {
		t.Errorf("expected A.Value()=20, got %v", avg)
	}

	// Delete: remove 10 from A → [20, 30] → avg = 25
	del := types.Batch{
		{Tuple: types.Tuple{"k": "A", "v": int64(10)}, Count: -1},
	}
	out2, err := g.Apply(del)
	if err != nil {
		t.Fatalf("Apply(delete) failed: %v", err)
	}
	if len(out2) == 0 {
		t.Fatalf("expected output delta after delete")
	}

	st2 := g.State()
	monoidA2, ok := st2["A"].(AvgMonoid)
	if !ok {
		t.Fatalf("expected AvgMonoid state for A after delete")
	}
	if monoidA2.Sum != 50.0 {
		t.Errorf("expected A.Sum=50 after delete, got %v", monoidA2.Sum)
	}
	if monoidA2.Count != 2.0 {
		t.Errorf("expected A.Count=2 after delete, got %v", monoidA2.Count)
	}
	if avg := monoidA2.Value(); avg != 25.0 {
		t.Errorf("expected A.Value()=25 after delete, got %v", avg)
	}

	// Check delta output
	avgDelta := out2[0].Tuple["avg_delta"]
	if avgDelta != 5.0 { // 25 - 20 = 5
		t.Errorf("expected avg_delta=5, got %v", avgDelta)
	}
}

func TestAvgMonoid_Properties(t *testing.T) {
	// Test monoid properties

	// Identity
	zero := AvgMonoid{}.Zero()
	m1 := AvgMonoid{Sum: 100, Count: 4}
	combined := m1.Combine(zero)
	if combined != m1 {
		t.Errorf("identity property failed: %v ⊕ zero = %v, expected %v", m1, combined, m1)
	}

	// Associativity: (a ⊕ b) ⊕ c = a ⊕ (b ⊕ c)
	a := AvgMonoid{Sum: 10, Count: 2}
	b := AvgMonoid{Sum: 20, Count: 3}
	c := AvgMonoid{Sum: 30, Count: 5}

	left := a.Combine(b).Combine(c)
	right := a.Combine(b.Combine(c))

	if left != right {
		t.Errorf("associativity failed: (a⊕b)⊕c = %v, a⊕(b⊕c) = %v", left, right)
	}

	// Invertibility: a ⊕ (-a) = zero
	positive := AvgMonoid{Sum: 50, Count: 5}
	negative := AvgMonoid{Sum: -50, Count: -5}
	result := positive.Combine(negative)
	if result != zero {
		t.Errorf("invertibility failed: %v ⊕ %v = %v, expected zero", positive, negative, result)
	}
}

// ============================================================================
// MIN/MAX Aggregation Tests
// ============================================================================

func TestMinAggBasic(t *testing.T) {
	keyFn := func(tu types.Tuple) any { return tu["k"] }
	aggInit := func() any { return NewSortedMultiset() }
	minAgg := &MinAgg{ColName: "v"}

	g := NewGroupAggOp(keyFn, aggInit, minAgg)

	// Insert: A: [30, 10, 20], B: [5]
	batch := types.Batch{
		{Tuple: types.Tuple{"k": "A", "v": 30}, Count: 1},
		{Tuple: types.Tuple{"k": "A", "v": 10}, Count: 1},
		{Tuple: types.Tuple{"k": "A", "v": 20}, Count: 1},
		{Tuple: types.Tuple{"k": "B", "v": 5}, Count: 1},
	}

	out, err := g.Apply(batch)
	if err != nil {
		t.Fatalf("Apply failed: %v", err)
	}
	if len(out) == 0 {
		t.Fatal("expected output")
	}

	// Check MIN for A and B
	state := g.State()
	monoidA := state["A"].(SortedMultiset)
	monoidB := state["B"].(SortedMultiset)

	if monoidA.Min() != "10" {
		t.Errorf("expected A min=10, got %v", monoidA.Min())
	}
	if monoidB.Min() != "5" {
		t.Errorf("expected B min=5, got %v", monoidB.Min())
	}

	// Delete min value from A (10) → new min = 20
	del := types.Batch{
		{Tuple: types.Tuple{"k": "A", "v": 10}, Count: -1},
	}
	out2, err := g.Apply(del)
	if err != nil {
		t.Fatalf("Apply(delete) failed: %v", err)
	}
	if len(out2) == 0 {
		t.Error("expected output delta when min changes")
	}

	state2 := g.State()
	monoidA2 := state2["A"].(SortedMultiset)
	if monoidA2.Min() != "20" {
		t.Errorf("after delete: expected A min=20, got %v", monoidA2.Min())
	}
}

func TestMaxAggBasic(t *testing.T) {
	keyFn := func(tu types.Tuple) any { return tu["k"] }
	aggInit := func() any { return NewSortedMultiset() }
	maxAgg := &MaxAgg{ColName: "v"}

	g := NewGroupAggOp(keyFn, aggInit, maxAgg)

	// Insert: A: [30, 10, 20], B: [100]
	batch := types.Batch{
		{Tuple: types.Tuple{"k": "A", "v": 30}, Count: 1},
		{Tuple: types.Tuple{"k": "A", "v": 10}, Count: 1},
		{Tuple: types.Tuple{"k": "A", "v": 20}, Count: 1},
		{Tuple: types.Tuple{"k": "B", "v": 100}, Count: 1},
	}

	out, err := g.Apply(batch)
	if err != nil {
		t.Fatalf("Apply failed: %v", err)
	}
	if len(out) == 0 {
		t.Fatal("expected output")
	}

	// Check MAX for A and B
	state := g.State()
	monoidA := state["A"].(SortedMultiset)
	monoidB := state["B"].(SortedMultiset)

	if monoidA.Max() != "30" {
		t.Errorf("expected A max=30, got %v", monoidA.Max())
	}
	if monoidB.Max() != "100" {
		t.Errorf("expected B max=100, got %v", monoidB.Max())
	}

	// Delete max value from A (30) → new max = 20
	del := types.Batch{
		{Tuple: types.Tuple{"k": "A", "v": 30}, Count: -1},
	}
	out2, err := g.Apply(del)
	if err != nil {
		t.Fatalf("Apply(delete) failed: %v", err)
	}
	if len(out2) == 0 {
		t.Error("expected output delta when max changes")
	}

	state2 := g.State()
	monoidA2 := state2["A"].(SortedMultiset)
	if monoidA2.Max() != "20" {
		t.Errorf("after delete: expected A max=20, got %v", monoidA2.Max())
	}
}

func TestSortedMultiset_Properties(t *testing.T) {
	// Test adding and removing values
	m := NewSortedMultiset()

	// Add values
	m.Add(10, 1)
	m.Add(20, 1)
	m.Add(5, 1)

	if m.Min() != "10" && m.Min() != "20" && m.Min() != "5" {
		// Check if a minimum exists
		if m.IsEmpty() {
			t.Errorf("expected non-empty multiset")
		}
	}

	if m.Max() != "10" && m.Max() != "20" && m.Max() != "5" {
		// Check if a maximum exists
		if m.IsEmpty() {
			t.Errorf("expected non-empty multiset")
		}
	}

	// Remove a value
	m.Add(20, -1)

	// Remove min
	m.Add(5, -1)

	// Remove all
	m.Add(10, -1)
	if m.Min() != nil {
		t.Errorf("after removing all: expected min=nil, got %v", m.Min())
	}
	if m.Max() != nil {
		t.Errorf("after removing all: expected max=nil, got %v", m.Max())
	}
}
