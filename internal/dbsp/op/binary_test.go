package op

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/ariyn/dbsp/internal/dbsp/types"
)

func testStableTupleKey(t types.Tuple) string {
	b, err := json.Marshal(t)
	if err == nil {
		return string(b)
	}
	return "<unmarshalable>"
}

func testBatchToCountMap(b types.Batch) map[string]int64 {
	m := make(map[string]int64)
	for _, td := range b {
		m[testStableTupleKey(td.Tuple)] += td.Count
	}
	for k, v := range m {
		if v == 0 {
			delete(m, k)
		}
	}
	return m
}

func TestJoinOp_SimpleJoin(t *testing.T) {
	// Create a simple join: Orders JOIN Customers on customer_id
	joinOp := NewJoinOp(
		func(tuple types.Tuple) any { return tuple["customer_id"] }, // left key
		func(tuple types.Tuple) any { return tuple["id"] },          // right key
		func(l, r types.Tuple) types.Tuple {
			// Combine left and right tuples
			result := make(types.Tuple)
			for k, v := range l {
				result[k] = v
			}
			for k, v := range r {
				result["c_"+k] = v // prefix customer columns with c_
			}
			return result
		},
	)

	// Initial state: empty
	// First batch: insert one customer
	customerBatch := types.Batch{
		{Tuple: types.Tuple{"id": 1, "name": "Alice"}, Count: 1},
	}

	// Second batch: insert one order for that customer
	orderBatch := types.Batch{
		{Tuple: types.Tuple{"order_id": 100, "customer_id": 1, "amount": 50}, Count: 1},
	}

	// Apply: no customers yet, so empty result from right
	result1, err := joinOp.ApplyBinary(types.Batch{}, customerBatch)
	if err != nil {
		t.Fatalf("ApplyBinary failed: %v", err)
	}
	if len(result1) != 0 {
		t.Errorf("Expected empty result with no left tuples, got %d tuples", len(result1))
	}

	// Apply: order joins with customer
	result2, err := joinOp.ApplyBinary(orderBatch, types.Batch{})
	if err != nil {
		t.Fatalf("ApplyBinary failed: %v", err)
	}

	if len(result2) != 1 {
		t.Fatalf("Expected 1 join result, got %d", len(result2))
	}

	joined := result2[0]
	if joined.Count != 1 {
		t.Errorf("Expected count 1, got %d", joined.Count)
	}
	if joined.Tuple["order_id"] != 100 {
		t.Errorf("Expected order_id 100, got %v", joined.Tuple["order_id"])
	}
	if joined.Tuple["c_name"] != "Alice" {
		t.Errorf("Expected c_name Alice, got %v", joined.Tuple["c_name"])
	}
}

func TestJoinOp_IncrementalJoin(t *testing.T) {
	// Test the three-term rule: d(R⋈S) = (dR⋈S) + (R⋈dS) + (dR⋈dS)
	joinOp := NewJoinOp(
		func(tuple types.Tuple) any { return tuple["key"] },
		func(tuple types.Tuple) any { return tuple["key"] },
		func(l, r types.Tuple) types.Tuple {
			return types.Tuple{
				"key":   l["key"],
				"l_val": l["val"],
				"r_val": r["val"],
			}
		},
	)

	// Phase 1: Insert initial data
	leftBatch1 := types.Batch{
		{Tuple: types.Tuple{"key": "a", "val": 1}, Count: 1},
	}
	rightBatch1 := types.Batch{
		{Tuple: types.Tuple{"key": "a", "val": 10}, Count: 1},
	}

	result1, err := joinOp.ApplyBinary(leftBatch1, rightBatch1)
	if err != nil {
		t.Fatalf("ApplyBinary failed: %v", err)
	}

	// Should have 3 results: (dL⋈S)=0 + (L⋈dS)=0 + (dL⋈dS)=1
	if len(result1) != 1 {
		t.Fatalf("Expected 1 result from dL⋈dS, got %d", len(result1))
	}
	if result1[0].Count != 1 {
		t.Errorf("Expected count 1, got %d", result1[0].Count)
	}

	// Phase 2: Insert more left data (should join with existing right state)
	leftBatch2 := types.Batch{
		{Tuple: types.Tuple{"key": "a", "val": 2}, Count: 1},
	}
	rightBatch2 := types.Batch{} // empty right delta

	result2, err := joinOp.ApplyBinary(leftBatch2, rightBatch2)
	if err != nil {
		t.Fatalf("ApplyBinary failed: %v", err)
	}

	// Should have 1 result: dL⋈S (new left joins with existing right state)
	if len(result2) != 1 {
		t.Fatalf("Expected 1 result from dL⋈S, got %d", len(result2))
	}
	if result2[0].Tuple["l_val"] != 2 {
		t.Errorf("Expected l_val=2, got %v", result2[0].Tuple["l_val"])
	}
	if result2[0].Tuple["r_val"] != 10 {
		t.Errorf("Expected r_val=10, got %v", result2[0].Tuple["r_val"])
	}

	// Phase 3: Insert more right data (should join with existing left state)
	leftBatch3 := types.Batch{} // empty left delta
	rightBatch3 := types.Batch{
		{Tuple: types.Tuple{"key": "a", "val": 20}, Count: 1},
	}

	result3, err := joinOp.ApplyBinary(leftBatch3, rightBatch3)
	if err != nil {
		t.Fatalf("ApplyBinary failed: %v", err)
	}

	// Should have 2 results: L⋈dS (existing left state joins with new right)
	// Left state has 2 tuples (val=1, val=2), right delta has 1 tuple (val=20)
	if len(result3) != 2 {
		t.Fatalf("Expected 2 results from L⋈dS, got %d", len(result3))
	}

	// Check both results
	vals := make(map[int]bool)
	for _, r := range result3 {
		if r.Tuple["r_val"] != 20 {
			t.Errorf("Expected r_val=20, got %v", r.Tuple["r_val"])
		}
		vals[r.Tuple["l_val"].(int)] = true
	}
	if !vals[1] || !vals[2] {
		t.Errorf("Expected l_val 1 and 2, got %v", vals)
	}
}

func TestJoinOp_Deletion(t *testing.T) {
	// Test join with deletions (Count = -1)
	joinOp := NewJoinOp(
		func(tuple types.Tuple) any { return tuple["key"] },
		func(tuple types.Tuple) any { return tuple["key"] },
		func(l, r types.Tuple) types.Tuple {
			return types.Tuple{
				"key":   l["key"],
				"l_val": l["val"],
				"r_val": r["val"],
			}
		},
	)

	// Phase 1: Insert data
	leftBatch1 := types.Batch{
		{Tuple: types.Tuple{"key": "x", "val": 1}, Count: 1},
	}
	rightBatch1 := types.Batch{
		{Tuple: types.Tuple{"key": "x", "val": 10}, Count: 1},
	}

	result1, err := joinOp.ApplyBinary(leftBatch1, rightBatch1)
	if err != nil {
		t.Fatalf("ApplyBinary failed: %v", err)
	}
	if len(result1) != 1 || result1[0].Count != 1 {
		t.Fatalf("Expected 1 insertion, got %v", result1)
	}

	// Phase 2: Delete left tuple
	leftBatch2 := types.Batch{
		{Tuple: types.Tuple{"key": "x", "val": 1}, Count: -1}, // deletion
	}
	rightBatch2 := types.Batch{}

	result2, err := joinOp.ApplyBinary(leftBatch2, rightBatch2)
	if err != nil {
		t.Fatalf("ApplyBinary failed: %v", err)
	}

	// Should produce a deletion in the join result (Count = -1)
	if len(result2) != 1 {
		t.Fatalf("Expected 1 deletion result, got %d", len(result2))
	}
	if result2[0].Count != -1 {
		t.Errorf("Expected count -1 (deletion), got %d", result2[0].Count)
	}
}

func TestJoinOp_MultipleKeys(t *testing.T) {
	// Test join with multiple matching keys
	joinOp := NewJoinOp(
		func(tuple types.Tuple) any { return tuple["dept"] },
		func(tuple types.Tuple) any { return tuple["dept"] },
		func(l, r types.Tuple) types.Tuple {
			return types.Tuple{
				"dept":      l["dept"],
				"emp_name":  l["name"],
				"dept_name": r["name"],
			}
		},
	)

	// Insert employees
	empBatch := types.Batch{
		{Tuple: types.Tuple{"dept": "eng", "name": "Alice"}, Count: 1},
		{Tuple: types.Tuple{"dept": "eng", "name": "Bob"}, Count: 1},
		{Tuple: types.Tuple{"dept": "sales", "name": "Charlie"}, Count: 1},
	}

	// Insert departments
	deptBatch := types.Batch{
		{Tuple: types.Tuple{"dept": "eng", "name": "Engineering"}, Count: 1},
		{Tuple: types.Tuple{"dept": "sales", "name": "Sales"}, Count: 1},
	}

	result, err := joinOp.ApplyBinary(empBatch, deptBatch)
	if err != nil {
		t.Fatalf("ApplyBinary failed: %v", err)
	}

	// Should have 3 results: Alice+Eng, Bob+Eng, Charlie+Sales
	if len(result) != 3 {
		t.Fatalf("Expected 3 join results, got %d", len(result))
	}

	// Verify join results
	engCount := 0
	salesCount := 0
	for _, r := range result {
		if r.Tuple["dept"] == "eng" {
			engCount++
			if r.Tuple["dept_name"] != "Engineering" {
				t.Errorf("Expected dept_name=Engineering, got %v", r.Tuple["dept_name"])
			}
		} else if r.Tuple["dept"] == "sales" {
			salesCount++
			if r.Tuple["dept_name"] != "Sales" {
				t.Errorf("Expected dept_name=Sales, got %v", r.Tuple["dept_name"])
			}
		}
	}

	if engCount != 2 {
		t.Errorf("Expected 2 Engineering employees, got %d", engCount)
	}
	if salesCount != 1 {
		t.Errorf("Expected 1 Sales employee, got %d", salesCount)
	}
}

func TestUnionOp(t *testing.T) {
	unionOp := NewUnionOp()

	leftBatch := types.Batch{
		{Tuple: types.Tuple{"id": 1}, Count: 1},
		{Tuple: types.Tuple{"id": 2}, Count: 1},
	}
	rightBatch := types.Batch{
		{Tuple: types.Tuple{"id": 3}, Count: 1},
	}

	result, err := unionOp.ApplyBinary(leftBatch, rightBatch)
	if err != nil {
		t.Fatalf("ApplyBinary failed: %v", err)
	}

	if len(result) != 3 {
		t.Fatalf("Expected 3 tuples in union, got %d", len(result))
	}
}

func TestDifferenceOp(t *testing.T) {
	diffOp := NewDifferenceOp()

	leftBatch := types.Batch{
		{Tuple: types.Tuple{"id": 1}, Count: 1},
		{Tuple: types.Tuple{"id": 2}, Count: 1},
	}
	rightBatch := types.Batch{
		{Tuple: types.Tuple{"id": 2}, Count: 1}, // subtract this
	}

	result, err := diffOp.ApplyBinary(leftBatch, rightBatch)
	if err != nil {
		t.Fatalf("ApplyBinary failed: %v", err)
	}

	// Should have 3 tuples: +id:1, +id:2, -id:2
	if len(result) != 3 {
		t.Fatalf("Expected 3 tuples, got %d", len(result))
	}

	// Check that id:2 appears with both +1 and -1
	countById := make(map[int]int64)
	for _, td := range result {
		id := td.Tuple["id"].(int)
		countById[id] += td.Count
	}

	if countById[1] != 1 {
		t.Errorf("Expected net count 1 for id:1, got %d", countById[1])
	}
	if countById[2] != 0 {
		t.Errorf("Expected net count 0 for id:2 (cancelled out), got %d", countById[2])
	}
}

func TestJoinOp_StateCompaction(t *testing.T) {
	joinOp := NewJoinOp(
		func(tuple types.Tuple) any { return tuple["key"] },
		func(tuple types.Tuple) any { return tuple["key"] },
		func(l, r types.Tuple) types.Tuple {
			return types.Tuple{"key": l["key"], "l": l["val"], "r": r["val"]}
		},
	)

	// Seed right state once.
	_, err := joinOp.ApplyBinary(nil, types.Batch{{Tuple: types.Tuple{"key": "k", "val": 10}, Count: 1}})
	if err != nil {
		t.Fatalf("ApplyBinary failed: %v", err)
	}

	// Repeated insert/delete on the same left tuple should not grow state.
	for i := 0; i < 50; i++ {
		_, err := joinOp.ApplyBinary(types.Batch{{Tuple: types.Tuple{"key": "k", "val": 1}, Count: 1}}, nil)
		if err != nil {
			t.Fatalf("ApplyBinary(insert) failed: %v", err)
		}
		_, err = joinOp.ApplyBinary(types.Batch{{Tuple: types.Tuple{"key": "k", "val": 1}, Count: -1}}, nil)
		if err != nil {
			t.Fatalf("ApplyBinary(delete) failed: %v", err)
		}
	}

	if len(joinOp.leftState) != 0 {
		t.Fatalf("expected leftState to be empty after net-zero churn, got %d keys", len(joinOp.leftState))
	}
	if len(joinOp.rightState) != 1 {
		t.Fatalf("expected rightState to keep seeded key, got %d keys", len(joinOp.rightState))
	}
}

func TestJoinOp_TTLExpiryRetractsOutput(t *testing.T) {
	now := time.Unix(0, 0)
	joinOp := NewJoinOp(
		func(tuple types.Tuple) any { return tuple["key"] },
		func(tuple types.Tuple) any { return tuple["key"] },
		func(l, r types.Tuple) types.Tuple {
			return types.Tuple{"key": l["key"], "l_val": l["val"], "r_val": r["val"]}
		},
	)
	joinOp.JoinTTL = 10 * time.Second
	joinOp.Now = func() time.Time { return now }

	// Insert one row on each side -> produces +1 join.
	out1, err := joinOp.ApplyBinary(
		types.Batch{{Tuple: types.Tuple{"key": "k", "val": 1}, Count: 1}},
		types.Batch{{Tuple: types.Tuple{"key": "k", "val": 10}, Count: 1}},
	)
	if err != nil {
		t.Fatalf("ApplyBinary failed: %v", err)
	}
	if len(out1) != 1 || out1[0].Count != 1 {
		t.Fatalf("expected one +1 join output, got %v", out1)
	}

	// Advance time beyond TTL and call with empty deltas -> should retract.
	now = now.Add(11 * time.Second)
	out2, err := joinOp.ApplyBinary(nil, nil)
	if err != nil {
		t.Fatalf("ApplyBinary failed: %v", err)
	}
	if len(out2) != 1 {
		t.Fatalf("expected one retraction due to TTL expiry, got %v", out2)
	}
	if out2[0].Count != -1 {
		t.Fatalf("expected retraction count -1, got %d", out2[0].Count)
	}
	if out2[0].Tuple["key"] != "k" || out2[0].Tuple["l_val"] != 1 || out2[0].Tuple["r_val"] != 10 {
		t.Fatalf("unexpected retraction tuple: %+v", out2[0].Tuple)
	}
	if len(joinOp.leftState) != 0 || len(joinOp.rightState) != 0 {
		t.Fatalf("expected join state to be empty after TTL eviction, left=%d right=%d", len(joinOp.leftState), len(joinOp.rightState))
	}
}

func TestJoinOp_TTLExpiryRetractsOutput_MultiplicityProduct(t *testing.T) {
	now := time.Unix(0, 0)
	joinOp := NewJoinOp(
		func(tuple types.Tuple) any { return tuple["key"] },
		func(tuple types.Tuple) any { return tuple["key"] },
		func(l, r types.Tuple) types.Tuple {
			return types.Tuple{"key": l["key"], "l": l["l"], "r": r["r"]}
		},
	)
	joinOp.JoinTTL = 10 * time.Second
	joinOp.Now = func() time.Time { return now }

	// left count=2, right count=3 => join output +6, TTL expiry => -6
	out1, err := joinOp.ApplyBinary(
		types.Batch{{Tuple: types.Tuple{"key": "k", "l": 1}, Count: 2}},
		types.Batch{{Tuple: types.Tuple{"key": "k", "r": 10}, Count: 3}},
	)
	if err != nil {
		t.Fatalf("ApplyBinary failed: %v", err)
	}
	if len(out1) != 1 || out1[0].Count != 6 {
		t.Fatalf("expected one +6 join output, got %v", out1)
	}

	now = now.Add(11 * time.Second)
	out2, err := joinOp.ApplyBinary(nil, nil)
	if err != nil {
		t.Fatalf("ApplyBinary failed: %v", err)
	}
	if len(out2) != 1 || out2[0].Count != -6 {
		t.Fatalf("expected one -6 retraction due to TTL expiry, got %v", out2)
	}
}

func TestJoinOp_TTLExpiry_LeftExpiresRightRemains_NoSecondRetract(t *testing.T) {
	now := time.Unix(0, 0)
	joinOp := NewJoinOp(
		func(tuple types.Tuple) any { return tuple["key"] },
		func(tuple types.Tuple) any { return tuple["key"] },
		func(l, r types.Tuple) types.Tuple {
			return types.Tuple{"key": l["key"], "l": l["l"], "r": r["r"]}
		},
	)
	joinOp.JoinTTL = 10 * time.Second
	joinOp.Now = func() time.Time { return now }

	// left at t=0
	_, err := joinOp.ApplyBinary(types.Batch{{Tuple: types.Tuple{"key": "k", "l": 1}, Count: 1}}, nil)
	if err != nil {
		t.Fatalf("ApplyBinary(left) failed: %v", err)
	}

	// right at t=5 -> produces +1 join
	now = now.Add(5 * time.Second)
	out1, err := joinOp.ApplyBinary(nil, types.Batch{{Tuple: types.Tuple{"key": "k", "r": 10}, Count: 1}})
	if err != nil {
		t.Fatalf("ApplyBinary(right) failed: %v", err)
	}
	if len(out1) != 1 || out1[0].Count != 1 {
		t.Fatalf("expected one +1 join output, got %v", out1)
	}

	// t=11: left expired (t=10), right still alive (t=15) -> retract -1, right remains
	now = time.Unix(0, 0).Add(11 * time.Second)
	out2, err := joinOp.ApplyBinary(nil, nil)
	if err != nil {
		t.Fatalf("ApplyBinary(evict) failed: %v", err)
	}
	if len(out2) != 1 || out2[0].Count != -1 {
		t.Fatalf("expected one -1 retraction due to left TTL expiry, got %v", out2)
	}
	if len(joinOp.leftState) != 0 {
		t.Fatalf("expected leftState empty after eviction, got %d keys", len(joinOp.leftState))
	}
	if len(joinOp.rightState) != 1 {
		t.Fatalf("expected rightState still present after left eviction, got %d keys", len(joinOp.rightState))
	}

	// t=16: right expired, but no left => no further retractions
	now = time.Unix(0, 0).Add(16 * time.Second)
	out3, err := joinOp.ApplyBinary(nil, nil)
	if err != nil {
		t.Fatalf("ApplyBinary(evict2) failed: %v", err)
	}
	if len(out3) != 0 {
		t.Fatalf("expected no output when only right expires with empty left, got %v", out3)
	}
	if len(joinOp.rightState) != 0 {
		t.Fatalf("expected rightState empty after eviction, got %d keys", len(joinOp.rightState))
	}
}

func TestJoinOp_TTLExpiry_BothSidesMultipleTuples_RetractsAllPairsOnce(t *testing.T) {
	now := time.Unix(0, 0)
	joinOp := NewJoinOp(
		func(tuple types.Tuple) any { return tuple["key"] },
		func(tuple types.Tuple) any { return tuple["key"] },
		func(l, r types.Tuple) types.Tuple {
			return types.Tuple{"key": l["key"], "l": l["l"], "r": r["r"]}
		},
	)
	joinOp.JoinTTL = 10 * time.Second
	joinOp.Now = func() time.Time { return now }

	// Seed both sides with 2 tuples each -> 4 join results.
	out1, err := joinOp.ApplyBinary(
		types.Batch{
			{Tuple: types.Tuple{"key": "k", "l": 1}, Count: 1},
			{Tuple: types.Tuple{"key": "k", "l": 2}, Count: 1},
		},
		types.Batch{
			{Tuple: types.Tuple{"key": "k", "r": 10}, Count: 1},
			{Tuple: types.Tuple{"key": "k", "r": 20}, Count: 1},
		},
	)
	if err != nil {
		t.Fatalf("ApplyBinary failed: %v", err)
	}
	if len(out1) != 4 {
		t.Fatalf("expected 4 join outputs, got %v", out1)
	}

	now = now.Add(11 * time.Second)
	out2, err := joinOp.ApplyBinary(nil, nil)
	if err != nil {
		t.Fatalf("ApplyBinary(evict) failed: %v", err)
	}
	counts := testBatchToCountMap(out2)

	expected := testBatchToCountMap(types.Batch{
		{Tuple: types.Tuple{"key": "k", "l": 1, "r": 10}, Count: -1},
		{Tuple: types.Tuple{"key": "k", "l": 1, "r": 20}, Count: -1},
		{Tuple: types.Tuple{"key": "k", "l": 2, "r": 10}, Count: -1},
		{Tuple: types.Tuple{"key": "k", "l": 2, "r": 20}, Count: -1},
	})

	if len(counts) != len(expected) {
		t.Fatalf("expected %d retractions, got %v", len(expected), out2)
	}
	for k, v := range expected {
		if counts[k] != v {
			t.Fatalf("unexpected retractions: expected %v got %v", expected, counts)
		}
	}
}

func TestJoinOp_TTLExpiry_EvictAndInsertSameTick_ProducesRetractAndNewJoin(t *testing.T) {
	now := time.Unix(0, 0)
	joinOp := NewJoinOp(
		func(tuple types.Tuple) any { return tuple["key"] },
		func(tuple types.Tuple) any { return tuple["key"] },
		func(l, r types.Tuple) types.Tuple {
			return types.Tuple{"key": l["key"], "l": l["l"], "r": r["r"]}
		},
	)
	joinOp.JoinTTL = 10 * time.Second
	joinOp.Now = func() time.Time { return now }

	// left at t=0
	_, err := joinOp.ApplyBinary(types.Batch{{Tuple: types.Tuple{"key": "k", "l": 1}, Count: 1}}, nil)
	if err != nil {
		t.Fatalf("ApplyBinary(left) failed: %v", err)
	}

	// right at t=5 -> +1 join with l=1
	now = now.Add(5 * time.Second)
	_, err = joinOp.ApplyBinary(nil, types.Batch{{Tuple: types.Tuple{"key": "k", "r": 10}, Count: 1}})
	if err != nil {
		t.Fatalf("ApplyBinary(right) failed: %v", err)
	}

	// t=11: left expires, but we also insert new left in the same tick.
	now = time.Unix(0, 0).Add(11 * time.Second)
	out, err := joinOp.ApplyBinary(types.Batch{{Tuple: types.Tuple{"key": "k", "l": 2}, Count: 1}}, nil)
	if err != nil {
		t.Fatalf("ApplyBinary(evict+insert) failed: %v", err)
	}

	counts := testBatchToCountMap(out)
	expected := testBatchToCountMap(types.Batch{
		{Tuple: types.Tuple{"key": "k", "l": 1, "r": 10}, Count: -1},
		{Tuple: types.Tuple{"key": "k", "l": 2, "r": 10}, Count: 1},
	})
	if len(counts) != len(expected) {
		t.Fatalf("unexpected output: expected %v got %v", expected, counts)
	}
	for k, v := range expected {
		if counts[k] != v {
			t.Fatalf("unexpected output: expected %v got %v", expected, counts)
		}
	}
}

func TestJoinOp_TTLExpiry_RefreshExtendsExpiration(t *testing.T) {
	now := time.Unix(0, 0)
	joinOp := NewJoinOp(
		func(tuple types.Tuple) any { return tuple["key"] },
		func(tuple types.Tuple) any { return tuple["key"] },
		func(l, r types.Tuple) types.Tuple {
			return types.Tuple{"key": l["key"], "l": l["l"], "r": r["r"]}
		},
	)
	joinOp.JoinTTL = 10 * time.Second
	joinOp.Now = func() time.Time { return now }

	// left at t=0
	_, err := joinOp.ApplyBinary(types.Batch{{Tuple: types.Tuple{"key": "k", "l": 1}, Count: 1}}, nil)
	if err != nil {
		t.Fatalf("ApplyBinary(left) failed: %v", err)
	}

	// right at t=5 -> +1 join
	now = now.Add(5 * time.Second)
	_, err = joinOp.ApplyBinary(nil, types.Batch{{Tuple: types.Tuple{"key": "k", "r": 10}, Count: 1}})
	if err != nil {
		t.Fatalf("ApplyBinary(right) failed: %v", err)
	}

	// refresh/duplicate insert on left at t=9 extends left expiration to t=19 and increases count to 2
	now = time.Unix(0, 0).Add(9 * time.Second)
	_, err = joinOp.ApplyBinary(types.Batch{{Tuple: types.Tuple{"key": "k", "l": 1}, Count: 1}}, nil)
	if err != nil {
		t.Fatalf("ApplyBinary(left refresh) failed: %v", err)
	}

	// t=11: neither side should be evicted (left expires at 19, right expires at 15)
	now = time.Unix(0, 0).Add(11 * time.Second)
	out1, err := joinOp.ApplyBinary(nil, nil)
	if err != nil {
		t.Fatalf("ApplyBinary(t=11) failed: %v", err)
	}
	if len(out1) != 0 {
		t.Fatalf("expected no output at t=11, got %v", out1)
	}

	// t=16: right expires and should retract against left count=2 -> one tuple with Count=-2
	now = time.Unix(0, 0).Add(16 * time.Second)
	out2, err := joinOp.ApplyBinary(nil, nil)
	if err != nil {
		t.Fatalf("ApplyBinary(t=16) failed: %v", err)
	}
	if len(out2) != 1 || out2[0].Count != -2 {
		t.Fatalf("expected one -2 retraction at t=16, got %v", out2)
	}
}
