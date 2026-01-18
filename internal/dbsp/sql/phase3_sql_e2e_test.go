package sqlconv

import (
	"sort"
	"testing"

	"github.com/ariyn/dbsp/internal/dbsp/op"
	"github.com/ariyn/dbsp/internal/dbsp/types"
)

type phase3AggSnapshot struct {
	sum   float64
	count int64
}

func phase3Key(td types.TupleDelta) string {
	if v, ok := td.Tuple["a.k"]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	if v, ok := td.Tuple["k"]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

func phase3ApplyAggDeltas(out types.Batch, snap map[string]phase3AggSnapshot) {
	for _, td := range out {
		k := phase3Key(td)
		if k == "" {
			continue
		}
		prev := snap[k]

		agd, _ := td.Tuple["agg_delta"].(float64)
		cd, _ := td.Tuple["count_delta"].(int64)

		m := td.Count
		if m == 0 {
			m = 1
		}
		prev.sum += agd * float64(m)
		prev.count += cd * m
		snap[k] = prev
	}
}

func phase3MapKeys(m map[string]phase3AggSnapshot) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func phase3AssertAggSnapshotEq(t *testing.T, got, want map[string]phase3AggSnapshot) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("snapshot key count mismatch: got=%d want=%d (gotKeys=%v wantKeys=%v)", len(got), len(want), phase3MapKeys(got), phase3MapKeys(want))
	}
	for k, w := range want {
		g, ok := got[k]
		if !ok {
			t.Fatalf("missing key %q in got snapshot (gotKeys=%v)", k, phase3MapKeys(got))
		}
		if g.sum != w.sum || g.count != w.count {
			t.Fatalf("snapshot mismatch for key %q: got(sum=%v,count=%v) want(sum=%v,count=%v)", k, g.sum, g.count, w.sum, w.count)
		}
	}
}

func TestPhase3_E2E_SQL_Filter_GroupAgg_MultiAgg(t *testing.T) {
	query := "SELECT k, SUM(v), COUNT(id) FROM t WHERE v >= 10 GROUP BY k"
	root, err := ParseQueryToDBSP(query)
	if err != nil {
		t.Fatalf("ParseQueryToDBSP: %v", err)
	}

	batches := []types.Batch{
		{
			{Tuple: types.Tuple{"k": "A", "v": 9.0, "id": int64(1)}, Count: 1},  // filtered out
			{Tuple: types.Tuple{"k": "A", "v": 10.0, "id": int64(2)}, Count: 1}, // in
		},
		{{Tuple: types.Tuple{"k": "A", "v": 10.0, "id": int64(2)}, Count: -1}},
		{{Tuple: types.Tuple{"k": "B", "v": 12.0, "id": int64(3)}, Count: 1}},
	}

	snap := make(map[string]phase3AggSnapshot)
	for _, b := range batches {
		out, err := op.Execute(root, b)
		if err != nil {
			t.Fatalf("Execute: %v", err)
		}
		phase3ApplyAggDeltas(out, snap)
	}

	want := map[string]phase3AggSnapshot{
		"A": {sum: 0.0, count: 0},
		"B": {sum: 12.0, count: 1},
	}
	phase3AssertAggSnapshotEq(t, snap, want)
}

func TestPhase3_E2E_SQL_Join_GroupAgg_MultiAgg(t *testing.T) {
	query := "SELECT a.k, SUM(b.v), COUNT(b.id) FROM a JOIN b ON a.id = b.id GROUP BY a.k"
	root, err := ParseQueryToDBSP(query)
	if err != nil {
		t.Fatalf("ParseQueryToDBSP: %v", err)
	}

	snap := make(map[string]phase3AggSnapshot)
	// 2-source graph: alternate ticks.
	ticks := []map[string]types.Batch{
		{"a": {{Tuple: types.Tuple{"a.id": int64(1), "a.k": "A"}, Count: 1}}},
		{"a": {{Tuple: types.Tuple{"a.id": int64(2), "a.k": "B"}, Count: 1}}},
		{"b": {{Tuple: types.Tuple{"b.id": int64(1), "b.v": 10.0}, Count: 1}}},
		{"b": {{Tuple: types.Tuple{"b.id": int64(2), "b.v": 7.0}, Count: 1}}},
		{"b": {{Tuple: types.Tuple{"b.id": int64(1), "b.v": 5.0}, Count: 1}}},
		{"b": {{Tuple: types.Tuple{"b.id": int64(1), "b.v": 5.0}, Count: -1}}},
	}
	for _, in := range ticks {
		out, err := op.ExecuteTick(root, in)
		if err != nil {
			t.Fatalf("ExecuteTick: %v", err)
		}
		phase3ApplyAggDeltas(out, snap)
	}

	want := map[string]phase3AggSnapshot{
		"A": {sum: 10.0, count: 1},
		"B": {sum: 7.0, count: 1},
	}
	phase3AssertAggSnapshotEq(t, snap, want)
}

func TestPhase3_E2E_SQL_FilterOverJoin_GroupAgg_MultiAgg(t *testing.T) {
	query := "SELECT a.k, SUM(b.v), COUNT(b.id) FROM a JOIN b ON a.id = b.id WHERE b.v >= 10 GROUP BY a.k"
	root, err := ParseQueryToDBSP(query)
	if err != nil {
		t.Fatalf("ParseQueryToDBSP: %v", err)
	}

	snap := make(map[string]phase3AggSnapshot)
	ticks := []map[string]types.Batch{
		{"a": {{Tuple: types.Tuple{"a.id": int64(1), "a.k": "A"}, Count: 1}}},
		// Two b rows joinable, but only v>=10 should count.
		{"b": {
			{Tuple: types.Tuple{"b.id": int64(1), "b.v": 9.0}, Count: 1},
			{Tuple: types.Tuple{"b.id": int64(1), "b.v": 10.0}, Count: 1},
		}},
		// Retraction of the passing row should revert the aggregate.
		{"b": {{Tuple: types.Tuple{"b.id": int64(1), "b.v": 10.0}, Count: -1}}},
	}
	for _, in := range ticks {
		out, err := op.ExecuteTick(root, in)
		if err != nil {
			t.Fatalf("ExecuteTick: %v", err)
		}
		phase3ApplyAggDeltas(out, snap)
	}

	want := map[string]phase3AggSnapshot{
		"A": {sum: 0.0, count: 0},
	}
	phase3AssertAggSnapshotEq(t, snap, want)
}

func TestPhase3_E2E_SQL_Filter_GroupAgg_MultiAgg_CountStar_TolerantNumericString(t *testing.T) {
	query := "SELECT k, SUM(v), COUNT(*) FROM t WHERE v >= 10 GROUP BY k"
	root, err := ParseQueryToDBSP(query)
	if err != nil {
		t.Fatalf("ParseQueryToDBSP: %v", err)
	}

	// v comes in as a numeric string; the engine should treat it as numeric
	// for both the filter and SUM aggregate.
	batches := []types.Batch{
		{
			{Tuple: types.Tuple{"k": "A", "v": "9"}, Count: 1},
			{Tuple: types.Tuple{"k": "A", "v": "10"}, Count: 1},
		},
		{{Tuple: types.Tuple{"k": "A", "v": "10"}, Count: -1}},
		{{Tuple: types.Tuple{"k": "B", "v": "12"}, Count: 1}},
	}

	snap := make(map[string]phase3AggSnapshot)
	for _, b := range batches {
		out, err := op.Execute(root, b)
		if err != nil {
			t.Fatalf("Execute: %v", err)
		}
		phase3ApplyAggDeltas(out, snap)
	}

	want := map[string]phase3AggSnapshot{
		"A": {sum: 0.0, count: 0},
		"B": {sum: 12.0, count: 1},
	}
	phase3AssertAggSnapshotEq(t, snap, want)
}
