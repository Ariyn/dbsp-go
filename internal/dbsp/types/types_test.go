package types

import (
	"encoding/json"
	"testing"
	"time"
)

func TestToFloat64Time(t *testing.T) {
	ts := time.Unix(1, 500)
	got, ok := ToFloat64Safe(ts)
	if !ok {
		t.Fatalf("expected time.Time to be convertible")
	}
	want := float64(ts.UnixNano())
	if got != want {
		t.Fatalf("unexpected float64 value: got=%v want=%v", got, want)
	}
}

func TestToInt64Time(t *testing.T) {
	ts := time.Unix(2, 750)
	got, ok := ToInt64Safe(ts)
	if !ok {
		t.Fatalf("expected time.Time to be convertible")
	}
	want := ts.UnixNano()
	if got != want {
		t.Fatalf("unexpected int64 value: got=%v want=%v", got, want)
	}
}

func TestEqualAnyFastNumericAndTimePaths(t *testing.T) {
	if !EqualAny(int64(42), float64(42)) {
		t.Fatal("expected int64 and float64 with same value to compare equal")
	}
	if !EqualAny(json.Number("42.5"), float64(42.5)) {
		t.Fatal("expected json.Number and float64 with same value to compare equal")
	}
	ts := time.Unix(3, 123)
	if !EqualAny(ts, ts) {
		t.Fatal("expected identical time.Time values to compare equal")
	}
	if EqualAny(ts, ts.Add(time.Nanosecond)) {
		t.Fatal("expected different time.Time values to compare unequal")
	}
}

func TestPackedTupleDistinguishesMissingFromNull(t *testing.T) {
	schema := NewPackedSchema([]string{"present_null", "missing"})
	packed := NewPackedTupleWithPresence(schema, []any{nil, nil}, []bool{true, false})

	if value, ok := packed.Get("present_null"); !ok || value != nil {
		t.Fatalf("expected present_null to be present with nil value, got value=%v ok=%v", value, ok)
	}
	if value, ok := packed.Get("missing"); ok || value != nil {
		t.Fatalf("expected missing to be absent, got value=%v ok=%v", value, ok)
	}

	materialized := packed.Materialize()
	if value, ok := materialized["present_null"]; !ok || value != nil {
		t.Fatalf("expected materialized null field to remain present, got value=%v ok=%v", value, ok)
	}
	if _, ok := materialized["missing"]; ok {
		t.Fatalf("expected missing field to stay absent after materialization: %v", materialized)
	}
}

func TestPackedTupleProjectPreservesPresence(t *testing.T) {
	schema := NewPackedSchema([]string{"a", "b", "c"})
	packed := NewPackedTupleWithPresence(schema, []any{int64(1), nil, "x"}, []bool{true, true, false})
	packed = packed.WithExtra("derived", int64(9))

	projected := packed.Project([]string{"b", "c", "derived"})

	if value, ok := projected.Get("b"); !ok || value != nil {
		t.Fatalf("expected projected b to remain present null, got value=%v ok=%v", value, ok)
	}
	if value, ok := projected.Get("c"); ok || value != nil {
		t.Fatalf("expected projected c to remain absent, got value=%v ok=%v", value, ok)
	}
	if value, ok := projected.Get("derived"); !ok || value != int64(9) {
		t.Fatalf("expected projected extra to be preserved, got value=%v ok=%v", value, ok)
	}
}

func TestPackedTupleWithExtrasMergesOnce(t *testing.T) {
	schema := NewPackedSchema([]string{"a"})
	packed := NewPackedTupleWithPresence(schema, []any{int64(1)}, []bool{true}).WithExtra("existing", int64(2))
	merged := packed.WithExtras(Tuple{"x": int64(3), "y": int64(4)})

	if value, ok := merged.Get("a"); !ok || value != int64(1) {
		t.Fatalf("expected base column to remain available, got value=%v ok=%v", value, ok)
	}
	if value, ok := merged.Get("existing"); !ok || value != int64(2) {
		t.Fatalf("expected existing extra to remain available, got value=%v ok=%v", value, ok)
	}
	if value, ok := merged.Get("x"); !ok || value != int64(3) {
		t.Fatalf("expected merged x extra, got value=%v ok=%v", value, ok)
	}
	if value, ok := merged.Get("y"); !ok || value != int64(4) {
		t.Fatalf("expected merged y extra, got value=%v ok=%v", value, ok)
	}
	if _, ok := packed.Get("x"); ok {
		t.Fatalf("expected original packed tuple to remain unchanged")
	}
}
