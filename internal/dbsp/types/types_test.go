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
