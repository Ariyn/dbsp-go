package types

import (
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
