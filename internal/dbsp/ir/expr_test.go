package ir

import (
	"encoding/json"
	"testing"
	"time"
)

func TestToTimeWithJSONNumber(t *testing.T) {
	v := json.Number("1735689600000")
	got, err := toTime(v)
	if err != nil {
		t.Fatalf("toTime error: %v", err)
	}
	if got.IsZero() {
		t.Fatalf("expected non-zero time")
	}
	if got.Year() != 2025 {
		t.Fatalf("expected year 2025, got %d", got.Year())
	}
}

func TestTimeBucketWithJSONNumber(t *testing.T) {
	interval := json.Number("300000")
	value := json.Number("1735689600000")
	out, err := evalTimeBucket(interval, value)
	if err != nil {
		t.Fatalf("evalTimeBucket error: %v", err)
	}
	bucket, ok := out.(time.Time)
	if !ok {
		t.Fatalf("expected time.Time, got %T", out)
	}
	if bucket.IsZero() {
		t.Fatalf("expected non-zero bucket time")
	}
}
