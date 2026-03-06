package op

import (
	"testing"
	"time"

	"github.com/ariyn/dbsp/internal/dbsp/types"
)

func TestStableAnyKeyFastPaths(t *testing.T) {
	if got := stableAnyKey("panel-1"); got != "s:panel-1" {
		t.Fatalf("unexpected string key encoding: %q", got)
	}
	decoded, err := decodeAnyKey("i:42")
	if err != nil {
		t.Fatalf("decode int key: %v", err)
	}
	if decoded.(int64) != 42 {
		t.Fatalf("expected int64 42, got %v (%T)", decoded, decoded)
	}
	ts := time.Unix(10, 20).UTC()
	encodedTime := stableAnyKey(ts)
	decoded, err = decodeAnyKey(encodedTime)
	if err != nil {
		t.Fatalf("decode time key: %v", err)
	}
	if !decoded.(time.Time).Equal(ts) {
		t.Fatalf("expected decoded time %v, got %v", ts, decoded)
	}
	encodedTuple := stableAnyKey(types.Tuple{"b": 2, "a": "x"})
	if encodedTuple != "m:a=s:x|b=i:2" {
		t.Fatalf("unexpected tuple key encoding: %q", encodedTuple)
	}
}

func TestStableTupleKeyCanonicalCachesColumnOrder(t *testing.T) {
	first := stableTupleKeyCanonical(types.Tuple{"b": 2, "a": "x", "c": true})
	second := stableTupleKeyCanonical(types.Tuple{"c": false, "a": "y", "b": 9})
	if first != "a=s:x|b=i:2|c=b:true" {
		t.Fatalf("unexpected first tuple key: %q", first)
	}
	if second != "a=s:y|b=i:9|c=b:false" {
		t.Fatalf("unexpected second tuple key: %q", second)
	}
}
