package types

import (
	"encoding/json"
	"testing"
)

func TestEqualAny_NumericCrossType(t *testing.T) {
	tests := []struct {
		name string
		a    any
		b    any
		want bool
	}{
		{name: "int float", a: 1, b: 1.0, want: true},
		{name: "int uint", a: int64(1), b: uint32(1), want: true},
		{name: "json number int", a: json.Number("1"), b: 1, want: true},
		{name: "json number float", a: json.Number("1.0"), b: float64(1), want: true},
		{name: "different values", a: 1, b: 2.0, want: false},
		{name: "negative vs uint", a: -1, b: uint64(1), want: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := EqualAny(tc.a, tc.b); got != tc.want {
				t.Fatalf("EqualAny(%v, %v)=%v, want %v", tc.a, tc.b, got, tc.want)
			}
		})
	}
}

func TestEqualAny_StructuralMapSlice(t *testing.T) {
	a := map[string]any{
		"items": []any{1, json.Number("2.0"), map[string]any{"x": 3}},
		"meta":  map[string]any{"ok": true},
	}
	b := map[string]any{
		"items": []any{1.0, 2, map[string]any{"x": 3.0}},
		"meta":  map[string]any{"ok": true},
	}

	if !EqualAny(a, b) {
		t.Fatalf("expected structural equality across map/slice with numeric coercion")
	}
}

func TestTuplesEqual_UsesEqualAny(t *testing.T) {
	t1 := Tuple{
		"id":      int64(1),
		"payload": map[string]any{"n": json.Number("1.0"), "tags": []any{"a", 2}},
	}
	t2 := Tuple{
		"id":      1.0,
		"payload": map[string]any{"n": 1, "tags": []any{"a", 2.0}},
	}

	if !TuplesEqual(t1, t2) {
		t.Fatalf("expected tuples to be equal")
	}
}
