package ir

import "testing"

func TestEnergyExprEval(t *testing.T) {
	expr := "(p_out + p_out_last) * timedelta_second / 2.0 / 3600.0"
	fn := BuildExprFunc(expr)
	tuple := map[string]any{
		"p_out":            2000.0,
		"p_out_last":       1980.0,
		"timedelta_second": 60.0,
	}
	v, err := fn(tuple)
	if err != nil {
		t.Fatalf("expr eval error: %v", err)
	}
	if v == nil {
		t.Fatalf("expr eval returned nil")
	}
}

func TestTimestampCastToDoubleFromString(t *testing.T) {
	expr := "(timestamp::DOUBLE / 1000000000.0)"
	fn := BuildExprFunc(expr)
	tuple := map[string]any{
		"timestamp": "2024-01-01T00:00:01Z",
	}
	v, err := fn(tuple)
	if err != nil {
		t.Fatalf("expr eval error: %v", err)
	}
	f, ok := v.(float64)
	if !ok {
		t.Fatalf("expected float64 result, got %T", v)
	}
	if f <= 0 {
		t.Fatalf("expected positive seconds, got %v", f)
	}
}
