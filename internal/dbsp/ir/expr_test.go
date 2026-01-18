package ir

import (
	"testing"

	"github.com/ariyn/dbsp/internal/dbsp/types"
)

func TestBuildExprFunc_ArithmeticAndCast(t *testing.T) {
	tup := types.Tuple{"a": int64(3)}

	f := BuildExprFunc("a + 2")
	v, err := f(tup)
	if err != nil {
		t.Fatalf("eval: %v", err)
	}
	if v.(float64) != 5 {
		t.Fatalf("expected 5, got %v", v)
	}

	f = BuildExprFunc("CAST(a AS BIGINT)")
	v, err = f(tup)
	if err != nil {
		t.Fatalf("eval cast: %v", err)
	}
	if v.(int64) != 3 {
		t.Fatalf("expected 3, got %v", v)
	}
}

func TestBuildExprFunc_CaseWhen(t *testing.T) {
	tup := types.Tuple{"a": int64(3)}
	f := BuildExprFunc("CASE WHEN a > 0 THEN 1 ELSE 0 END")
	v, err := f(tup)
	if err != nil {
		t.Fatalf("eval: %v", err)
	}
	if v.(int64) != 1 {
		t.Fatalf("expected 1, got %v", v)
	}
}
