package op

import (
	"testing"

	"github.com/ariyn/dbsp/internal/dbsp/types"
)

func TestProjectOpKeepsSimplePackedProjection(t *testing.T) {
	op := &ProjectOp{Columns: []string{"b"}}
	schema := types.NewPackedSchema([]string{"a", "b"})
	batch := types.Batch{{
		Packed: types.NewPackedTupleWithPresence(schema, []any{int64(1), nil}, []bool{true, true}),
		Count:  1,
	}}

	out, err := op.Apply(batch)
	if err != nil {
		t.Fatalf("apply failed: %v", err)
	}
	if len(out) != 1 {
		t.Fatalf("expected 1 output row, got %d", len(out))
	}
	if out[0].Packed == nil {
		t.Fatalf("expected packed output, got %+v", out[0])
	}
	if value, ok := out[0].Packed.Get("b"); !ok || value != nil {
		t.Fatalf("expected projected column to remain present null, got value=%v ok=%v", value, ok)
	}
	if _, ok := out[0].Packed.Get("a"); ok {
		t.Fatalf("expected dropped column to be absent, got %+v", out[0].Packed.Materialize())
	}
}

func TestProjectOpKeepsPackedBaseForComputedExpressions(t *testing.T) {
	op := &ProjectOp{
		Columns: []string{"a"},
		Exprs: []ProjectExprFn{{
			OutCol: "sum",
			Eval: func(tuple types.Tuple) (any, error) {
				return types.ToInt64(tuple["a"]) + types.ToInt64(tuple["b"]), nil
			},
		}},
	}
	schema := types.NewPackedSchema([]string{"a", "b"})
	batch := types.Batch{{
		Packed: types.NewPackedTupleWithPresence(schema, []any{int64(2), int64(3)}, []bool{true, true}),
		Count:  1,
	}}

	out, err := op.Apply(batch)
	if err != nil {
		t.Fatalf("apply failed: %v", err)
	}
	if len(out) != 1 {
		t.Fatalf("expected 1 output row, got %d", len(out))
	}
	if out[0].Packed == nil {
		t.Fatalf("expected packed output for computed projection, got %+v", out[0])
	}
	if _, ok := out[0].Packed.Get("b"); ok {
		t.Fatalf("expected dropped column to stay absent, got %+v", out[0].Packed.Materialize())
	}
	if got, ok := out[0].Packed.Get("sum"); !ok || types.ToInt64(got) != 5 {
		t.Fatalf("expected computed sum=5, got value=%v ok=%v", got, ok)
	}
	if got, ok := out[0].Packed.Get("a"); !ok || types.ToInt64(got) != 2 {
		t.Fatalf("expected projected base column a=2, got value=%v ok=%v", got, ok)
	}
}

func TestProjectOpKeepInputWithComputedExpressionsKeepsPackedBase(t *testing.T) {
	op := &ProjectOp{
		KeepInput: true,
		Exprs: []ProjectExprFn{{
			OutCol: "sum",
			Eval: func(tuple types.Tuple) (any, error) {
				return types.ToInt64(tuple["a"]) + types.ToInt64(tuple["b"]), nil
			},
		}},
	}
	schema := types.NewPackedSchema([]string{"a", "b"})
	batch := types.Batch{{
		Packed: types.NewPackedTupleWithPresence(schema, []any{int64(2), int64(3)}, []bool{true, true}),
		Count:  1,
	}}

	out, err := op.Apply(batch)
	if err != nil {
		t.Fatalf("apply failed: %v", err)
	}
	if len(out) != 1 {
		t.Fatalf("expected 1 output row, got %d", len(out))
	}
	if out[0].Packed == nil {
		t.Fatalf("expected packed output, got %+v", out[0])
	}
	if got, ok := out[0].Packed.Get("a"); !ok || types.ToInt64(got) != 2 {
		t.Fatalf("expected base column a=2, got value=%v ok=%v", got, ok)
	}
	if got, ok := out[0].Packed.Get("b"); !ok || types.ToInt64(got) != 3 {
		t.Fatalf("expected base column b=3, got value=%v ok=%v", got, ok)
	}
	if got, ok := out[0].Packed.Get("sum"); !ok || types.ToInt64(got) != 5 {
		t.Fatalf("expected computed sum=5, got value=%v ok=%v", got, ok)
	}
}
