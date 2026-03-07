package sink

import (
	"testing"

	"github.com/apache/arrow/go/v15/arrow"
	"github.com/apache/arrow/go/v15/arrow/array"
	"github.com/apache/arrow/go/v15/arrow/memory"
	"github.com/ariyn/dbsp/internal/dbsp/types"
)

func TestAppendTupleDeltasToArrowBuildersSupportsPackedRows(t *testing.T) {
	allocator := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "energy", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
		{Name: "temp", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)
	builders := []array.Builder{
		array.NewStringBuilder(allocator),
		array.NewFloat64Builder(allocator),
		array.NewStringBuilder(allocator),
	}
	defer func() {
		for _, builder := range builders {
			builder.Release()
		}
	}()

	packedSchema := types.NewPackedSchema([]string{"id", "energy"})
	batch := types.Batch{{
		Packed: types.NewPackedTupleWithPresence(packedSchema, []any{"panel-a", 12.5}, []bool{true, true}).WithExtra("temp", "25.5"),
		Count:  1,
	}}

	if err := AppendTupleDeltasToArrowBuilders(schema, builders, batch); err != nil {
		t.Fatalf("AppendTupleDeltasToArrowBuilders: %v", err)
	}

	idArr := builders[0].NewArray().(*array.String)
	defer idArr.Release()
	energyArr := builders[1].NewArray().(*array.Float64)
	defer energyArr.Release()
	tempArr := builders[2].NewArray().(*array.String)
	defer tempArr.Release()

	if got := idArr.Value(0); got != "panel-a" {
		t.Fatalf("expected id=panel-a, got %q", got)
	}
	if got := energyArr.Value(0); got != 12.5 {
		t.Fatalf("expected energy=12.5, got %v", got)
	}
	if got := tempArr.Value(0); got != "25.5" {
		t.Fatalf("expected temp=25.5, got %q", got)
	}
}