package sink

import (
	"fmt"

	"github.com/apache/arrow/go/v15/arrow"
	"github.com/apache/arrow/go/v15/arrow/array"
	"github.com/ariyn/dbsp/cmd/dbsp/config"
	"github.com/ariyn/dbsp/internal/dbsp/types"
)

func AppendTupleDeltasToArrowBuilders(schema *arrow.Schema, builders []array.Builder, batch types.Batch) error {
	for _, td := range batch {
		for i, f := range schema.Fields() {
			val, ok := td.Tuple[f.Name]
			if !ok || val == nil {
				builders[i].AppendNull()
				continue
			}

			switch f.Type.ID() {
			case arrow.INT64:
				iv, _ := types.ToInt64Safe(val)
				builders[i].(*array.Int64Builder).Append(iv)
			case arrow.FLOAT64:
				fv, _ := types.ToFloat64Safe(val)
				builders[i].(*array.Float64Builder).Append(fv)
			case arrow.STRING:
				builders[i].(*array.StringBuilder).Append(fmt.Sprintf("%v", val))
			default:
				builders[i].(*array.StringBuilder).Append(fmt.Sprintf("%v", val))
			}
		}
	}
	return nil
}

func BuildArrowSchema(ps *config.ParquetSchema) *arrow.Schema {
	fields := make([]arrow.Field, 0, len(ps.Columns))
	for _, c := range ps.Columns {
		var dt arrow.DataType
		switch c.Type {
		case "int64":
			dt = arrow.PrimitiveTypes.Int64
		case "float64":
			dt = arrow.PrimitiveTypes.Float64
		case "string":
			dt = arrow.BinaryTypes.String
		default:
			dt = arrow.BinaryTypes.String
		}
		fields = append(fields, arrow.Field{Name: c.Name, Type: dt, Nullable: true})
	}
	return arrow.NewSchema(fields, nil)
}
