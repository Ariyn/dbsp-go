package sink

import (
	"fmt"

	"github.com/apache/arrow/go/v15/arrow"
	"github.com/apache/arrow/go/v15/arrow/array"
	"github.com/ariyn/dbsp/cmd/dbsp/config"
	"github.com/ariyn/dbsp/cmd/dbsp/provider"
	"github.com/ariyn/dbsp/internal/dbsp/types"
)

func AppendTupleDeltasToArrowBuilders(schema *arrow.Schema, builders []array.Builder, batch types.Batch) error {
	for _, td := range batch {
		for i, f := range schema.Fields() {
			if f.Name == "__count" {
				builders[i].(*array.Int64Builder).Append(td.Count)
				continue
			}

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

func BuildArrowSchema(ps *config.ParquetSchema, includeCount bool) *arrow.Schema {
	fields := make([]arrow.Field, 0, len(ps.Columns)+1)
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
	if includeCount {
		// Always include __count for Z-set multiplicity
		fields = append(fields, arrow.Field{Name: "__count", Type: arrow.PrimitiveTypes.Int64, Nullable: false})
	}

	return arrow.NewSchema(fields, nil)
}

// NoopCloseSink delegates writes while preventing shared sinks from being closed multiple times.
type NoopCloseSink struct {
	inner provider.Sink
}

func NewNoopCloseSink(inner provider.Sink) *NoopCloseSink {
	return &NoopCloseSink{inner: inner}
}

func (s *NoopCloseSink) WriteBatch(batch types.Batch) error {
	return s.inner.WriteBatch(batch)
}

func (s *NoopCloseSink) ReplayWriteBatch(batch types.Batch) error {
	if rs, ok := s.inner.(provider.ReplaySink); ok {
		return rs.ReplayWriteBatch(batch)
	}
	return s.inner.WriteBatch(batch)
}

func (s *NoopCloseSink) WriteBatchWithPartition(batch types.Batch, values map[string]string) error {
	if ps, ok := s.inner.(PartitionedSink); ok {
		return ps.WriteBatchWithPartition(batch, values)
	}
	return s.inner.WriteBatch(batch)
}

func (s *NoopCloseSink) Close() error {
	return nil
}
