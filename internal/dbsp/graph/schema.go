package graph

import (
	"sort"
	"strings"

	"github.com/ariyn/dbsp/internal/dbsp/ir"
	"github.com/ariyn/dbsp/internal/dbsp/op"
)

// SchemaField describes a column and its best-effort type.
type SchemaField struct {
	Name string
	Type string
}

// Schema is an ordered list of fields.
type Schema []SchemaField

func schemaFromMap(input map[string]string) Schema {
	if len(input) == 0 {
		return nil
	}
	keys := make([]string, 0, len(input))
	for k := range input {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := make(Schema, 0, len(keys))
	for _, k := range keys {
		out = append(out, SchemaField{Name: k, Type: input[k]})
	}
	return out
}

func schemaFieldMap(schema Schema) map[string]string {
	out := make(map[string]string, len(schema))
	for _, f := range schema {
		out[f.Name] = f.Type
	}
	return out
}

func formatSchema(schema Schema) string {
	if len(schema) == 0 {
		return ""
	}
	parts := make([]string, 0, len(schema))
	for _, f := range schema {
		if strings.TrimSpace(f.Type) == "" {
			parts = append(parts, f.Name)
			continue
		}
		parts = append(parts, f.Name+":"+f.Type)
	}
	return strings.Join(parts, ", ")
}

func addFieldOrdered(out Schema, seen map[string]struct{}, name, typ string) Schema {
	if name == "" {
		return out
	}
	if _, ok := seen[name]; ok {
		return out
	}
	seen[name] = struct{}{}
	out = append(out, SchemaField{Name: name, Type: typ})
	return out
}

func copySchema(schema Schema) Schema {
	if len(schema) == 0 {
		return nil
	}
	out := make(Schema, len(schema))
	copy(out, schema)
	return out
}

func mergeSchemas(left, right Schema) Schema {
	out := make(Schema, 0, len(left)+len(right))
	seen := make(map[string]struct{}, len(left)+len(right))
	for _, f := range left {
		out = addFieldOrdered(out, seen, f.Name, f.Type)
	}
	for _, f := range right {
		name := f.Name
		if _, ok := seen[name]; ok {
			name = name + "__right"
		}
		out = addFieldOrdered(out, seen, name, f.Type)
	}
	return out
}

func inferAggTypeByName(name, inputType string) string {
	switch strings.ToUpper(strings.TrimSpace(name)) {
	case "SUM", "AVG":
		return "float64"
	case "COUNT":
		return "int64"
	case "MIN", "MAX":
		if inputType != "" {
			return inputType
		}
		return "unknown"
	default:
		return "unknown"
	}
}

func inferAggTypeByFunc(agg op.AggFunc, inputType string) string {
	switch agg.(type) {
	case *op.SumAgg, *op.AvgAgg:
		return "float64"
	case *op.CountAgg:
		return "int64"
	case *op.MinAgg, *op.MaxAgg:
		if inputType != "" {
			return inputType
		}
		return "unknown"
	default:
		return "unknown"
	}
}

func aggOutputNameForFunc(agg op.AggFunc) string {
	switch a := agg.(type) {
	case *op.SumAgg:
		if strings.TrimSpace(a.DeltaCol) != "" {
			return a.DeltaCol
		}
		return "agg_delta"
	case *op.AvgAgg:
		if strings.TrimSpace(a.DeltaCol) != "" {
			return a.DeltaCol
		}
		return "avg_delta"
	case *op.CountAgg:
		if strings.TrimSpace(a.DeltaCol) != "" {
			return a.DeltaCol
		}
		return "count_delta"
	case *op.MinAgg:
		return "min"
	case *op.MaxAgg:
		return "max"
	default:
		return "agg_delta"
	}
}

func aggInputTypeForFunc(agg op.AggFunc, input map[string]string) string {
	switch a := agg.(type) {
	case *op.SumAgg:
		return input[a.ColName]
	case *op.AvgAgg:
		return input[a.ColName]
	case *op.CountAgg:
		return input[a.ColName]
	case *op.MinAgg:
		return input[a.ColName]
	case *op.MaxAgg:
		return input[a.ColName]
	default:
		return ""
	}
}

// InferLogicalSchemas returns a best-effort output schema per logical node.
func InferLogicalSchemas(root ir.LogicalNode, input map[string]string) map[ir.LogicalNode]Schema {
	infer := &logicalSchemaInferer{
		cache:       make(map[ir.LogicalNode]Schema),
		cteSchemas:  make(map[string]Schema),
		inputSchema: schemaFromMap(input),
	}
	_ = infer.infer(root)
	return infer.cache
}

type logicalSchemaInferer struct {
	cache       map[ir.LogicalNode]Schema
	cteSchemas  map[string]Schema
	inputSchema Schema
}

func (i *logicalSchemaInferer) infer(n ir.LogicalNode) Schema {
	if n == nil {
		return nil
	}
	if cached, ok := i.cache[n]; ok {
		return cached
	}

	var out Schema
	switch t := n.(type) {
	case *ir.LogicalScan:
		out = copySchema(i.inputSchema)
	case *ir.LogicalCTERef:
		out = copySchema(i.cteSchemas[t.CTEName])
	case *ir.LogicalFilter:
		out = i.infer(t.Input)
	case *ir.LogicalProject:
		inSchema := i.infer(t.Input)
		inMap := schemaFieldMap(inSchema)
		seen := make(map[string]struct{})
		if t.KeepInput {
			for _, f := range inSchema {
				out = addFieldOrdered(out, seen, f.Name, f.Type)
			}
		}
		for _, col := range t.Columns {
			out = addFieldOrdered(out, seen, col, inMap[col])
		}
		for _, expr := range t.Exprs {
			out = addFieldOrdered(out, seen, expr.As, "unknown")
		}
	case *ir.LogicalGroupAgg:
		inSchema := i.infer(t.Input)
		inMap := schemaFieldMap(inSchema)
		seen := make(map[string]struct{})
		for _, key := range t.Keys {
			out = addFieldOrdered(out, seen, key, inMap[key])
		}
		if len(t.Aggs) > 0 {
			for _, agg := range t.Aggs {
				name := strings.TrimSpace(agg.As)
				if name == "" {
					base := strings.ToLower(strings.TrimSpace(agg.Name))
					if strings.TrimSpace(agg.Col) != "" {
						base = base + "_" + strings.ReplaceAll(strings.TrimSpace(agg.Col), " ", "")
					}
					name = base
				}
				aggType := inferAggTypeByName(agg.Name, inMap[agg.Col])
				out = addFieldOrdered(out, seen, name, aggType)
			}
		} else if t.AggName != "" {
			name := strings.ToLower(strings.TrimSpace(t.AggName))
			if strings.TrimSpace(t.AggCol) != "" {
				name = name + "_" + strings.ReplaceAll(strings.TrimSpace(t.AggCol), " ", "")
			}
			aggType := inferAggTypeByName(t.AggName, inMap[t.AggCol])
			out = addFieldOrdered(out, seen, name, aggType)
		}
	case *ir.LogicalWindowFunc:
		inSchema := i.infer(t.Input)
		inMap := schemaFieldMap(inSchema)
		out = copySchema(inSchema)
		seen := make(map[string]struct{})
		for _, f := range out {
			seen[f.Name] = struct{}{}
		}
		argType := "unknown"
		if len(t.Spec.Args) > 0 {
			argType = inMap[t.Spec.Args[0]]
		}
		out = addFieldOrdered(out, seen, t.OutputCol, argType)
	case *ir.LogicalWindowAgg:
		inSchema := i.infer(t.Input)
		inMap := schemaFieldMap(inSchema)
		out = copySchema(inSchema)
		seen := make(map[string]struct{})
		for _, f := range out {
			seen[f.Name] = struct{}{}
		}
		aggType := inferAggTypeByName(t.AggName, inMap[t.AggCol])
		out = addFieldOrdered(out, seen, t.OutputCol, aggType)
	case *ir.LogicalJoin:
		left := i.infer(t.Left)
		right := i.infer(t.Right)
		out = mergeSchemas(left, right)
	case *ir.LogicalSort:
		out = i.infer(t.Input)
	case *ir.LogicalView:
		out = i.infer(t.Input)
	case *ir.LogicalLimit:
		out = i.infer(t.Input)
	case *ir.LogicalWith:
		for _, name := range t.CTENames {
			if cte, ok := t.CTEs[name]; ok {
				i.cteSchemas[name] = i.infer(cte)
			}
		}
		out = i.infer(t.Body)
	default:
		out = nil
	}

	i.cache[n] = out
	return out
}

// InferOperatorSchemas returns a best-effort output schema per operator node.
func InferOperatorSchemas(root *op.Node, input map[string]string) map[*op.Node]Schema {
	infer := &operatorSchemaInferer{
		cache:       make(map[*op.Node]Schema),
		inputSchema: schemaFromMap(input),
	}
	_ = infer.infer(root)
	return infer.cache
}

type operatorSchemaInferer struct {
	cache       map[*op.Node]Schema
	inputSchema Schema
}

func (i *operatorSchemaInferer) infer(n *op.Node) Schema {
	if n == nil {
		return nil
	}
	if cached, ok := i.cache[n]; ok {
		return cached
	}

	var out Schema
	if n.Source != "" {
		out = copySchema(i.inputSchema)
		i.cache[n] = out
		return out
	}

	var inSchema Schema
	if len(n.Inputs) > 0 {
		inSchema = i.infer(n.Inputs[0])
	}

	if len(n.Inputs) == 2 {
		right := i.infer(n.Inputs[1])
		switch t := n.Op.(type) {
		case *op.BinaryOp:
			if t.Type == op.BinaryJoin {
				out = mergeSchemas(inSchema, right)
			} else {
				out = inSchema
			}
		case *op.ExplicitJoinOp:
			out = mergeSchemas(inSchema, right)
		default:
			out = inSchema
		}
	} else {
		out = applyOperatorSchema(n.Op, inSchema)
	}

	i.cache[n] = out
	return out
}

func applyOperatorSchema(opv op.Operator, inSchema Schema) Schema {
	if opv == nil {
		return inSchema
	}
	switch t := opv.(type) {
	case *op.ProjectOp:
		inMap := schemaFieldMap(inSchema)
		seen := make(map[string]struct{})
		var out Schema
		if t.KeepInput {
			for _, f := range inSchema {
				out = addFieldOrdered(out, seen, f.Name, f.Type)
			}
		}
		for _, col := range t.Columns {
			out = addFieldOrdered(out, seen, col, inMap[col])
		}
		for _, expr := range t.Exprs {
			out = addFieldOrdered(out, seen, expr.OutCol, "unknown")
		}
		return out
	case *op.GroupAggOp:
		inMap := schemaFieldMap(inSchema)
		seen := make(map[string]struct{})
		var out Schema
		if len(t.GroupKeyColNames) > 0 {
			for _, key := range t.GroupKeyColNames {
				out = addFieldOrdered(out, seen, key, inMap[key])
			}
		} else if t.KeyColName != "" {
			out = addFieldOrdered(out, seen, t.KeyColName, inMap[t.KeyColName])
		}
		if len(t.Aggs) > 0 {
			for _, agg := range t.Aggs {
				name := aggOutputNameForFunc(agg.Fn)
				inputType := aggInputTypeForFunc(agg.Fn, inMap)
				aggType := inferAggTypeByFunc(agg.Fn, inputType)
				out = addFieldOrdered(out, seen, name, aggType)
			}
		} else if t.AggFn != nil {
			name := aggOutputNameForFunc(t.AggFn)
			inputType := aggInputTypeForFunc(t.AggFn, inMap)
			aggType := inferAggTypeByFunc(t.AggFn, inputType)
			out = addFieldOrdered(out, seen, name, aggType)
		}
		return out
	case *op.WindowAggOp:
		inMap := schemaFieldMap(inSchema)
		seen := make(map[string]struct{})
		var out Schema
		out = addFieldOrdered(out, seen, "__window_start", "int64")
		out = addFieldOrdered(out, seen, "__window_end", "int64")
		for _, key := range t.GroupKeys {
			out = addFieldOrdered(out, seen, key, inMap[key])
		}
		if t.KeepInput {
			for _, f := range inSchema {
				out = addFieldOrdered(out, seen, f.Name, f.Type)
			}
		}
		name := "agg_result"
		inputType := aggInputTypeForFunc(t.AggFn, inMap)
		aggType := inferAggTypeByFunc(t.AggFn, inputType)
		if _, isMin := t.AggFn.(*op.MinAgg); isMin {
			name = "min"
		}
		if _, isMax := t.AggFn.(*op.MaxAgg); isMax {
			name = "max"
		}
		out = addFieldOrdered(out, seen, name, aggType)
		return out
	case *op.SortOp, *op.LimitOp, *op.TopKOp, *op.IntegrateOp, *op.DelayOp, *op.MapOp:
		return inSchema
	case *op.ChainedOp:
		out := inSchema
		for _, child := range t.Ops {
			out = applyOperatorSchema(child, out)
		}
		return out
	case *op.BinaryOp:
		if t.Type == op.BinaryJoin {
			return inSchema
		}
		return inSchema
	case *op.ExplicitJoinOp:
		return inSchema
	default:
		return inSchema
	}
}
