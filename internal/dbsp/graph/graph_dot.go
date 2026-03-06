package graph

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/ariyn/dbsp/internal/dbsp/ir"
	"github.com/ariyn/dbsp/internal/dbsp/op"
)

// Options controls DOT output details.
type Options struct {
	Verbose bool
}

// LogicalPlanDOT renders a logical plan as Graphviz DOT.
func LogicalPlanDOT(root ir.LogicalNode, opts Options) string {
	b := &logicalDotBuilder{
		ids:   make(map[ir.LogicalNode]int),
		edges: make(map[string]struct{}),
		opts:  opts,
	}
	b.buf.WriteString("digraph LogicalPlan {\n")
	b.buf.WriteString("  rankdir=LR;\n")
	b.buf.WriteString("  node [shape=box];\n")
	if root != nil {
		b.walk(root)
	}
	b.buf.WriteString("}\n")
	return b.buf.String()
}

// OperatorGraphDOT renders an operator DAG as Graphviz DOT.
func OperatorGraphDOT(root *op.Node, opts Options) string {
	b := &operatorDotBuilder{
		ids:   make(map[*op.Node]int),
		edges: make(map[string]struct{}),
		opts:  opts,
	}
	b.buf.WriteString("digraph OperatorGraph {\n")
	b.buf.WriteString("  rankdir=LR;\n")
	b.buf.WriteString("  node [shape=box];\n")
	if root != nil {
		b.walk(root)
	}
	b.buf.WriteString("}\n")
	return b.buf.String()
}

type logicalDotBuilder struct {
	buf   bytes.Buffer
	next  int
	ids   map[ir.LogicalNode]int
	edges map[string]struct{}
	opts  Options
}

func (b *logicalDotBuilder) walk(n ir.LogicalNode) int {
	if n == nil {
		return -1
	}
	if id, ok := b.ids[n]; ok {
		return id
	}
	id := b.next
	b.next++
	b.ids[n] = id

	label := b.logicalLabel(n)
	b.buf.WriteString(fmt.Sprintf("  n%d [label=\"%s\"];\n", id, dotEscape(label)))

	for _, child := range b.logicalChildren(n) {
		childID := b.walk(child)
		if childID >= 0 {
			b.emitEdge(childID, id)
		}
	}
	return id
}

func (b *logicalDotBuilder) emitEdge(from, to int) {
	key := fmt.Sprintf("%d->%d", from, to)
	if _, ok := b.edges[key]; ok {
		return
	}
	b.edges[key] = struct{}{}
	b.buf.WriteString(fmt.Sprintf("  n%d -> n%d;\n", from, to))
}

func (b *logicalDotBuilder) logicalLabel(n ir.LogicalNode) string {
	switch t := n.(type) {
	case *ir.LogicalScan:
		if b.opts.Verbose {
			return fmt.Sprintf("%s\nTable: %s", logicalTypeName(t), t.Table)
		}
		return fmt.Sprintf("%s\n%s", logicalTypeName(t), t.Table)
	case *ir.LogicalCTERef:
		return fmt.Sprintf("%s\n%s", logicalTypeName(t), t.CTEName)
	case *ir.LogicalFilter:
		if b.opts.Verbose {
			return fmt.Sprintf("%s\nPredicate: %s", logicalTypeName(t), t.PredicateSQL)
		}
	case *ir.LogicalProject:
		if b.opts.Verbose {
			cols := strings.Join(t.Columns, ", ")
			exprs := make([]string, 0, len(t.Exprs))
			for _, e := range t.Exprs {
				if e.As != "" {
					exprs = append(exprs, e.As)
				}
			}
			parts := []string{}
			if cols != "" {
				parts = append(parts, "Columns: "+cols)
			}
			if len(exprs) > 0 {
				parts = append(parts, "Exprs: "+strings.Join(exprs, ", "))
			}
			if len(parts) > 0 {
				return fmt.Sprintf("%s\n%s", logicalTypeName(t), strings.Join(parts, "\n"))
			}
		}
	case *ir.LogicalGroupAgg:
		if b.opts.Verbose {
			keys := strings.Join(t.Keys, ", ")
			agg := t.AggName
			if agg == "" && len(t.Aggs) > 0 {
				agg = t.Aggs[0].Name
			}
			parts := []string{}
			if keys != "" {
				parts = append(parts, "Keys: "+keys)
			}
			if agg != "" {
				parts = append(parts, "Agg: "+agg)
			}
			if len(parts) > 0 {
				return fmt.Sprintf("%s\n%s", logicalTypeName(t), strings.Join(parts, "\n"))
			}
		}
	case *ir.LogicalWindowFunc:
		if b.opts.Verbose {
			parts := []string{fmt.Sprintf("Func: %s", t.Spec.FuncName)}
			if len(t.Spec.PartitionBy) > 0 {
				parts = append(parts, "PartitionBy: "+strings.Join(t.Spec.PartitionBy, ", "))
			}
			if t.Spec.OrderBy != "" {
				parts = append(parts, "OrderBy: "+t.Spec.OrderBy)
			}
			return fmt.Sprintf("%s\n%s", logicalTypeName(t), strings.Join(parts, "\n"))
		}
	case *ir.LogicalWindowAgg:
		if b.opts.Verbose {
			parts := []string{fmt.Sprintf("Agg: %s", t.AggName)}
			if len(t.PartitionBy) > 0 {
				parts = append(parts, "PartitionBy: "+strings.Join(t.PartitionBy, ", "))
			}
			if t.OrderBy != "" {
				parts = append(parts, "OrderBy: "+t.OrderBy)
			}
			return fmt.Sprintf("%s\n%s", logicalTypeName(t), strings.Join(parts, "\n"))
		}
	case *ir.LogicalJoin:
		if b.opts.Verbose {
			return fmt.Sprintf("%s\n%s x %s", logicalTypeName(t), t.LeftTable, t.RightTable)
		}
	case *ir.LogicalSort:
		if b.opts.Verbose {
			return fmt.Sprintf("%s\nBy: %s", logicalTypeName(t), strings.Join(t.OrderColumns, ", "))
		}
	case *ir.LogicalView:
		if b.opts.Verbose {
			parts := []string{"Name: " + t.Name}
			if len(t.PartitionBy) > 0 {
				parts = append(parts, "PartitionBy: "+strings.Join(t.PartitionBy, ", "))
			}
			return fmt.Sprintf("%s\n%s", logicalTypeName(t), strings.Join(parts, "\n"))
		}
	case *ir.LogicalLimit:
		if b.opts.Verbose {
			return fmt.Sprintf("%s\nLimit: %d\nOffset: %d", logicalTypeName(t), t.Limit, t.Offset)
		}
	case *ir.LogicalWith:
		if b.opts.Verbose {
			return fmt.Sprintf("%s\nCTEs: %s", logicalTypeName(t), strings.Join(t.CTENames, ", "))
		}
	}
	return logicalTypeName(n)
}

func (b *logicalDotBuilder) logicalChildren(n ir.LogicalNode) []ir.LogicalNode {
	switch t := n.(type) {
	case *ir.LogicalFilter:
		return []ir.LogicalNode{t.Input}
	case *ir.LogicalProject:
		return []ir.LogicalNode{t.Input}
	case *ir.LogicalGroupAgg:
		return []ir.LogicalNode{t.Input}
	case *ir.LogicalWindowFunc:
		return []ir.LogicalNode{t.Input}
	case *ir.LogicalWindowAgg:
		return []ir.LogicalNode{t.Input}
	case *ir.LogicalJoin:
		return []ir.LogicalNode{t.Left, t.Right}
	case *ir.LogicalSort:
		return []ir.LogicalNode{t.Input}
	case *ir.LogicalView:
		return []ir.LogicalNode{t.Input}
	case *ir.LogicalLimit:
		return []ir.LogicalNode{t.Input}
	case *ir.LogicalWith:
		children := make([]ir.LogicalNode, 0, len(t.CTENames)+1)
		for _, name := range t.CTENames {
			if cte, ok := t.CTEs[name]; ok {
				children = append(children, cte)
			}
		}
		children = append(children, t.Body)
		return children
	}
	return nil
}

type operatorDotBuilder struct {
	buf   bytes.Buffer
	next  int
	ids   map[*op.Node]int
	edges map[string]struct{}
	opts  Options
}

func (b *operatorDotBuilder) walk(n *op.Node) int {
	if n == nil {
		return -1
	}
	if id, ok := b.ids[n]; ok {
		return id
	}
	id := b.next
	b.next++
	b.ids[n] = id

	label := b.operatorLabel(n)
	b.buf.WriteString(fmt.Sprintf("  n%d [label=\"%s\"];\n", id, dotEscape(label)))

	for _, child := range n.Inputs {
		childID := b.walk(child)
		if childID >= 0 {
			b.emitEdge(childID, id)
		}
	}
	return id
}

func (b *operatorDotBuilder) emitEdge(from, to int) {
	key := fmt.Sprintf("%d->%d", from, to)
	if _, ok := b.edges[key]; ok {
		return
	}
	b.edges[key] = struct{}{}
	b.buf.WriteString(fmt.Sprintf("  n%d -> n%d;\n", from, to))
}

func (b *operatorDotBuilder) operatorLabel(n *op.Node) string {
	lines := []string{opTypeName(n.Op)}
	if n.Source != "" {
		lines = append(lines, "Source: "+n.Source)
	}
	if len(n.PartitionBy) > 0 {
		lines = append(lines, "PartitionBy: "+strings.Join(n.PartitionBy, ", "))
	}
	if b.opts.Verbose {
		if chained, ok := n.Op.(*op.ChainedOp); ok {
			parts := make([]string, 0, len(chained.Ops))
			for _, child := range chained.Ops {
				parts = append(parts, opTypeName(child))
			}
			if len(parts) > 0 {
				lines = append(lines, "Ops: "+strings.Join(parts, " -> "))
			}
		}
	}
	return strings.Join(lines, "\n")
}

func opTypeName(opv op.Operator) string {
	if opv == nil {
		return "<nil>"
	}
	t := fmt.Sprintf("%T", opv)
	t = strings.TrimPrefix(t, "*")
	if idx := strings.LastIndex(t, "/"); idx >= 0 {
		t = t[idx+1:]
	}
	return t
}

func logicalTypeName(n ir.LogicalNode) string {
	if n == nil {
		return "<nil>"
	}
	t := fmt.Sprintf("%T", n)
	t = strings.TrimPrefix(t, "*")
	if idx := strings.LastIndex(t, "/"); idx >= 0 {
		t = t[idx+1:]
	}
	return t
}

func dotEscape(s string) string {
	replacer := strings.NewReplacer(
		"\\", "\\\\",
		"\"", "\\\"",
		"\n", "\\n",
	)
	return replacer.Replace(s)
}
