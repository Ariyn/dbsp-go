package ir

// Logical IR for a very small subset of relational plans.

// LogicalNode is the interface for logical plan nodes.
type LogicalNode interface {
	nodeName() string
}

// LogicalScan represents scanning a single table (no predicates for now).
type LogicalScan struct {
	Table string
}

func (s *LogicalScan) nodeName() string { return "LogicalScan" }

// LogicalFilter represents a filter (WHERE clause) over input.
type LogicalFilter struct {
	// PredicateSQL is the SQL WHERE condition (e.g., "status = 'active'")
	PredicateSQL string
	// Input is the child logical node
	Input LogicalNode
}

func (f *LogicalFilter) nodeName() string { return "LogicalFilter" }

// LogicalProject represents column projection (SELECT specific columns).
type LogicalProject struct {
	// Columns to project (column names)
	Columns []string
	// Input is the child logical node
	Input LogicalNode
}

func (p *LogicalProject) nodeName() string { return "LogicalProject" }

// WindowSpec describes a simple tumbling window over a time column.
// For now we keep it minimal: a time column name and fixed window size
// in milliseconds. Nil WindowSpec means "no windowing".
type WindowSpec struct {
	// TimeCol is the column that contains the event time.
	TimeCol string
	// SizeMillis is the window size in milliseconds.
	SizeMillis int64
}

// LogicalGroupAgg represents grouping and aggregation over input.
type LogicalGroupAgg struct {
	// Group keys (column names)
	Keys []string
	// AggName: SUM, COUNT
	AggName string
	// AggCol: column to aggregate (for SUM); empty for COUNT(*)
	AggCol string
	// WindowSpec is optional window metadata. If nil, this is a normal
	// non-windowed group-by. If non-nil, the engine will treat this as
	// a tumbling-window aggregation over TimeCol with the given size.
	WindowSpec *WindowSpec
	// Input is the child logical node
	Input LogicalNode
}

func (g *LogicalGroupAgg) nodeName() string { return "LogicalGroupAgg" }
