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
	// Exprs are computed expressions in SELECT list (require alias)
	Exprs []ProjectExpr
	// Input is the child logical node
	Input LogicalNode
}

func (p *LogicalProject) nodeName() string { return "LogicalProject" }

// ProjectExpr represents a computed SELECT expression.
// ExprSQL is a small SQL expression subset (e.g., CAST, arithmetic, CASE WHEN).
// As is the output column name.
type ProjectExpr struct {
	ExprSQL string
	As      string
}

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
	// Aggs is the preferred representation for one or more aggregate functions.
	// When empty, the legacy AggName/AggCol fields may be used.
	Aggs []AggSpec
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

// AggSpec describes a single aggregate call in a GROUP BY.
//
// Example: SUM(v) -> {Name:"SUM", Col:"v"}
// COUNT(id) -> {Name:"COUNT", Col:"id"}
//
// NOTE: COUNT(*) is represented as Col=="*" in some parsing paths, but
// multi-aggregate support may restrict it depending on higher-level policy.
type AggSpec struct {
	Name string
	Col  string
}

// WindowFuncSpec describes a window function specification
type WindowFuncSpec struct {
	// Function name: LAG, LEAD, ROW_NUMBER, etc.
	FuncName string
	// Arguments to the function (e.g., column name for LAG)
	Args []string
	// PARTITION BY columns
	PartitionBy []string
	// ORDER BY column
	OrderBy string
	// For LAG/LEAD: offset (default 1)
	Offset int
}

// LogicalWindowFunc represents a window function application
type LogicalWindowFunc struct {
	// WindowSpec describes the window function
	Spec WindowFuncSpec
	// OutputCol is the name of the output column
	OutputCol string
	// Input is the child logical node
	Input LogicalNode
}

func (w *LogicalWindowFunc) nodeName() string { return "LogicalWindowFunc" }

// FrameSpec describes a window frame specification
type FrameSpec struct {
	// Type: ROWS, RANGE, or GROUPS
	Type string
	// StartType: UNBOUNDED PRECEDING, CURRENT ROW, <value> PRECEDING/FOLLOWING
	StartType string
	// StartValue: numeric value for <value> PRECEDING/FOLLOWING
	StartValue string
	// EndType: UNBOUNDED FOLLOWING, CURRENT ROW, <value> PRECEDING/FOLLOWING
	EndType string
	// EndValue: numeric value for <value> PRECEDING/FOLLOWING
	EndValue string
}

// TimeWindowSpec describes time-based windowing (Tumbling/Sliding/Session)
type TimeWindowSpec struct {
	// WindowType: TUMBLING, SLIDING, or SESSION
	WindowType string
	// TimeCol is the timestamp column
	TimeCol string
	// SizeMillis is the window size in milliseconds
	SizeMillis int64
	// SlideMillis is the slide size for sliding windows (0 for tumbling)
	SlideMillis int64
	// GapMillis is the inactivity gap for session windows
	GapMillis int64
}

// LogicalWindowAgg represents a window aggregate function
type LogicalWindowAgg struct {
	// AggName: SUM, AVG, COUNT, MIN, MAX
	AggName string
	// AggCol: column to aggregate
	AggCol string
	// PartitionBy columns
	PartitionBy []string
	// OrderBy column
	OrderBy string
	// FrameSpec describes the window frame (for row-based windows)
	FrameSpec *FrameSpec
	// TimeWindowSpec describes time-based windowing (for event-time windows)
	TimeWindowSpec *TimeWindowSpec
	// OutputCol is the name of the output column
	OutputCol string
	// Input is the child logical node
	Input LogicalNode
}

func (w *LogicalWindowAgg) nodeName() string { return "LogicalWindowAgg" }

// JoinCondition describes a single join condition (equi-join on columns)
type JoinCondition struct {
	// LeftCol is the column name from the left table (with table prefix, e.g., "a.id")
	LeftCol string
	// RightCol is the column name from the right table (with table prefix, e.g., "b.id")
	RightCol string
}

// LogicalJoin represents an inner equi-join between two tables
type LogicalJoin struct {
	// LeftTable is the left table name
	LeftTable string
	// RightTable is the right table name
	RightTable string
	// Conditions are the join conditions (ON clause)
	Conditions []JoinCondition
	// Left is the left input (typically LogicalScan)
	Left LogicalNode
	// Right is the right input (typically LogicalScan)
	Right LogicalNode
}

func (j *LogicalJoin) nodeName() string { return "LogicalJoin" }

// LogicalSort represents ORDER BY clause
type LogicalSort struct {
	// OrderColumns are the columns to sort by
	OrderColumns []string
	// Descending indicates whether each column is sorted descending
	Descending []bool
	// Input is the child logical node
	Input LogicalNode
}

func (s *LogicalSort) nodeName() string { return "LogicalSort" }

// LogicalLimit represents LIMIT and OFFSET clauses
type LogicalLimit struct {
	// Limit is the maximum number of rows to return
	Limit int64
	// Offset is the number of rows to skip
	Offset int64
	// Input is the child logical node
	Input LogicalNode
}

func (l *LogicalLimit) nodeName() string { return "LogicalLimit" }
