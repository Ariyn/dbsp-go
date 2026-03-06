package metrics

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestCollectorSetObservePipelineBatch(t *testing.T) {
	collectors := newCollectorSet(prometheus.NewRegistry())
	collectors.observePipelineBatch(10, 12, time.Millisecond, 2*time.Millisecond, 3*time.Millisecond, time.Millisecond, 7*time.Millisecond)

	if got := testutil.ToFloat64(collectors.pipelineBatches); got != 1 {
		t.Fatalf("expected 1 pipeline batch, got %v", got)
	}
	if got := testutil.ToFloat64(collectors.pipelineRowsTotal.WithLabelValues("input")); got != 10 {
		t.Fatalf("expected 10 input rows, got %v", got)
	}
	if got := testutil.ToFloat64(collectors.pipelineRowsTotal.WithLabelValues("output")); got != 12 {
		t.Fatalf("expected 12 output rows, got %v", got)
	}
	if got := testutil.CollectAndCount(collectors.pipelineStageDuration); got != 5 {
		t.Fatalf("expected 5 stage duration histograms, got %d", got)
	}
}

func TestCollectorSetObserveOperatorBatch(t *testing.T) {
	collectors := newCollectorSet(prometheus.NewRegistry())
	collectors.observeOperatorBatch("WindowAggOp", 10, 12, 9, 7, 2)

	if got := testutil.ToFloat64(collectors.operatorAppliesTotal.WithLabelValues("WindowAggOp")); got != 1 {
		t.Fatalf("expected 1 operator apply, got %v", got)
	}
	if got := testutil.ToFloat64(collectors.operatorRowsTotal.WithLabelValues("WindowAggOp", "input")); got != 10 {
		t.Fatalf("expected 10 operator input rows, got %v", got)
	}
	if got := testutil.ToFloat64(collectors.operatorRowsTotal.WithLabelValues("WindowAggOp", "output")); got != 12 {
		t.Fatalf("expected 12 operator output rows, got %v", got)
	}
	if got := testutil.ToFloat64(collectors.operatorOutputTotal.WithLabelValues("WindowAggOp", "distinct")); got != 9 {
		t.Fatalf("expected 9 distinct output rows, got %v", got)
	}
	if got := testutil.ToFloat64(collectors.operatorOutputTotal.WithLabelValues("WindowAggOp", "repeated")); got != 3 {
		t.Fatalf("expected 3 repeated output rows, got %v", got)
	}
	if got := testutil.ToFloat64(collectors.operatorAppendTotal.WithLabelValues("WindowAggOp", "hit")); got != 7 {
		t.Fatalf("expected 7 append hits, got %v", got)
	}
	if got := testutil.ToFloat64(collectors.operatorAppendTotal.WithLabelValues("WindowAggOp", "miss")); got != 2 {
		t.Fatalf("expected 2 append misses, got %v", got)
	}
}
