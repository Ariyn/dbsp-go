package main

import (
	"testing"

	sqlconv "github.com/ariyn/dbsp/internal/dbsp/sql"
	"github.com/ariyn/dbsp/internal/dbsp/op"
)

func TestApplyWatermarkConfig_WrapsWindowAgg(t *testing.T) {
	query := "SELECT k, COUNT(*) AS c FROM events GROUP BY k, TUMBLE(ts, INTERVAL '10' SECOND)"
	root, err := sqlconv.ParseQueryToIncrementalDBSP(query)
	if err != nil {
		t.Fatalf("ParseQueryToIncrementalDBSP: %v", err)
	}

	wm, err := buildWatermarkConfig(WatermarkYAMLConfig{Enabled: true, Policy: "drop", MaxOutOfOrderness: "2s", AllowedLateness: "1s"})
	if err != nil {
		t.Fatalf("buildWatermarkConfig: %v", err)
	}
	applyWatermarkConfig(root, wm)

	// WindowAggOp may be root.Op or inside a ChainedOp depending on the graph.
	if _, ok := root.Op.(*op.WatermarkAwareWindowOp); ok {
		return
	}
	if chained, ok := root.Op.(*op.ChainedOp); ok {
		for _, inner := range chained.Ops {
			if _, ok := inner.(*op.WatermarkAwareWindowOp); ok {
				return
			}
		}
	}
	// As a fallback, the window op could be on a child.
	for _, in := range root.Inputs {
		if in != nil {
			if _, ok := in.Op.(*op.WatermarkAwareWindowOp); ok {
				return
			}
		}
	}
	
	t.Fatalf("expected a WindowAggOp to be wrapped with WatermarkAwareWindowOp somewhere, got root=%T", root.Op)
}
