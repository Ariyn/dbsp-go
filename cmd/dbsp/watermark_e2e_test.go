package main

import (
	"testing"

	"github.com/ariyn/dbsp/internal/dbsp/op"
	sqlconv "github.com/ariyn/dbsp/internal/dbsp/sql"
	"github.com/ariyn/dbsp/internal/dbsp/types"
)

func TestWatermarkE2E_DropLateEvents_DropsOutOfOrderEvent(t *testing.T) {
	query := "SELECT k, COUNT(*) AS c FROM events GROUP BY k, TUMBLE(ts, INTERVAL '10' SECOND)"
	root, err := sqlconv.ParseQueryToIncrementalDBSP(query)
	if err != nil {
		t.Fatalf("ParseQueryToIncrementalDBSP: %v", err)
	}

	wm, err := buildWatermarkConfig(WatermarkYAMLConfig{Enabled: true, Policy: "drop", MaxOutOfOrderness: "0s", AllowedLateness: "0s"})
	if err != nil {
		t.Fatalf("buildWatermarkConfig: %v", err)
	}
	applyWatermarkConfig(root, wm)

	batch := types.Batch{
		{Tuple: types.Tuple{"ts": int64(10000), "k": "A"}, Count: 1},
		// Out-of-order: this would be late/too-late relative to watermark=10000.
		{Tuple: types.Tuple{"ts": int64(1000), "k": "A"}, Count: 1},
	}

	out, err := op.Execute(root, batch)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}

	var sum int64
	for _, td := range out {
		v, ok := td.Tuple["count_delta"]
		if !ok || v == nil {
			continue
		}
		switch x := v.(type) {
		case int64:
			sum += x
		case int:
			sum += int64(x)
		}
	}

	if sum != 1 {
		t.Fatalf("expected count_delta sum=1 (late event dropped), got %d (out=%+v)", sum, out)
	}
}

func TestWatermarkE2E_EmitLateEvents_MarksLateOutput(t *testing.T) {
	query := "SELECT k, COUNT(*) AS c FROM events GROUP BY k, TUMBLE(ts, INTERVAL '10' SECOND)"
	root, err := sqlconv.ParseQueryToIncrementalDBSP(query)
	if err != nil {
		t.Fatalf("ParseQueryToIncrementalDBSP: %v", err)
	}

	// Allow lateness so the out-of-order event is late-but-allowed.
	wm, err := buildWatermarkConfig(WatermarkYAMLConfig{Enabled: true, Policy: "emit", MaxOutOfOrderness: "0s", AllowedLateness: "9s"})
	if err != nil {
		t.Fatalf("buildWatermarkConfig: %v", err)
	}
	applyWatermarkConfig(root, wm)

	batch := types.Batch{
		{Tuple: types.Tuple{"ts": int64(10000), "k": "A"}, Count: 1},
		{Tuple: types.Tuple{"ts": int64(2000), "k": "A"}, Count: 1},
	}

	out, err := op.Execute(root, batch)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}

	foundLate := false
	for _, td := range out {
		if b, ok := td.Tuple["__late"].(bool); ok && b {
			foundLate = true
			break
		}
	}
	if !foundLate {
		t.Fatalf("expected at least one output delta marked __late=true, got out=%+v", out)
	}
}
