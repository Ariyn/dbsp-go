package sqlconv

import (
	"strings"
	"testing"

	"github.com/ariyn/dbsp/internal/dbsp/ir"
	"github.com/ariyn/dbsp/internal/dbsp/op"
	"github.com/ariyn/dbsp/internal/dbsp/types"
)

func sampleTelemetryBatch() types.Batch {
	return types.Batch{
		{Tuple: types.Tuple{
			"timestamp":  int64(1_700_000_000_000_000_000),
			"entity_id":  "e1",
			"group_id":   "g1",
			"event_date": "2026-02-27",
			"metric_x":   7.0,
			"metric_y":   2.0,
			"baseline":   8.0,
			"signal":     10.0,
		}, Count: 1},
		{Tuple: types.Tuple{
			"timestamp":  int64(1_700_000_300_000_000_000),
			"entity_id":  "e1",
			"group_id":   "g1",
			"event_date": "2026-02-27",
			"metric_x":   6.0,
			"metric_y":   3.0,
			"baseline":   8.5,
			"signal":     11.0,
		}, Count: 1},
	}
}

func TestTrace_LaggedDataHasPreviousValues(t *testing.T) {
	query := `WITH stage1 AS (
		SELECT
			timestamp,
			entity_id,
			group_id,
			event_date,
			metric_x,
			metric_y,
			baseline,
			signal AS normalized_signal,
			LAG(timestamp) OVER (PARTITION BY entity_id ORDER BY timestamp) AS previous_timestamp,
			LAG(metric_x) OVER (PARTITION BY entity_id ORDER BY timestamp) AS previous_metric_x,
			LAG(metric_y) OVER (PARTITION BY entity_id ORDER BY timestamp) AS previous_metric_y
		FROM events
	)
	SELECT *
	FROM stage1
	PARTITION BY group_id, event_date`

	root, err := ParseQueryToIncrementalDBSP(query)
	if err != nil {
		t.Fatalf("ParseQueryToIncrementalDBSP failed: %v", err)
	}

	out, err := op.Execute(root, sampleTelemetryBatch())
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}
	if len(out) == 0 {
		t.Fatalf("expected lagged_data output")
	}

	found := false
	foundDifferentTs := false
	for _, td := range out {
		tu := td.Tuple
		if tu["previous_timestamp"] != nil && tu["previous_metric_x"] != nil && tu["previous_metric_y"] != nil {
			found = true
			if tu["timestamp"] != tu["previous_timestamp"] {
				foundDifferentTs = true
			}
		}
	}
	if !found {
		t.Fatalf("expected at least one lagged row with previous values, got %v", out)
	}
	if !foundDifferentTs {
		t.Fatalf("expected lagged previous_timestamp to differ from current timestamp, got %v", out)
	}
}

func TestTrace_PowerCalcProducesEnergyInputs(t *testing.T) {
	timedeltaExpr := ir.BuildExprFunc("(timestamp::DOUBLE / 1000000000.0) - (previous_timestamp::DOUBLE / 1000000000.0)")
	v0, err := timedeltaExpr(types.Tuple{
		"timestamp":          int64(1_700_000_300_000_000_000),
		"previous_timestamp": int64(1_700_000_000_000_000_000),
	})
	if err != nil {
		t.Fatalf("direct timedelta expression eval failed: %v", err)
	}
	if types.ToFloat64(v0) == 0 {
		t.Fatalf("direct timedelta expression expected non-zero result, got %v", v0)
	}

	query := `WITH stage1 AS (
		SELECT
			timestamp,
			entity_id,
			group_id,
			event_date,
			metric_x,
			metric_y,
			baseline,
			signal AS normalized_signal,
			LAG(timestamp) OVER (PARTITION BY entity_id ORDER BY timestamp) AS previous_timestamp,
			LAG(metric_x) OVER (PARTITION BY entity_id ORDER BY timestamp) AS previous_metric_x,
			LAG(metric_y) OVER (PARTITION BY entity_id ORDER BY timestamp) AS previous_metric_y
		FROM events
	),
	stage2 AS (
		SELECT
			timestamp,
			previous_timestamp,
			entity_id,
			group_id,
			event_date,
			normalized_signal,
			metric_y,
			baseline,
			metric_x,
			metric_x * metric_y AS score,
			previous_metric_x * previous_metric_y AS previous_score,
			(timestamp::DOUBLE / 1000000000.0) - (previous_timestamp::DOUBLE / 1000000000.0) AS delta_seconds
		FROM stage1
		WHERE previous_timestamp IS NOT NULL AND timestamp IS NOT NULL
	)
	SELECT *
	FROM stage2
	PARTITION BY group_id, event_date`

	root, err := ParseQueryToIncrementalDBSP(query)
	if err != nil {
		t.Fatalf("ParseQueryToIncrementalDBSP failed: %v", err)
	}

	out, err := op.Execute(root, sampleTelemetryBatch())
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}
	if len(out) == 0 {
		t.Fatalf("expected stage2 output")
	}

	hasNonZeroTimedelta := false
	hasEnergyDelta := false
	energyExpr := ir.BuildExprFunc("(score + previous_score) * delta_seconds / 2.0 / 3600.0")
	sumAgg := &op.SumAgg{
		ColName:  "(score + previous_score) * delta_seconds / 2.0 / 3600.0",
		DeltaCol: "weighted_score",
		Expr:     energyExpr,
	}
	var sumState any = float64(0)

	for _, td := range out {
		tu := td.Tuple
		if tu["score"] == nil || tu["previous_score"] == nil || tu["delta_seconds"] == nil {
			continue
		}
		if types.ToFloat64(tu["delta_seconds"]) != 0 {
			hasNonZeroTimedelta = true
		}

		v, err := energyExpr(tu)
		if err != nil {
			t.Fatalf("weighted score expression eval failed: %v tuple=%v", err, tu)
		}
		if v == nil {
			continue
		}

		newState, delta := sumAgg.Apply(sumState, td)
		sumState = newState
		if delta != nil {
			if _, ok := delta.Tuple["weighted_score"]; ok {
				hasEnergyDelta = true
			}
		}
	}

	if !hasNonZeroTimedelta {
		t.Fatalf("expected at least one non-zero delta_seconds in stage2 output, got out=%v", out)
	}
	if !hasEnergyDelta {
		t.Fatalf("expected SumAgg weighted_score delta from stage2 output, got out=%v", out)
	}
}

func TestTrace_CombinedDataOutputsDeltaColumns(t *testing.T) {
	query := `WITH stage1 AS (
		SELECT
			timestamp,
			entity_id,
			group_id,
			event_date,
			metric_x,
			metric_y,
			baseline,
			signal AS normalized_signal,
			LAG(timestamp) OVER (PARTITION BY entity_id ORDER BY timestamp) AS previous_timestamp,
			LAG(metric_x) OVER (PARTITION BY entity_id ORDER BY timestamp) AS previous_metric_x,
			LAG(metric_y) OVER (PARTITION BY entity_id ORDER BY timestamp) AS previous_metric_y
		FROM events
	),
	stage2 AS (
		SELECT
			timestamp,
			entity_id,
			group_id,
			event_date,
			normalized_signal,
			metric_y,
			baseline,
			metric_x,
			metric_x * metric_y AS score,
			previous_metric_x * previous_metric_y AS previous_score,
			(timestamp::DOUBLE / 1000000000.0) - (previous_timestamp::DOUBLE / 1000000000.0) AS delta_seconds
		FROM stage1
		WHERE previous_timestamp IS NOT NULL AND timestamp IS NOT NULL
	),
	stage3 AS (
		SELECT
			entity_id AS id,
			group_id,
			event_date,
			TIME_BUCKET(INTERVAL '5 min', timestamp::TIMESTAMP) AS bucket_time,
			AVG(metric_y) AS avg_metric_y,
			AVG(metric_y * metric_x) AS avg_product,
			AVG(baseline) AS avg_baseline,
			AVG(metric_x) AS avg_metric_x,
			AVG(normalized_signal) AS avg_signal,
			SUM((score + previous_score) * delta_seconds / 2.0 / 3600.0) AS weighted_score
		FROM stage2
		GROUP BY id, group_id, event_date, bucket_time
	)
	SELECT *
	FROM stage3
	PARTITION BY group_id, event_date`

	root, err := ParseQueryToIncrementalDBSP(query)
	if err != nil {
		t.Fatalf("ParseQueryToIncrementalDBSP failed: %v", err)
	}

	out, err := op.Execute(root, sampleTelemetryBatch())
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}
	if len(out) == 0 {
		t.Fatalf("expected non-empty output deltas")
	}

	tuple := out[0].Tuple
	if _, ok := tuple["avg_metric_y"]; !ok {
		t.Fatalf("expected alias column avg_metric_y in delta output, got tuple=%v", tuple)
	}
	hasWeightedScore := false
	for _, td := range out {
		if _, ok := td.Tuple["weighted_score"]; ok {
			hasWeightedScore = true
			break
		}
	}
	if !hasWeightedScore {
		t.Fatalf("expected weighted_score aggregate alias in output batch, got out=%v", out)
	}
	if _, ok := tuple["avg_delta"]; ok {
		t.Fatalf("expected no generic avg_delta in projected output tuple, got %v", tuple)
	}
	if tuple["id"] == nil {
		t.Fatalf("expected grouped alias key id to be materialized, got %v", tuple)
	}
	if tuple["bucket_time"] == nil {
		t.Fatalf("expected grouped alias key bucket_time to be materialized, got %v", tuple)
	}
}

func TestTrace_FullGenericQueryProducesOutputDelta(t *testing.T) {
	query := `WITH stage1 AS (
		SELECT
			timestamp,
			entity_id,
			group_id,
			event_date,
			metric_x,
			metric_y,
			baseline,
			signal AS normalized_signal,
			LAG(timestamp) OVER (PARTITION BY entity_id ORDER BY timestamp) AS previous_timestamp,
			LAG(metric_x) OVER (PARTITION BY entity_id ORDER BY timestamp) AS previous_metric_x,
			LAG(metric_y) OVER (PARTITION BY entity_id ORDER BY timestamp) AS previous_metric_y
		FROM events
	),
	stage2 AS (
		SELECT
			previous_timestamp AS timestamp_start,
			timestamp AS timestamp_end,
			timestamp,
			entity_id,
			group_id,
			event_date,
			metric_x,
			metric_y,
			baseline,
			normalized_signal,
			metric_x * metric_y AS score,
			previous_metric_x * previous_metric_y AS previous_score,
			(timestamp::DOUBLE / 1000000000.0) - (previous_timestamp::DOUBLE / 1000000000.0) AS delta_seconds
		FROM stage1
		WHERE previous_timestamp IS NOT NULL AND timestamp IS NOT NULL
	),
	stage3 AS (
		SELECT
			entity_id AS id,
			group_id,
			event_date,
			TIME_BUCKET(INTERVAL '5 min', timestamp::TIMESTAMP) AS bucket_time,
			AVG(metric_y) AS avg_metric_y,
			AVG(metric_y * metric_x) AS avg_product,
			AVG(baseline) AS avg_baseline,
			AVG(metric_x) AS avg_metric_x,
			AVG(normalized_signal) AS avg_signal,
			SUM((score + previous_score) * delta_seconds / 2.0 / 3600.0) AS weighted_score
		FROM stage2
		GROUP BY id, group_id, event_date, bucket_time
	),
	final_result AS (
		SELECT
			ROUND(avg_metric_y, 2) AS metric_y,
			ROUND(avg_product, 2) AS score,
			ROUND(avg_baseline, 2) AS baseline,
			ROUND(avg_metric_x, 2) AS metric_x,
			ROUND(avg_signal, 2) AS signal,
			weighted_score,
			SUM(weighted_score) OVER (PARTITION BY id ORDER BY bucket_time) AS cumulative_weighted_score,
			id,
			group_id,
			event_date,
			STRFTIME(bucket_time, '%H:%M:%S') AS clock_time,
			bucket_time AS timestamp
		FROM stage3
	)
	SELECT *
	FROM final_result
	ORDER BY clock_time
	PARTITION BY group_id, event_date`

	root, err := ParseQueryToIncrementalDBSP(query)
	if err != nil {
		t.Fatalf("ParseQueryToIncrementalDBSP failed: %v", err)
	}

	out, err := op.Execute(root, sampleTelemetryBatch())
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}
	if len(out) == 0 {
		t.Fatalf("expected non-empty output delta after AST recovery, got %v", out)
	}

	foundMaterialized := false
	for _, td := range out {
		tu := td.Tuple
		if tu["group_id"] != nil && tu["event_date"] != nil && tu["weighted_score"] != nil {
			foundMaterialized = true
			break
		}
	}
	if !foundMaterialized {
		t.Fatalf("expected final_result projection columns to be materialized, got %v", out)
	}
}

func TestTrace_CombinedDataAggregateExtractionShape(t *testing.T) {
	query := `WITH stage1 AS (
		SELECT
			timestamp,
			entity_id,
			group_id,
			event_date,
			metric_x,
			metric_y,
			baseline,
			signal AS normalized_signal,
			LAG(timestamp) OVER (PARTITION BY entity_id ORDER BY timestamp) AS previous_timestamp,
			LAG(metric_x) OVER (PARTITION BY entity_id ORDER BY timestamp) AS previous_metric_x,
			LAG(metric_y) OVER (PARTITION BY entity_id ORDER BY timestamp) AS previous_metric_y
		FROM events
	),
	stage2 AS (
		SELECT
			timestamp,
			entity_id,
			group_id,
			event_date,
			normalized_signal,
			metric_y,
			baseline,
			metric_x,
			metric_x * metric_y AS score,
			previous_metric_x * previous_metric_y AS previous_score,
			(timestamp::DOUBLE / 1000000000.0) - (previous_timestamp::DOUBLE / 1000000000.0) AS delta_seconds
		FROM stage1
		WHERE previous_timestamp IS NOT NULL AND timestamp IS NOT NULL
	),
	stage3 AS (
		SELECT
			entity_id AS id,
			group_id,
			event_date,
			TIME_BUCKET(INTERVAL '5 min', timestamp::TIMESTAMP) AS bucket_time,
			AVG(metric_y) AS avg_metric_y,
			AVG(metric_y * metric_x) AS avg_product,
			AVG(baseline) AS avg_baseline,
			AVG(metric_x) AS avg_metric_x,
			AVG(normalized_signal) AS avg_signal,
			SUM((score + previous_score) * delta_seconds / 2.0 / 3600.0) AS weighted_score
		FROM stage2
		GROUP BY id, group_id, event_date, bucket_time
	)
	SELECT *
	FROM stage3
	PARTITION BY group_id, event_date`

	lp, err := ParseQueryToLogicalPlan(query)
	if err != nil {
		t.Fatalf("ParseQueryToLogicalPlan failed: %v", err)
	}

	var root ir.LogicalNode = lp
	if v, ok := lp.(*ir.LogicalView); ok {
		root = v.Input
	}
	with, ok := root.(*ir.LogicalWith)
	if !ok {
		t.Fatalf("expected LogicalWith root, got %T", root)
	}

	cte, ok := with.CTEs["stage3"]
	if !ok {
		t.Fatalf("stage3 CTE not found")
	}
	if p, ok := cte.(*ir.LogicalProject); ok {
		cte = p.Input
	}
	g, ok := cte.(*ir.LogicalGroupAgg)
	if !ok {
		t.Fatalf("expected stage3 to be LogicalGroupAgg, got %T", cte)
	}

	foundWeightedScore := false
	var scoreExprSQL string
	for _, a := range g.Aggs {
		if a.Name == "SUM" && a.As == "weighted_score" {
			foundWeightedScore = true
			scoreExprSQL = a.Col
			break
		}
	}
	if !foundWeightedScore {
		t.Fatalf("expected SUM aggregate with alias weighted_score in LogicalGroupAgg, got aggs=%v", g.Aggs)
	}
	if scoreExprSQL == "" {
		t.Fatalf("expected non-empty SUM weighted_score expression, got aggs=%v", g.Aggs)
	}
	if !strings.Contains(scoreExprSQL, "delta_seconds") {
		t.Fatalf("expected SUM weighted_score expression to include delta_seconds, got %q", scoreExprSQL)
	}
}

func TestTrace_PowerCalcTimedeltaExprNormalized(t *testing.T) {
	query := `WITH stage1 AS (
		SELECT
			timestamp,
			entity_id,
			group_id,
			event_date,
			metric_x,
			metric_y,
			baseline,
			signal AS normalized_signal,
			LAG(timestamp) OVER (PARTITION BY entity_id ORDER BY timestamp) AS previous_timestamp,
			LAG(metric_x) OVER (PARTITION BY entity_id ORDER BY timestamp) AS previous_metric_x,
			LAG(metric_y) OVER (PARTITION BY entity_id ORDER BY timestamp) AS previous_metric_y
		FROM events
	),
	stage2 AS (
		SELECT
			timestamp,
			previous_timestamp,
			entity_id,
			group_id,
			event_date,
			normalized_signal,
			metric_y,
			baseline,
			metric_x,
			metric_x * metric_y AS score,
			previous_metric_x * previous_metric_y AS previous_score,
			(timestamp::DOUBLE / 1000000000.0) - (previous_timestamp::DOUBLE / 1000000000.0) AS delta_seconds
		FROM stage1
		WHERE previous_timestamp IS NOT NULL AND timestamp IS NOT NULL
	)
	SELECT *
	FROM stage2
	PARTITION BY group_id, event_date`

	lp, err := ParseQueryToLogicalPlan(query)
	if err != nil {
		t.Fatalf("ParseQueryToLogicalPlan failed: %v", err)
	}

	var root ir.LogicalNode = lp
	if v, ok := lp.(*ir.LogicalView); ok {
		root = v.Input
	}
	with, ok := root.(*ir.LogicalWith)
	if !ok {
		t.Fatalf("expected LogicalWith root, got %T", root)
	}

	cte, ok := with.CTEs["stage2"]
	if !ok {
		t.Fatalf("stage2 CTE not found")
	}
	p, ok := cte.(*ir.LogicalProject)
	if !ok {
		t.Fatalf("expected stage2 to be LogicalProject, got %T", cte)
	}

	var expr string
	for _, e := range p.Exprs {
		if e.As == "delta_seconds" {
			expr = e.ExprSQL
			break
		}
	}
	if expr == "" {
		t.Fatalf("delta_seconds expression not found in stage2 project exprs=%v", p.Exprs)
	}
	t.Logf("stage2 delta_seconds expr: %s", expr)
	if strings.Contains(expr, "'timestamp'") || strings.Contains(expr, "'previous_timestamp'") {
		t.Fatalf("timedelta expression should not contain quoted identifier literals, got %q", expr)
	}
	if !strings.Contains(expr, "previous_timestamp") || !strings.Contains(expr, "timestamp") {
		t.Fatalf("delta expression should reference timestamp and previous_timestamp, got %q", expr)
	}
}
