package sqlconv

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/ariyn/dbsp/internal/dbsp/ir"
	"github.com/ariyn/dbsp/internal/dbsp/op"
	"github.com/ariyn/dbsp/internal/dbsp/types"
)

func TestAutoGroupingRegression(t *testing.T) {
	tests := []struct {
		name    string
		query   string
		wantAgg bool
	}{
		{
			name:    "TIME_BUCKET with aggregates should trigger Auto-Grouping",
			query:   "SELECT key_a, TIME_BUCKET(INTERVAL '5 min', ts::TIMESTAMP) as bucket, AVG(value_a) as avg_a FROM source_table",
			wantAgg: true,
		},
		{
			name:    "Aggregates without GROUP BY should trigger Auto-Grouping",
			query:   "SELECT AVG(value_a) as avg_a, SUM(value_b) as sum_b FROM source_table",
			wantAgg: true,
		},
		{
			name:    "Normal SELECT without aggregates should NOT trigger Auto-Grouping",
			query:   "SELECT key_a, key_b FROM source_table",
			wantAgg: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lp, err := ParseQueryToLogicalPlan(tt.query)
			if err != nil {
				t.Fatalf("ParseQueryToLogicalPlan failed: %v", err)
			}
			hasAgg := false
			curr := lp
			for curr != nil {
				if _, ok := curr.(*ir.LogicalGroupAgg); ok {
					hasAgg = true
					break
				}
				switch v := curr.(type) {
				case *ir.LogicalProject:
					curr = v.Input
				case *ir.LogicalView:
					curr = v.Input
				default:
					curr = nil
				}
			}
			if hasAgg != tt.wantAgg {
				t.Errorf("Auto-Grouping trigger mismatch: got hasAgg=%v, want %v", hasAgg, tt.wantAgg)
			}
		})
	}
}

func TestNilTimeRegression(t *testing.T) {
	query := "SELECT TIME_BUCKET(INTERVAL '5 min', ts::TIMESTAMP) AS bucket FROM source_table"
	_, err := ParseQueryToLogicalPlan(query)
	if err != nil {
		t.Errorf("Failed to parse time bucket query: %v", err)
	}
}

func TestAggregateProjectionMapping(t *testing.T) {
	query := "SELECT ROUND(AVG(value_a), 2) AS avg_a FROM source_table"
	lp, err := ParseQueryToLogicalPlan(query)
	if err != nil {
		t.Fatalf("Failed to parse aggregate projection: %v", err)
	}
	if lp == nil {
		t.Fatal("Logical plan is nil")
	}
}

func TestAutoGroupingIncludesTimeBucketAliasAndAggAliases(t *testing.T) {
	query := "SELECT key_a, key_b, TIME_BUCKET(INTERVAL '5 min', ts::TIMESTAMP) AS bucket, ROUND(AVG(value_a), 2) AS avg_a, SUM(value_b) AS sum_b FROM source_table"
	lp, err := ParseQueryToLogicalPlan(query)
	if err != nil {
		t.Fatalf("ParseQueryToLogicalPlan failed: %v", err)
	}
	ga := findFirstGroupAgg(lp)
	if ga == nil {
		t.Fatal("Expected LogicalGroupAgg but got nil")
	}
	keys := make(map[string]struct{}, len(ga.Keys))
	for _, k := range ga.Keys {
		keys[k] = struct{}{}
	}
	for _, want := range []string{"key_a", "key_b", "bucket"} {
		if _, ok := keys[want]; !ok {
			t.Fatalf("Expected group key %q to be present, got keys=%v", want, ga.Keys)
		}
	}
	if len(ga.Aggs) == 0 {
		t.Fatal("Expected multi-aggregate specs")
	}
	aliases := make(map[string]struct{}, len(ga.Aggs))
	for _, a := range ga.Aggs {
		if a.As != "" {
			aliases[a.As] = struct{}{}
		}
	}
	for _, want := range []string{"avg_a", "sum_b"} {
		if _, ok := aliases[want]; !ok {
			t.Fatalf("Expected aggregate alias %q to be present, got aliases=%v", want, aliases)
		}
	}
}

func TestWindowAggregateDoesNotTriggerGroupAgg(t *testing.T) {
	query := "SELECT SUM(value_b) OVER (PARTITION BY key_a ORDER BY bucket) AS running_sum, key_a FROM source_table"
	lp, err := ParseQueryToLogicalPlan(query)
	if err != nil {
		t.Fatalf("ParseQueryToLogicalPlan failed: %v", err)
	}
	if ga := findFirstGroupAgg(lp); ga != nil {
		t.Fatalf("Did not expect LogicalGroupAgg for window aggregate query, got keys=%v", ga.Keys)
	}
}

func TestGroupByBeforeWindowAgg(t *testing.T) {
	query := "SELECT key_a, ts, SUM(value_b) AS total, SUM(value_b) OVER (PARTITION BY key_a ORDER BY ts) AS running_total FROM source_table GROUP BY key_a, ts"
	lp, err := ParseQueryToLogicalPlan(query)
	if err != nil {
		t.Fatalf("ParseQueryToLogicalPlan failed: %v", err)
	}
	chain := collectLogicalChain(lp)
	waIdx := indexOfNode(chain, "*ir.LogicalWindowAgg")
	gaIdx := indexOfNode(chain, "*ir.LogicalGroupAgg")
	if waIdx == -1 {
		t.Fatalf("Expected LogicalWindowAgg but got nil; chain=%v", chain)
	}
	if gaIdx == -1 {
		t.Fatalf("Expected LogicalGroupAgg but got nil; chain=%v", chain)
	}
	if waIdx > gaIdx {
		t.Fatalf("Expected window aggregate to run after group by (window above group); chain=%v", chain)
	}
}

func TestIncrementalPlanEmitsValueDeltas(t *testing.T) {
	query := "SELECT key_a, SUM(value_b) AS total FROM source_table GROUP BY key_a"
	root, err := ParseQueryToIncrementalDBSP(query)
	if err != nil {
		t.Fatalf("ParseQueryToIncrementalDBSP failed: %v", err)
	}
	if root == nil {
		t.Fatal("Expected non-nil root operator")
	}
	g := findFirstGroupAggOp(root)
	if g == nil {
		t.Fatal("Expected GroupAggOp in incremental plan")
	}
	if !g.EmitValue {
		t.Fatal("Expected GroupAggOp to emit value deltas")
	}
}

func TestWindowAggEmitsValueDeltas(t *testing.T) {
	query := "SELECT key_a, SUM(value_b) OVER (PARTITION BY key_a ORDER BY ts) AS running_sum FROM source_table"
	root, err := ParseQueryToIncrementalDBSP(query)
	if err != nil {
		t.Fatalf("ParseQueryToIncrementalDBSP failed: %v", err)
	}
	if root == nil {
		t.Fatal("Expected non-nil root operator")
	}
	wa := findFirstWindowAggOp(root)
	if wa == nil {
		t.Fatal("Expected WindowAggOp in incremental plan")
	}
	if !wa.EmitValue {
		t.Fatal("Expected WindowAggOp to emit value deltas")
	}
}

func TestWindowFuncAliasPreservedInLogicalPlan(t *testing.T) {
	query := "WITH lagged_data AS (SELECT timestamp, panel_position, LAG(timestamp) OVER (PARTITION BY panel_position ORDER BY timestamp) AS timestamp_last FROM events) SELECT * FROM lagged_data"
	lp, err := ParseQueryToLogicalPlan(query)
	if err != nil {
		t.Fatalf("ParseQueryToLogicalPlan failed: %v", err)
	}
	wf := findWindowFuncByOutput(lp, "timestamp_last")
	if wf == nil {
		t.Fatal("Expected LogicalWindowFunc with output timestamp_last")
	}
}

func TestWindowFuncAliasPreservedInDBSP(t *testing.T) {
	query := "WITH lagged_data AS (SELECT timestamp, panel_position, LAG(timestamp) OVER (PARTITION BY panel_position ORDER BY timestamp) AS timestamp_last FROM events) SELECT * FROM lagged_data"
	root, err := ParseQueryToIncrementalDBSP(query)
	if err != nil {
		t.Fatalf("ParseQueryToIncrementalDBSP failed: %v", err)
	}
	lag := findFirstOrderedWindowOp(root)
	if lag == nil {
		t.Fatal("Expected OrderedWindowOp in incremental plan")
	}
	if lag.OutputCol != "timestamp_last" {
		t.Fatalf("Expected OrderedWindowOp OutputCol=timestamp_last, got %q", lag.OutputCol)
	}
}

func TestGroupByDeduplicatesAggregateAliases(t *testing.T) {
	query := "SELECT panel_position AS id, TIME_BUCKET(INTERVAL '5 min', timestamp::TIMESTAMP) AS binned_date, ROUND(AVG(i_out), 2) AS i_out, ROUND(AVG(i_out * v_out), 2) AS p, ROUND(AVG(v_in), 2) AS v_in, ROUND(AVG(v_out), 2) AS v_out, ROUND(AVG(temp), 2) AS temp, SUM((p_out + p_out_last) * timedelta_second / 2.0 / 3600.0) AS energy FROM power_calc GROUP BY id, binned_date"
	lp, err := ParseQueryToLogicalPlan(query)
	if err != nil {
		t.Fatalf("ParseQueryToLogicalPlan failed: %v", err)
	}
	ga := findFirstGroupAgg(lp)
	if ga == nil {
		t.Fatal("Expected LogicalGroupAgg")
	}
	energyCount := 0
	for _, agg := range ga.Aggs {
		if agg.As == "energy" {
			energyCount++
		}
	}
	if energyCount != 1 {
		t.Fatalf("Expected exactly one energy aggregate, got %d (%v)", energyCount, ga.Aggs)
	}
}

func TestFullQueryEnergyAliasNotDuplicated(t *testing.T) {
	query := "WITH lagged_data AS ( SELECT timestamp, panel_position, plant_id, local_date, v_out, i_out, v_in, temp, LAG(timestamp) OVER (PARTITION BY panel_position ORDER BY timestamp) AS timestamp_last, LAG(v_out) OVER (PARTITION BY panel_position ORDER BY timestamp) AS v_out_last, LAG(i_out) OVER (PARTITION BY panel_position ORDER BY timestamp) AS i_out_last FROM events ), power_calc AS ( SELECT timestamp_last AS timestamp_start, timestamp AS timestamp_end, timestamp, panel_position, plant_id, local_date, v_out, i_out, v_in, temp, v_out * i_out AS p_out, v_out_last * i_out_last AS p_out_last, (timestamp::DOUBLE / 1000000000.0) - (timestamp_last::DOUBLE / 1000000000.0) AS timedelta_second FROM lagged_data WHERE timestamp_last IS NOT NULL AND timestamp IS NOT NULL ), combined_data AS ( SELECT panel_position AS id, plant_id, local_date, TIME_BUCKET(INTERVAL '5 min', timestamp::TIMESTAMP) AS binned_date, ROUND(AVG(i_out), 2) AS i_out, ROUND(AVG(i_out * v_out), 2) AS p, ROUND(AVG(v_in), 2) AS v_in, ROUND(AVG(v_out), 2) AS v_out, ROUND(AVG(temp), 2) AS temp, SUM((p_out + p_out_last) * timedelta_second / 2.0 / 3600.0) AS energy FROM power_calc GROUP BY id, plant_id, local_date, binned_date ) SELECT * FROM combined_data"
	lp, err := ParseQueryToLogicalPlan(query)
	if err != nil {
		t.Fatalf("ParseQueryToLogicalPlan failed: %v", err)
	}
	ga := findGroupAggInWith(lp, []string{"combined_data", "power_calc", "lagged_data"})
	if ga == nil {
		t.Fatal("Expected LogicalGroupAgg in WITH query")
	}
	energyCount := 0
	for _, agg := range ga.Aggs {
		if agg.As == "energy" {
			energyCount++
		}
	}
	if energyCount != 1 {
		t.Fatalf("Expected exactly one energy aggregate in WITH query, got %d (%v)", energyCount, ga.Aggs)
	}
}

func TestEnergyAndCumulativeEnergyE2E(t *testing.T) {
	query := "WITH lagged_data AS ( SELECT timestamp, panel_position, plant_id, local_date, v_out, i_out, v_in, temp, LAG(timestamp) OVER (PARTITION BY panel_position ORDER BY timestamp) AS timestamp_last, LAG(v_out) OVER (PARTITION BY panel_position ORDER BY timestamp) AS v_out_last, LAG(i_out) OVER (PARTITION BY panel_position ORDER BY timestamp) AS i_out_last FROM events ), power_calc AS ( SELECT timestamp_last AS timestamp_start, timestamp AS timestamp_end, timestamp, panel_position, plant_id, local_date, v_out, i_out, v_in, temp, v_out * i_out AS p_out, v_out_last * i_out_last AS p_out_last, (timestamp::DOUBLE / 1000000000.0) - (timestamp_last::DOUBLE / 1000000000.0) AS timedelta_second FROM lagged_data WHERE timestamp_last IS NOT NULL AND timestamp IS NOT NULL ), combined_data AS ( SELECT panel_position AS id, plant_id, local_date, TIME_BUCKET(INTERVAL '5 min', timestamp::TIMESTAMP) AS binned_date, ROUND(AVG(i_out), 2) AS i_out, ROUND(AVG(i_out * v_out), 2) AS p, ROUND(AVG(v_in), 2) AS v_in, ROUND(AVG(v_out), 2) AS v_out, ROUND(AVG(temp), 2) AS temp, SUM((p_out + p_out_last) * timedelta_second / 2.0 / 3600.0) AS energy FROM power_calc GROUP BY id, plant_id, local_date, binned_date ), final_data AS ( SELECT i_out, p, v_in, v_out, temp, energy, SUM(energy) OVER (PARTITION BY id ORDER BY binned_date) AS cumulative_energy, id, plant_id, local_date, STRFTIME(binned_date, '%H:%M:%S') AS date, binned_date AS timestamp FROM combined_data ) SELECT * FROM final_data WHERE id = 'panel-1' ORDER BY date"
	root, err := ParseQueryToIncrementalDBSP(query)
	if err != nil {
		t.Fatalf("ParseQueryToIncrementalDBSP failed: %v", err)
	}

	base := time.Date(2026, 2, 27, 7, 0, 0, 0, time.UTC)
	rows := []types.Tuple{
		{"timestamp": base.UnixNano(), "panel_position": "panel-1", "plant_id": "plant-a", "local_date": "2026-02-27", "v_out": 10.0, "i_out": 2.0, "v_in": 100.0, "temp": 25.0},
		{"timestamp": base.Add(60 * time.Second).UnixNano(), "panel_position": "panel-1", "plant_id": "plant-a", "local_date": "2026-02-27", "v_out": 20.0, "i_out": 2.0, "v_in": 100.0, "temp": 25.0},
		{"timestamp": base.Add(120 * time.Second).UnixNano(), "panel_position": "panel-1", "plant_id": "plant-a", "local_date": "2026-02-27", "v_out": 30.0, "i_out": 2.0, "v_in": 100.0, "temp": 25.0},
	}

	assertPositiveEnergySnapshot := func(outBatches []types.Batch) {
		snapshot := map[string]types.Tuple{}
		for _, out := range outBatches {
			for _, td := range out {
				id := td.Tuple["id"]
				ts := td.Tuple["timestamp"]
				key := ""
				if id != nil || ts != nil {
					key = keyForTuple(id, ts)
				}
				if key == "" {
					continue
				}
				if td.Count < 0 {
					delete(snapshot, key)
					continue
				}
				snapshot[key] = td.Tuple
			}
		}

		if len(snapshot) == 0 {
			t.Fatal("Expected final snapshot row but got none")
		}
		var final types.Tuple
		for _, row := range snapshot {
			final = row
		}
		if final["temp"] == nil {
			t.Fatalf("Expected non-nil temp, got nil (row=%v)", final)
		}
		temp := types.ToFloat64(final["temp"])
		energy := types.ToFloat64(final["energy"])
		cumulative := types.ToFloat64(final["cumulative_energy"])
		if temp != 25.0 {
			t.Fatalf("Expected temp=25.0, got %v (row=%v)", final["temp"], final)
		}
		if energy <= 0 {
			t.Fatalf("Expected positive energy, got %v (row=%v)", final["energy"], final)
		}
		if cumulative <= 0 {
			t.Fatalf("Expected positive cumulative_energy, got %v (row=%v)", final["cumulative_energy"], final)
		}
	}

	var sequentialOut []types.Batch
	for _, row := range rows {
		out, err := op.Execute(root, types.Batch{{Tuple: row, Count: 1}})
		if err != nil {
			t.Fatalf("Execute failed: %v", err)
		}
		sequentialOut = append(sequentialOut, out)
	}
	assertPositiveEnergySnapshot(sequentialOut)

	root, err = ParseQueryToIncrementalDBSP(query)
	if err != nil {
		t.Fatalf("ParseQueryToIncrementalDBSP failed: %v", err)
	}
	batched := make(types.Batch, 0, len(rows))
	for _, row := range rows {
		batched = append(batched, types.TupleDelta{Tuple: row, Count: 1})
	}
	out, err := op.Execute(root, batched)
	if err != nil {
		t.Fatalf("Execute batched failed: %v", err)
	}
	assertPositiveEnergySnapshot([]types.Batch{out})
}

func TestFullQueryRequiredFieldsRemainSelective(t *testing.T) {
	query := "WITH lagged_data AS ( SELECT timestamp, panel_position, plant_id, local_date, v_out, i_out, v_in, temp, LAG(timestamp) OVER (PARTITION BY panel_position ORDER BY timestamp) AS timestamp_last, LAG(v_out) OVER (PARTITION BY panel_position ORDER BY timestamp) AS v_out_last, LAG(i_out) OVER (PARTITION BY panel_position ORDER BY timestamp) AS i_out_last FROM events ), power_calc AS ( SELECT timestamp_last AS timestamp_start, timestamp AS timestamp_end, timestamp, panel_position, plant_id, local_date, v_out, i_out, v_in, temp, v_out * i_out AS p_out, v_out_last * i_out_last AS p_out_last, (timestamp::DOUBLE / 1000000000.0) - (timestamp_last::DOUBLE / 1000000000.0) AS timedelta_second FROM lagged_data WHERE timestamp_last IS NOT NULL AND timestamp IS NOT NULL ), combined_data AS ( SELECT panel_position AS id, plant_id, local_date, TIME_BUCKET(INTERVAL '5 min', timestamp::TIMESTAMP) AS binned_date, ROUND(AVG(i_out), 2) AS i_out, ROUND(AVG(i_out * v_out), 2) AS p, ROUND(AVG(v_in), 2) AS v_in, ROUND(AVG(v_out), 2) AS v_out, ROUND(AVG(temp), 2) AS temp, SUM((p_out + p_out_last) * timedelta_second / 2.0 / 3600.0) AS energy FROM power_calc GROUP BY id, plant_id, local_date, binned_date ), final_data AS ( SELECT i_out, p, v_in, v_out, temp, energy, SUM(energy) OVER (PARTITION BY id ORDER BY binned_date) AS cumulative_energy, id, plant_id, local_date, STRFTIME(binned_date, '%H:%M:%S') AS date, binned_date AS timestamp FROM combined_data ) SELECT * FROM final_data WHERE id = 'panel-1' ORDER BY date"

	lp, err := ParseQueryToLogicalPlan(query)
	if err != nil {
		t.Fatalf("ParseQueryToLogicalPlan failed: %v", err)
	}

	cols := ir.CollectRequiredInputColumns(lp)
	if cols == nil {
		t.Fatal("expected selective required fields, got keep-all")
	}

	for _, name := range []string{"timestamp", "panel_position", "plant_id", "local_date", "v_out", "i_out", "v_in", "temp"} {
		if _, ok := cols[name]; !ok {
			t.Fatalf("expected source field %q in required fields: %v", name, cols)
		}
	}
}

func findFirstGroupAgg(n ir.LogicalNode) *ir.LogicalGroupAgg {
	for n != nil {
		if ga, ok := n.(*ir.LogicalGroupAgg); ok {
			return ga
		}
		n = nextLogicalInput(n)
	}
	return nil
}

func findFirstWindowAgg(n ir.LogicalNode) *ir.LogicalWindowAgg {
	for n != nil {
		if wa, ok := n.(*ir.LogicalWindowAgg); ok {
			return wa
		}
		n = nextLogicalInput(n)
	}
	return nil
}

func collectLogicalChain(n ir.LogicalNode) []string {
	var out []string
	for n != nil {
		out = append(out, reflect.TypeOf(n).String())
		n = nextLogicalInput(n)
	}
	return out
}

func indexOfNode(chain []string, name string) int {
	for i, v := range chain {
		if v == name {
			return i
		}
	}
	return -1
}

func nextLogicalInput(n ir.LogicalNode) ir.LogicalNode {
	switch v := n.(type) {
	case *ir.LogicalProject:
		return v.Input
	case *ir.LogicalView:
		return v.Input
	case *ir.LogicalFilter:
		return v.Input
	case *ir.LogicalGroupAgg:
		return v.Input
	case *ir.LogicalWindowAgg:
		return v.Input
	case *ir.LogicalWindowFunc:
		return v.Input
	case *ir.LogicalSort:
		return v.Input
	case *ir.LogicalLimit:
		return v.Input
	case *ir.LogicalWith:
		return v.Body
	default:
		return nil
	}
}

func findFirstGroupAggOp(n *op.Node) *op.GroupAggOp {
	if n == nil {
		return nil
	}
	if g, ok := n.Op.(*op.GroupAggOp); ok {
		return g
	}
	for _, in := range n.Inputs {
		if g := findFirstGroupAggOp(in); g != nil {
			return g
		}
	}
	return nil
}

func findFirstWindowAggOp(n *op.Node) *op.WindowAggOp {
	if n == nil {
		return nil
	}
	if w, ok := n.Op.(*op.WindowAggOp); ok {
		return w
	}
	if chained, ok := n.Op.(*op.ChainedOp); ok {
		for _, inner := range chained.Ops {
			if w, ok := inner.(*op.WindowAggOp); ok {
				return w
			}
		}
	}
	for _, in := range n.Inputs {
		if w := findFirstWindowAggOp(in); w != nil {
			return w
		}
	}
	return nil
}

func findWindowFuncByOutput(n ir.LogicalNode, output string) *ir.LogicalWindowFunc {
	if with, ok := n.(*ir.LogicalWith); ok {
		for _, cte := range with.CTEs {
			if wf := findWindowFuncByOutput(cte, output); wf != nil {
				return wf
			}
		}
		return findWindowFuncByOutput(with.Body, output)
	}
	for n != nil {
		if wf, ok := n.(*ir.LogicalWindowFunc); ok {
			if wf.OutputCol == output {
				return wf
			}
		}
		n = nextLogicalInput(n)
	}
	return nil
}

func findFirstOrderedWindowOp(n *op.Node) *op.OrderedWindowOp {
	if n == nil {
		return nil
	}
	if lag, ok := n.Op.(*op.OrderedWindowOp); ok {
		return lag
	}
	if chained, ok := n.Op.(*op.ChainedOp); ok {
		for _, inner := range chained.Ops {
			if lag, ok := inner.(*op.OrderedWindowOp); ok {
				return lag
			}
		}
	}
	for _, in := range n.Inputs {
		if lag := findFirstOrderedWindowOp(in); lag != nil {
			return lag
		}
	}
	return nil
}

func keyForTuple(id any, ts any) string {
	if id == nil && ts == nil {
		return ""
	}
	return fmt.Sprintf("%v|%v", id, ts)
}

func findGroupAggInWith(n ir.LogicalNode, cteNames []string) *ir.LogicalGroupAgg {
	with, ok := n.(*ir.LogicalWith)
	if !ok {
		return findFirstGroupAgg(n)
	}
	for _, name := range cteNames {
		if cte, ok := with.CTEs[name]; ok {
			if ga := findFirstGroupAgg(cte); ga != nil {
				return ga
			}
		}
	}
	return findFirstGroupAgg(with.Body)
}
