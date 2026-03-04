package sqlconv

import (
	"strings"
	"testing"

	"github.com/ariyn/dbsp/internal/dbsp/ir"
)

func TestCombinedDataPlan_HasGroupKeysAndAggs(t *testing.T) {
	query := `WITH lagged_data AS (
		SELECT
			timestamp,
			device_id,
			site_id,
			event_day,
			voltage,
			current,
			input_voltage,
			temperature_c AS temperature,
			LAG(timestamp) OVER (PARTITION BY device_id ORDER BY timestamp) AS previous_timestamp,
			LAG(voltage) OVER (PARTITION BY device_id ORDER BY timestamp) AS previous_voltage,
			LAG(current) OVER (PARTITION BY device_id ORDER BY timestamp) AS previous_current
		FROM events
	),
	power_calc AS (
		SELECT
			timestamp,
			device_id,
			site_id,
			event_day,
			temperature,
			current,
			input_voltage,
			voltage,
			voltage * current AS power,
			previous_voltage * previous_current AS previous_power,
			(timestamp::DOUBLE / 1000000000.0) - (previous_timestamp::DOUBLE / 1000000000.0) AS delta_seconds
		FROM lagged_data
		WHERE previous_timestamp IS NOT NULL AND timestamp IS NOT NULL
	),
	combined_data AS (
		SELECT
			device_id AS entity_id,
			site_id,
			event_day,
			TIME_BUCKET(INTERVAL '5 min', timestamp::TIMESTAMP) AS bucket_time,
			AVG(current) AS avg_current,
			AVG(current * voltage) AS avg_power,
			AVG(input_voltage) AS avg_input_voltage,
			AVG(voltage) AS avg_voltage,
			AVG(temperature) AS avg_temperature,
			SUM((power + previous_power) * delta_seconds / 2.0 / 3600.0) AS energy_kwh
		FROM power_calc
		GROUP BY entity_id, site_id, event_day, bucket_time
	)
	SELECT *
	FROM combined_data`

	lp, err := ParseQueryToLogicalPlan(query)
	if err != nil {
		t.Fatalf("ParseQueryToLogicalPlan failed: %v", err)
	}

	ga := findFirstGroupAgg(lp)
	if ga == nil {
		t.Fatalf("expected LogicalGroupAgg in plan")
	}
	if len(ga.Keys) == 0 {
		t.Fatalf("expected non-empty group keys")
	}
	keys := strings.Join(ga.Keys, ",")
	for _, must := range []string{"entity_id", "site_id", "event_day", "bucket_time"} {
		if !strings.Contains(keys, must) {
			t.Fatalf("expected group key %s in keys=%v", must, ga.Keys)
		}
	}
	if len(ga.Aggs) == 0 && ga.AggName == "" {
		t.Fatalf("expected aggregate specs in plan")
	}
	if len(ga.Aggs) > 0 {
		aliases := make(map[string]struct{}, len(ga.Aggs))
		for _, a := range ga.Aggs {
			if strings.Contains(a.Col, "'") {
				t.Fatalf("unexpected quoted aggregate column %q in %+v", a.Col, ga.Aggs)
			}
			aliases[a.As] = struct{}{}
		}
		for _, must := range []string{"avg_current", "avg_power", "avg_input_voltage", "avg_voltage", "avg_temperature", "energy_kwh"} {
			if _, ok := aliases[must]; !ok {
				t.Fatalf("expected aggregate alias %s in %+v", must, ga.Aggs)
			}
		}
	}
}

func TestExtractCTEBodyQueries_CombinedDataBodyRecovered(t *testing.T) {
	query := `WITH lagged_data AS (
		SELECT timestamp, device_id, site_id, event_day, voltage, current, input_voltage, temperature_c AS temperature,
		LAG(timestamp) OVER (PARTITION BY device_id ORDER BY timestamp) AS previous_timestamp,
		LAG(voltage) OVER (PARTITION BY device_id ORDER BY timestamp) AS previous_voltage,
		LAG(current) OVER (PARTITION BY device_id ORDER BY timestamp) AS previous_current
		FROM events
	),
	power_calc AS (
		SELECT timestamp, device_id, site_id, event_day, temperature, current, input_voltage, voltage,
		voltage * current AS power,
		previous_voltage * previous_current AS previous_power,
		(timestamp::DOUBLE / 1000000000.0) - (previous_timestamp::DOUBLE / 1000000000.0) AS delta_seconds
		FROM lagged_data
		WHERE previous_timestamp IS NOT NULL AND timestamp IS NOT NULL
	),
	combined_data AS (
		SELECT device_id AS entity_id, site_id, event_day,
		TIME_BUCKET(INTERVAL '5 min', timestamp::TIMESTAMP) AS bucket_time,
		AVG(current) AS avg_current,
		AVG(current * voltage) AS avg_power,
		AVG(input_voltage) AS avg_input_voltage,
		AVG(voltage) AS avg_voltage,
		AVG(temperature) AS avg_temperature,
		SUM((power + previous_power) * delta_seconds / 2.0 / 3600.0) AS energy_kwh
		FROM power_calc
		GROUP BY entity_id, site_id, event_day, bucket_time
	)
	SELECT * FROM combined_data`

	bodies := extractCTEBodyQueries(query)
	body := strings.ToUpper(bodies["combined_data"])
	if !strings.Contains(body, "SUM((POWER + PREVIOUS_POWER) * DELTA_SECONDS / 2.0 / 3600.0) AS ENERGY_KWH") {
		t.Fatalf("combined_data CTE body not recovered as expected: %q", bodies["combined_data"])
	}
}

func TestCombinedDataPlan_UsesCTERefForPowerCalc(t *testing.T) {
	query := `WITH lagged_data AS (
		SELECT timestamp, device_id, site_id, event_day, voltage, current, input_voltage, temperature_c AS temperature,
		LAG(timestamp) OVER (PARTITION BY device_id ORDER BY timestamp) AS previous_timestamp,
		LAG(voltage) OVER (PARTITION BY device_id ORDER BY timestamp) AS previous_voltage,
		LAG(current) OVER (PARTITION BY device_id ORDER BY timestamp) AS previous_current
		FROM events
	),
	power_calc AS (
		SELECT timestamp, device_id, site_id, event_day, temperature, current, input_voltage, voltage,
		voltage * current AS power,
		previous_voltage * previous_current AS previous_power,
		(timestamp::DOUBLE / 1000000000.0) - (previous_timestamp::DOUBLE / 1000000000.0) AS delta_seconds
		FROM lagged_data
		WHERE previous_timestamp IS NOT NULL AND timestamp IS NOT NULL
	),
	combined_data AS (
		SELECT device_id AS entity_id, site_id, event_day,
		TIME_BUCKET(INTERVAL '5 min', timestamp::TIMESTAMP) AS bucket_time,
		AVG(current) AS avg_current,
		AVG(current * voltage) AS avg_power,
		AVG(input_voltage) AS avg_input_voltage,
		AVG(voltage) AS avg_voltage,
		AVG(temperature) AS avg_temperature,
		SUM((power + previous_power) * delta_seconds / 2.0 / 3600.0) AS energy_kwh
		FROM power_calc
		GROUP BY entity_id, site_id, event_day, bucket_time
	)
	SELECT * FROM combined_data`

	lp, err := ParseQueryToLogicalPlan(query)
	if err != nil {
		t.Fatalf("ParseQueryToLogicalPlan failed: %v", err)
	}
	if hasScanByName(lp, "power_calc") {
		t.Fatalf("expected power_calc to be resolved as CTE ref, found plain scan")
	}
	with, ok := lp.(*ir.LogicalWith)
	if !ok {
		t.Fatalf("expected LogicalWith root, got %T", lp)
	}
	combined := with.CTEs["combined_data"]
	if combined == nil {
		t.Fatalf("combined_data CTE missing from WITH map")
	}
	if ref := findFirstCTERef(combined); ref == "" || !strings.EqualFold(ref, "power_calc") {
		t.Fatalf("expected combined_data to reference power_calc CTE, got %q", ref)
	}
}

func TestCombinedDataPlan_HasPreGroupAliasMaterialization(t *testing.T) {
	query := `SELECT device_id AS id, site_id, event_day,
	TIME_BUCKET(INTERVAL '5 min', timestamp::TIMESTAMP) AS bucket_time,
	AVG(current) AS avg_current,
	SUM((power + previous_power) * delta_seconds / 2.0 / 3600.0) AS energy_kwh
	FROM power_calc
	GROUP BY id, site_id, event_day, bucket_time`

	lp, err := ParseQueryToLogicalPlan(query)
	if err != nil {
		t.Fatalf("ParseQueryToLogicalPlan failed: %v", err)
	}
	if !hasPreGroupAliasProjection(lp, "id", "bucket_time") {
		t.Fatalf("expected pre-group alias materialization projection for id/bucket_time")
	}
	proj := findPreGroupAliasProjection(lp)
	if proj == nil {
		t.Fatalf("expected to find pre-group alias projection")
	}
	aliasExpr := map[string]string{}
	for _, e := range proj.Exprs {
		aliasExpr[e.As] = e.ExprSQL
	}
	if strings.TrimSpace(aliasExpr["id"]) != "device_id" {
		t.Fatalf("expected id expr device_id, got %q", aliasExpr["id"])
	}
	if strings.TrimSpace(aliasExpr["bucket_time"]) != "timestamp" {
		t.Fatalf("expected bucket_time expr timestamp, got %q", aliasExpr["bucket_time"])
	}
}

func hasPreGroupAliasProjection(n ir.LogicalNode, aliases ...string) bool {
	ga, ok := n.(*ir.LogicalGroupAgg)
	if !ok {
		switch t := n.(type) {
		case *ir.LogicalProject:
			return hasPreGroupAliasProjection(t.Input, aliases...)
		case *ir.LogicalFilter:
			return hasPreGroupAliasProjection(t.Input, aliases...)
		case *ir.LogicalWindowFunc:
			return hasPreGroupAliasProjection(t.Input, aliases...)
		case *ir.LogicalWindowAgg:
			return hasPreGroupAliasProjection(t.Input, aliases...)
		case *ir.LogicalSort:
			return hasPreGroupAliasProjection(t.Input, aliases...)
		case *ir.LogicalWith:
			if hasPreGroupAliasProjection(t.Body, aliases...) {
				return true
			}
			for _, k := range t.CTENames {
				if hasPreGroupAliasProjection(t.CTEs[k], aliases...) {
					return true
				}
			}
		}
		return false
	}
	proj, ok := ga.Input.(*ir.LogicalProject)
	if !ok || !proj.KeepInput {
		return false
	}
	set := map[string]struct{}{}
	for _, e := range proj.Exprs {
		set[e.As] = struct{}{}
	}
	for _, a := range aliases {
		if _, ok := set[a]; !ok {
			return false
		}
	}
	return true
}

func findPreGroupAliasProjection(n ir.LogicalNode) *ir.LogicalProject {
	ga, ok := n.(*ir.LogicalGroupAgg)
	if ok {
		if proj, ok := ga.Input.(*ir.LogicalProject); ok && proj.KeepInput {
			return proj
		}
		return nil
	}
	switch t := n.(type) {
	case *ir.LogicalProject:
		return findPreGroupAliasProjection(t.Input)
	case *ir.LogicalFilter:
		return findPreGroupAliasProjection(t.Input)
	case *ir.LogicalWindowFunc:
		return findPreGroupAliasProjection(t.Input)
	case *ir.LogicalWindowAgg:
		return findPreGroupAliasProjection(t.Input)
	case *ir.LogicalSort:
		return findPreGroupAliasProjection(t.Input)
	case *ir.LogicalWith:
		if p := findPreGroupAliasProjection(t.Body); p != nil {
			return p
		}
		for _, k := range t.CTENames {
			if p := findPreGroupAliasProjection(t.CTEs[k]); p != nil {
				return p
			}
		}
	}
	return nil
}

func hasScanByName(n ir.LogicalNode, name string) bool {
	switch t := n.(type) {
	case *ir.LogicalScan:
		return strings.EqualFold(t.Table, name)
	case *ir.LogicalProject:
		return hasScanByName(t.Input, name)
	case *ir.LogicalFilter:
		return hasScanByName(t.Input, name)
	case *ir.LogicalGroupAgg:
		return hasScanByName(t.Input, name)
	case *ir.LogicalWindowFunc:
		return hasScanByName(t.Input, name)
	case *ir.LogicalWindowAgg:
		return hasScanByName(t.Input, name)
	case *ir.LogicalSort:
		return hasScanByName(t.Input, name)
	case *ir.LogicalWith:
		if hasScanByName(t.Body, name) {
			return true
		}
		for _, k := range t.CTENames {
			if hasScanByName(t.CTEs[k], name) {
				return true
			}
		}
	}
	return false
}

func findFirstCTERef(n ir.LogicalNode) string {
	switch t := n.(type) {
	case *ir.LogicalCTERef:
		return t.CTEName
	case *ir.LogicalProject:
		return findFirstCTERef(t.Input)
	case *ir.LogicalFilter:
		return findFirstCTERef(t.Input)
	case *ir.LogicalGroupAgg:
		return findFirstCTERef(t.Input)
	case *ir.LogicalWindowFunc:
		return findFirstCTERef(t.Input)
	case *ir.LogicalWindowAgg:
		return findFirstCTERef(t.Input)
	case *ir.LogicalSort:
		return findFirstCTERef(t.Input)
	case *ir.LogicalWith:
		if r := findFirstCTERef(t.Body); r != "" {
			return r
		}
		for _, k := range t.CTENames {
			if r := findFirstCTERef(t.CTEs[k]); r != "" {
				return r
			}
		}
	}
	return ""
}

func findFirstGroupAgg(n ir.LogicalNode) *ir.LogicalGroupAgg {
	switch t := n.(type) {
	case *ir.LogicalGroupAgg:
		return t
	case *ir.LogicalProject:
		return findFirstGroupAgg(t.Input)
	case *ir.LogicalFilter:
		return findFirstGroupAgg(t.Input)
	case *ir.LogicalWindowFunc:
		return findFirstGroupAgg(t.Input)
	case *ir.LogicalWindowAgg:
		return findFirstGroupAgg(t.Input)
	case *ir.LogicalSort:
		return findFirstGroupAgg(t.Input)
	case *ir.LogicalWith:
		if got := findFirstGroupAgg(t.Body); got != nil {
			return got
		}
		for _, name := range t.CTENames {
			if got := findFirstGroupAgg(t.CTEs[name]); got != nil {
				return got
			}
		}
	}
	return nil
}
