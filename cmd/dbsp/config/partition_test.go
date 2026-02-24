package config

import "testing"

func TestValidatePartitionConfig(t *testing.T) {
	cfg := PartitionConfig{
		Enabled: true,
		Keys:    []string{"plant_id", "local_date"},
	}

	if err := ValidatePartitionConfig(cfg, "SELECT panel_position, SUM(v_out*i_out) AS p FROM sales GROUP BY panel_position"); err != nil {
		t.Fatalf("expected valid config, got error: %v", err)
	}
}

func TestValidatePartitionConfigRejectsJobs(t *testing.T) {
	cfg := PartitionConfig{
		Enabled: true,
		Keys:    []string{"plant_id", "local_date"},
		Jobs: []map[string]interface{}{
			{"values": map[string]string{"plant_id": "P-1", "local_date": "2026-02-24"}},
		},
	}

	if err := ValidatePartitionConfig(cfg, "SELECT panel_position, SUM(v_out*i_out) AS p FROM sales GROUP BY panel_position"); err == nil {
		t.Fatal("expected error because partition.jobs is not supported")
	}
}

func TestValidatePartitionConfigMissingTransformQuery(t *testing.T) {
	cfg := PartitionConfig{
		Enabled: true,
		Keys:    []string{"plant_id", "local_date"},
	}

	if err := ValidatePartitionConfig(cfg, ""); err == nil {
		t.Fatal("expected error for empty transform query")
	}
}

func TestQueryContainsPartitionPredicate(t *testing.T) {
	if !QueryContainsPartitionPredicate("SELECT * FROM t WHERE plant_id='P-1'", []string{"plant_id", "local_date"}) {
		t.Fatal("expected predicate detection for plant_id")
	}
	if QueryContainsPartitionPredicate("SELECT * FROM t", []string{"plant_id", "local_date"}) {
		t.Fatal("did not expect predicate detection without WHERE")
	}
}

func TestBuildHivePartitionPath(t *testing.T) {
	keys := []string{"plant_id", "local_date"}
	vals := map[string]string{"plant_id": "P-1", "local_date": "2026-02-24"}

	got := BuildHivePartitionPath("/tmp/out.parquet", keys, vals)
	want := "/tmp/plant_id=P-1/local_date=2026-02-24/out.parquet"
	if got != want {
		t.Fatalf("expected %q, got %q", want, got)
	}

	gotPrefix := BuildHivePartitionPath("/tmp/out", keys, vals)
	wantPrefix := "/tmp/out/plant_id=P-1/local_date=2026-02-24"
	if gotPrefix != wantPrefix {
		t.Fatalf("expected %q, got %q", wantPrefix, gotPrefix)
	}
}
