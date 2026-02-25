package main

import (
	"strings"
	"testing"
)

func TestParsePipelineConfig_RejectsDeprecatedJoinTTLKey(t *testing.T) {
	yamlBody := []byte(`pipeline:
  source:
    type: csv
    config:
      path: examples/data.csv
  transform:
    type: sql
    join_ttl: "10m"
    query: "SELECT 1"
  sink:
    type: console
    config: {}
  wal:
    enabled: false
    path: /tmp/wal.db
  partition:
    enabled: false
`)

	_, err := parsePipelineConfig(yamlBody)
	if err == nil {
		t.Fatalf("expected config parse error for deprecated join_ttl key")
	}
	if !strings.Contains(strings.ToLower(err.Error()), "join_ttl") {
		t.Fatalf("expected error to mention join_ttl, got: %v", err)
	}
}

func TestValidateTransformTTL(t *testing.T) {
	if _, err := validateTransformTTL("", false); err != nil {
		t.Fatalf("expected empty ttl to pass, got: %v", err)
	}

	if _, err := validateTransformTTL("not-a-duration", true); err == nil {
		t.Fatalf("expected invalid ttl parse error")
	}

	if _, err := validateTransformTTL("24h", false); err == nil {
		t.Fatalf("expected wal-enabled validation error")
	}

	d, err := validateTransformTTL("24h", true)
	if err != nil {
		t.Fatalf("expected valid ttl when wal is enabled, got: %v", err)
	}
	if d.Hours() != 24 {
		t.Fatalf("expected ttl duration 24h, got: %v", d)
	}
}
