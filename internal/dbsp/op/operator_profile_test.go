package op

import (
	"testing"

	"github.com/ariyn/dbsp/internal/dbsp/types"
)

func TestLoadOperatorProfileConfigFromEnvDefaults(t *testing.T) {
	getenv := func(key string) string {
		switch key {
		case "DBSP_OPERATOR_PROFILE":
			return "1"
		default:
			return ""
		}
	}
	cfg := loadOperatorProfileConfigFromEnv(getenv)
	if !cfg.enabled {
		t.Fatal("expected operator profile to be enabled")
	}
	if cfg.every != 20 {
		t.Fatalf("expected default every=20, got %d", cfg.every)
	}
}

func TestDistinctTupleCountIgnoresDuplicateTuples(t *testing.T) {
	batch := types.Batch{
		{Tuple: types.Tuple{"id": "a", "ts": int64(1)}, Count: 1},
		{Tuple: types.Tuple{"id": "a", "ts": int64(1)}, Count: -1},
		{Tuple: types.Tuple{"id": "a", "ts": int64(2)}, Count: 1},
	}
	if got := distinctTupleCount(batch); got != 2 {
		t.Fatalf("expected 2 distinct tuples, got %d", got)
	}
}