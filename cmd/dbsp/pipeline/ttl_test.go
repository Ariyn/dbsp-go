package pipeline

import (
	"testing"
	"time"
)

func TestParseTTL(t *testing.T) {
	got, err := ParseTTL("10s")
	if err != nil {
		t.Fatalf("ParseTTL failed: %v", err)
	}
	if got != 10*time.Second {
		t.Fatalf("expected 10s, got %s", got)
	}

	got, err = ParseTTL("5 minutes")
	if err != nil {
		t.Fatalf("ParseTTL failed: %v", err)
	}
	if got != 5*time.Minute {
		t.Fatalf("expected 5m, got %s", got)
	}
}
