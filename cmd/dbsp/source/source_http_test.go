package source

import (
	"net"
	"strings"
	"testing"
	"time"

	"github.com/ariyn/dbsp/cmd/dbsp/config"
	"github.com/ariyn/dbsp/internal/dbsp/types"
)

func TestParseRequestBodyReaderFiltersFields(t *testing.T) {
	s := &HTTPSource{
		schema: map[string]string{
			"a":  "int",
			"ts": "timestamp",
		},
		requiredFields: map[string]struct{}{
			"a":  {},
			"ts": {},
		},
		timestampUnit: "ms",
	}
	body := `[{"a":1,"b":"x","ts":1710000000000,"extra":123}]`
	batch, err := s.parseRequestBodyReader(strings.NewReader(body))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(batch) != 1 {
		t.Fatalf("expected 1 record, got %d", len(batch))
	}
	tuple := batch[0].Tuple
	if _, ok := tuple["a"]; !ok {
		t.Fatalf("expected field a")
	}
	if _, ok := tuple["ts"]; !ok {
		t.Fatalf("expected field ts")
	}
	if _, ok := tuple["b"]; ok {
		t.Fatalf("did not expect field b")
	}
	if _, ok := tuple["extra"]; ok {
		t.Fatalf("did not expect field extra")
	}
	if _, ok := tuple["ts"].(time.Time); !ok {
		t.Fatalf("expected timestamp to be time.Time, got %T", tuple["ts"])
	}
}

func TestParseRequestBodyReaderKeepAll(t *testing.T) {
	s := &HTTPSource{schema: map[string]string{"a": "int"}}
	body := `[{"a":1,"b":"x"}]`
	batch, err := s.parseRequestBodyReader(strings.NewReader(body))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(batch) != 1 {
		t.Fatalf("expected 1 record, got %d", len(batch))
	}
	if _, ok := batch[0].Tuple["b"]; !ok {
		t.Fatalf("expected field b")
	}
}

func TestParseRequestBodyReaderSingleObject(t *testing.T) {
	s := &HTTPSource{
		schema: map[string]string{"a": "int"},
		requiredFields: map[string]struct{}{
			"a": {},
		},
	}
	body := `{"a":2}`
	batch, err := s.parseRequestBodyReader(strings.NewReader(body))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(batch) != 1 {
		t.Fatalf("expected 1 record, got %d", len(batch))
	}
	if got := types.ToInt64(batch[0].Tuple["a"]); got != 2 {
		t.Fatalf("expected a=2, got %d", got)
	}
}

func TestNewHTTPSourceDisablesFilteringWithEmptySchema(t *testing.T) {
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	_ = listener.Close()

	required := map[string]struct{}{"plant_id": {}, "local_date": {}}
	httpCfg := config.HTTPSourceConfig{Port: port, Path: "/ingest"}
	s, err := NewHTTPSource(httpCfg, required)
	if err != nil {
		t.Fatalf("NewHTTPSource: %v", err)
	}
	defer s.Close()

	if s.requiredFields != nil {
		t.Fatalf("expected requiredFields to be nil when schema is empty")
	}

	body := `[{"plant_id":"p","local_date":"2026-02-27","v_out":123}]`
	batch, err := s.parseRequestBodyReader(strings.NewReader(body))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(batch) != 1 {
		t.Fatalf("expected 1 record, got %d", len(batch))
	}
	if _, ok := batch[0].Tuple["v_out"]; !ok {
		t.Fatalf("expected v_out to be kept when filtering is disabled")
	}
}
