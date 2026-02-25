package source

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/ariyn/dbsp/internal/dbsp/types"
)

func TestHTTPSource(t *testing.T) {
	config := map[string]interface{}{
		"port": 8081, // Use different port for testing
		"path": "/test",
		"schema": map[string]string{
			"id":    "int",
			"value": "float",
		},
	}

	source, err := NewHTTPSource(config)
	if err != nil {
		t.Fatalf("NewHTTPSource failed: %v", err)
	}
	defer source.Close()

	// Give server time to start
	time.Sleep(100 * time.Millisecond)

	// Send data
	data := []map[string]interface{}{
		{"id": 1, "value": 10.5},
		{"id": 2, "value": 20.0},
	}
	jsonData, _ := json.Marshal(data)

	resp, err := http.Post("http://localhost:8081/test", "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		t.Fatalf("HTTP Post failed: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected status 200, got %d", resp.StatusCode)
	}

	// Read batch
	batch, err := source.NextBatch()
	if err != nil {
		t.Fatalf("NextBatch failed: %v", err)
	}

	if len(batch) != 2 {
		t.Errorf("Expected 2 records, got %d", len(batch))
	}

	checkTupleGeneric(t, batch[0], "id", 1, "value", 10.5)
	checkTupleGeneric(t, batch[1], "id", 2, "value", 20.0)
}

func checkTupleGeneric(t *testing.T, td types.TupleDelta, k1 string, v1 interface{}, k2 string, v2 interface{}) {
	if td.Tuple[k1] != v1 {
		t.Errorf("Expected %s=%v, got %v", k1, v1, td.Tuple[k1])
	}
	if td.Tuple[k2] != v2 {
		t.Errorf("Expected %s=%v, got %v", k2, v2, td.Tuple[k2])
	}
}

func TestHTTPAutoConvert(t *testing.T) {
	config := map[string]interface{}{
		"port":           8082,
		"path":           "/ingest",
		"auto_convert":   true,
		"timestamp_unit": "auto",
		"schema": map[string]string{
			"id":        "int",
			"active":    "bool",
			"state":     "json",
			"timestamp": "timestamp",
		},
	}

	source, err := NewHTTPSource(config)
	if err != nil {
		t.Fatalf("NewHTTPSource failed: %v", err)
	}
	defer source.Close()

	time.Sleep(100 * time.Millisecond)

	// Records with various types
	data := []map[string]interface{}{
		{
			"id":        "1",
			"active":    "true",
			"state":     map[string]interface{}{"is_ready": true},
			"timestamp": "2026-02-25T12:00:00Z",
			"extra":     "keep as is",
		},
		{
			"id":        2.0,
			"active":    0,
			"timestamp": 1771963200, // s
		},
		{
			"id":        3,
			"active":    true,
			"timestamp": 1771963200000, // ms
		},
	}

	jsonData, _ := json.Marshal(data)
	resp, err := http.Post("http://localhost:8082/ingest", "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		t.Fatalf("HTTP Post failed: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected status 200, got %d", resp.StatusCode)
	}

	batch, err := source.NextBatch()
	if err != nil {
		t.Fatalf("NextBatch failed: %v", err)
	}

	if len(batch) != 3 {
		t.Fatalf("Expected 3 records, got %d", len(batch))
	}

	// record 1 checks
	t1 := batch[0].Tuple
	if t1["id"] != 1 {
		t.Errorf("id: expected 1, got %v", t1["id"])
	}
	if t1["active"] != true {
		t.Errorf("active: expected true, got %v", t1["active"])
	}
	if s, ok := t1["state"].(map[string]interface{}); !ok || !s["is_ready"].(bool) {
		t.Errorf("state: expected map with is_ready=true, got %v", t1["state"])
	}
	if ts, ok := t1["timestamp"].(time.Time); !ok || ts.UTC().Format(time.RFC3339) != "2026-02-25T12:00:00Z" {
		t.Errorf("timestamp: expected 2026-02-25T12:00:00Z, got %v", t1["timestamp"])
	}
	if t1["extra"] != "keep as is" {
		t.Errorf("extra: expected 'keep as is', got %v", t1["extra"])
	}

	// record 2 checks (numeric s)
	t2 := batch[1].Tuple
	if t2["active"] != false {
		t.Errorf("active: expected false, got %v", t2["active"])
	}
	if ts, ok := t2["timestamp"].(time.Time); !ok || ts.Unix() != 1771963200 {
		t.Errorf("timestamp(s): expected 1771963200, got %v", t2["timestamp"])
	}

	// record 3 checks (numeric ms)
	t3 := batch[2].Tuple
	if ts, ok := t3["timestamp"].(time.Time); !ok || ts.Unix() != 1771963200 {
		t.Errorf("timestamp(ms): expected 1771963200 s, got %v", ts.Unix())
	}

	// Test HTTP 400 on conversion failure
	badData := []map[string]interface{}{
		{"id": "not-an-int"},
	}
	jsonData, _ = json.Marshal(badData)
	resp, err = http.Post("http://localhost:8082/ingest", "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		t.Fatalf("HTTP Post failed: %v", err)
	}
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("Expected status 400, got %d", resp.StatusCode)
	}
}

func TestHTTPIngest_MethodAndJSONErrors(t *testing.T) {
	s := &HTTPSource{
		buffer: make(chan types.TupleDelta, 10),
		schema: map[string]string{"id": "int"},
	}

	// Method not allowed
	req := httptest.NewRequest(http.MethodGet, "/ingest", nil)
	rr := httptest.NewRecorder()
	s.handleIngest(rr, req)
	if rr.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", rr.Code)
	}

	// Invalid JSON
	req = httptest.NewRequest(http.MethodPost, "/ingest", strings.NewReader("{invalid"))
	rr = httptest.NewRecorder()
	s.handleIngest(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for invalid json, got %d", rr.Code)
	}
}

func TestHTTPIngest_AtomicRejectOnConversionError(t *testing.T) {
	s := &HTTPSource{
		buffer: make(chan types.TupleDelta, 10),
		schema: map[string]string{"id": "int"},
	}

	body := `[{"id":"1"},{"id":"bad"}]`
	req := httptest.NewRequest(http.MethodPost, "/ingest", strings.NewReader(body))
	rr := httptest.NewRecorder()
	s.handleIngest(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rr.Code)
	}
	if got := len(s.buffer); got != 0 {
		t.Fatalf("expected no enqueued tuples on failure, got %d", got)
	}
}

func TestHTTPIngest_SingleObjectHappyPath(t *testing.T) {
	s := &HTTPSource{
		buffer: make(chan types.TupleDelta, 10),
		schema: map[string]string{
			"id":        "int",
			"active":    "bool",
			"timestamp": "timestamp",
		},
		timestampUnit: "auto",
	}

	body := `{"id":"7","active":"true","timestamp":1771963200000000000,"note":"x"}`
	req := httptest.NewRequest(http.MethodPost, "/ingest", strings.NewReader(body))
	rr := httptest.NewRecorder()
	s.handleIngest(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", rr.Code, rr.Body.String())
	}
	if len(s.buffer) != 1 {
		t.Fatalf("expected 1 buffered tuple, got %d", len(s.buffer))
	}
	td := <-s.buffer
	if td.Tuple["id"] != 7 {
		t.Fatalf("expected converted id=7, got %v", td.Tuple["id"])
	}
	if td.Tuple["active"] != true {
		t.Fatalf("expected converted active=true, got %v", td.Tuple["active"])
	}
	if _, ok := td.Tuple["timestamp"].(time.Time); !ok {
		t.Fatalf("expected timestamp as time.Time, got %T", td.Tuple["timestamp"])
	}
	if td.Tuple["note"] != "x" {
		t.Fatalf("expected passthrough field note=x, got %v", td.Tuple["note"])
	}
}

func TestParseValue_BoolAndTimestampEdgeCases(t *testing.T) {
	s := &HTTPSource{timestampUnit: "auto"}

	if v, err := s.parseValue(1.0, "bool"); err != nil || v != true {
		t.Fatalf("expected bool true from 1.0, got v=%v err=%v", v, err)
	}
	if v, err := s.parseValue(0.0, "bool"); err != nil || v != false {
		t.Fatalf("expected bool false from 0.0, got v=%v err=%v", v, err)
	}
	if _, err := s.parseValue(2.0, "bool"); err == nil {
		t.Fatalf("expected error for invalid bool number")
	}

	if ts, err := s.parseValue("2026-02-25T12:00:00Z", "timestamp"); err != nil {
		t.Fatalf("expected valid RFC3339 timestamp parse, err=%v", err)
	} else if _, ok := ts.(time.Time); !ok {
		t.Fatalf("expected time.Time, got %T", ts)
	}

	if _, err := s.parseValue("2026/02/25 12:00:00", "timestamp"); err == nil {
		t.Fatalf("expected invalid RFC3339 error")
	}
}

func TestParseTimestamp_UnitsAndFallback(t *testing.T) {
	t.Run("explicit units", func(t *testing.T) {
		cases := []struct {
			name string
			unit string
			in   int64
			unix int64
		}{
			{name: "seconds", unit: "s", in: 1771963200, unix: 1771963200},
			{name: "milliseconds", unit: "ms", in: 1771963200000, unix: 1771963200},
			{name: "microseconds", unit: "us", in: 1771963200000000, unix: 1771963200},
			{name: "nanoseconds", unit: "ns", in: 1771963200000000000, unix: 1771963200},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				s := &HTTPSource{timestampUnit: tc.unit}
				ts, err := s.parseTimestamp(tc.in)
				if err != nil {
					t.Fatalf("parseTimestamp error: %v", err)
				}
				if ts.Unix() != tc.unix {
					t.Fatalf("expected unix=%d got=%d", tc.unix, ts.Unix())
				}
			})
		}
	})

	t.Run("auto detect", func(t *testing.T) {
		s := &HTTPSource{timestampUnit: "auto"}
		checks := []struct {
			in   int64
			unix int64
		}{
			{in: 1771963200, unix: 1771963200},
			{in: 1771963200000, unix: 1771963200},
			{in: 1771963200000000, unix: 1771963200},
			{in: 1771963200000000000, unix: 1771963200},
		}
		for _, c := range checks {
			ts, err := s.parseTimestamp(c.in)
			if err != nil {
				t.Fatalf("parseTimestamp error: %v", err)
			}
			if ts.Unix() != c.unix {
				t.Fatalf("expected unix=%d got=%d for in=%d", c.unix, ts.Unix(), c.in)
			}
		}
	})

	t.Run("invalid unit fallback to seconds", func(t *testing.T) {
		s := &HTTPSource{timestampUnit: "bogus"}
		ts, err := s.parseTimestamp(1771963200)
		if err != nil {
			t.Fatalf("parseTimestamp error: %v", err)
		}
		if ts.Unix() != 1771963200 {
			t.Fatalf("expected seconds fallback, got %d", ts.Unix())
		}
	})
}

func TestNewHTTPSource_DefaultsAndClose(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to reserve test port: %v", err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	_ = l.Close()

	cfg := map[string]interface{}{
		"port": port,
		"path": "",
		"schema": map[string]string{
			"id":        "int",
			"timestamp": "timestamp",
		},
	}

	s, err := NewHTTPSource(cfg)
	if err != nil {
		t.Fatalf("NewHTTPSource failed: %v", err)
	}
	defer s.Close()

	time.Sleep(100 * time.Millisecond)

	body := []map[string]interface{}{
		{"id": 1, "timestamp": 1771963200000},
		{"id": 2, "timestamp": 1771963200},
	}
	jsonData, _ := json.Marshal(body)

	resp, err := http.Post(fmt.Sprintf("http://127.0.0.1:%d/ingest", port), "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		t.Fatalf("HTTP Post failed: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected status 200, got %d", resp.StatusCode)
	}

	batch, err := s.NextBatch()
	if err != nil {
		t.Fatalf("NextBatch failed: %v", err)
	}
	if len(batch) != 2 {
		t.Fatalf("expected 2 records from default batching behavior, got %d", len(batch))
	}

	if ts, ok := batch[0].Tuple["timestamp"].(time.Time); !ok || ts.Unix() != 1771963200 {
		t.Fatalf("expected auto timestamp conversion for ms input, got %v", batch[0].Tuple["timestamp"])
	}
	if ts, ok := batch[1].Tuple["timestamp"].(time.Time); !ok || ts.Unix() != 1771963200 {
		t.Fatalf("expected auto timestamp conversion for s input, got %v", batch[1].Tuple["timestamp"])
	}

	if err := s.Close(); err != nil {
		t.Fatalf("Close should return nil, got %v", err)
	}
}

func TestParseValue_PrimitivesAndErrors(t *testing.T) {
	s := &HTTPSource{timestampUnit: "auto"}

	if v, err := s.parseValue(42.0, "int"); err != nil || v != 42 {
		t.Fatalf("int from float64 failed: v=%v err=%v", v, err)
	}
	if v, err := s.parseValue("7", "int"); err != nil || v != 7 {
		t.Fatalf("int from string failed: v=%v err=%v", v, err)
	}
	if _, err := s.parseValue(true, "int"); err == nil {
		t.Fatalf("expected error for invalid int type")
	}

	if v, err := s.parseValue(3.25, "float"); err != nil || v != 3.25 {
		t.Fatalf("float from float64 failed: v=%v err=%v", v, err)
	}
	if v, err := s.parseValue("2.5", "float"); err != nil || v != 2.5 {
		t.Fatalf("float from string failed: v=%v err=%v", v, err)
	}
	if _, err := s.parseValue(false, "float"); err == nil {
		t.Fatalf("expected error for invalid float type")
	}

	if v, err := s.parseValue(123, "string"); err != nil || v != "123" {
		t.Fatalf("string conversion failed: v=%v err=%v", v, err)
	}

	obj := map[string]interface{}{"x": 1}
	if v, err := s.parseValue(obj, "json"); err != nil {
		t.Fatalf("json passthrough failed: err=%v", err)
	} else if m, ok := v.(map[string]interface{}); !ok || m["x"] != 1 {
		t.Fatalf("json passthrough failed: v=%v", v)
	}

	if v, err := s.parseValue("raw", "mystery"); err != nil || v != "raw" {
		t.Fatalf("default passthrough failed: v=%v err=%v", v, err)
	}

	if v, err := s.parseValue(float64(1771963200000), "timestamp"); err != nil {
		t.Fatalf("timestamp from float64 failed: err=%v", err)
	} else if ts, ok := v.(time.Time); !ok || ts.Unix() != 1771963200 {
		t.Fatalf("timestamp from float64 conversion mismatch: v=%v", v)
	}

	if _, err := s.parseValue(true, "timestamp"); err == nil {
		t.Fatalf("expected error for invalid timestamp type")
	}
}

func TestNextBatch_DoneAndGuardPaths(t *testing.T) {
	t.Run("closed done channel returns nil nil", func(t *testing.T) {
		done := make(chan struct{})
		close(done)
		s := &HTTPSource{
			buffer: make(chan types.TupleDelta, 1),
			done:   done,
		}

		batch, err := s.NextBatch()
		if err != nil {
			t.Fatalf("expected nil err, got %v", err)
		}
		if batch != nil {
			t.Fatalf("expected nil batch, got %v", batch)
		}
	})

	t.Run("maxBatchSize<=0 returns single item", func(t *testing.T) {
		s := &HTTPSource{
			buffer:       make(chan types.TupleDelta, 4),
			done:         make(chan struct{}),
			maxBatchSize: 0,
		}
		s.buffer <- types.TupleDelta{Tuple: types.Tuple{"id": 1}, Count: 1}
		s.buffer <- types.TupleDelta{Tuple: types.Tuple{"id": 2}, Count: 1}

		batch, err := s.NextBatch()
		if err != nil {
			t.Fatalf("expected nil err, got %v", err)
		}
		if len(batch) != 1 {
			t.Fatalf("expected single item batch, got %d", len(batch))
		}
		if got := batch[0].Tuple["id"]; got != 1 {
			t.Fatalf("expected first id=1, got %v", got)
		}
	})

	t.Run("maxBatchDelay<=0 fast drains buffered items up to max size", func(t *testing.T) {
		s := &HTTPSource{
			buffer:        make(chan types.TupleDelta, 8),
			done:          make(chan struct{}),
			maxBatchSize:  3,
			maxBatchDelay: 0,
		}
		s.buffer <- types.TupleDelta{Tuple: types.Tuple{"id": 1}, Count: 1}
		s.buffer <- types.TupleDelta{Tuple: types.Tuple{"id": 2}, Count: 1}
		s.buffer <- types.TupleDelta{Tuple: types.Tuple{"id": 3}, Count: 1}
		s.buffer <- types.TupleDelta{Tuple: types.Tuple{"id": 4}, Count: 1}

		batch, err := s.NextBatch()
		if err != nil {
			t.Fatalf("expected nil err, got %v", err)
		}
		if len(batch) != 3 {
			t.Fatalf("expected drained batch size 3, got %d", len(batch))
		}
		for i := 0; i < 3; i++ {
			expected := i + 1
			if got := batch[i].Tuple["id"]; got != expected {
				t.Fatalf("expected id=%d at idx=%d, got %v", expected, i, got)
			}
		}
	})
}
