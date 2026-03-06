package source

import (
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/ariyn/dbsp/cmd/dbsp/config"
	"github.com/ariyn/dbsp/internal/dbsp/types"
	"github.com/ariyn/dbsp/internal/dbsp/wal"
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

	if s.requiredFields == nil {
		t.Fatalf("expected requiredFields to remain enabled when schema is empty")
	}

	body := `[{"plant_id":"p","local_date":"2026-02-27","v_out":123}]`
	batch, err := s.parseRequestBodyReader(strings.NewReader(body))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(batch) != 1 {
		t.Fatalf("expected 1 record, got %d", len(batch))
	}
	if _, ok := batch[0].Tuple["v_out"]; ok {
		t.Fatalf("did not expect v_out when filtering is enabled")
	}
	if _, ok := batch[0].Tuple["plant_id"]; !ok {
		t.Fatalf("expected plant_id to be kept")
	}
	if _, ok := batch[0].Tuple["local_date"]; !ok {
		t.Fatalf("expected local_date to be kept")
	}
}

func TestParseRequestBodyReaderDoesNotKeepSchemaOnlyFields(t *testing.T) {
	s := &HTTPSource{
		schema: map[string]string{
			"a": "int",
			"b": "string",
		},
		requiredFields: map[string]struct{}{
			"a": {},
		},
	}
	body := `[{"a":1,"b":"x"}]`
	batch, err := s.parseRequestBodyReader(strings.NewReader(body))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(batch) != 1 {
		t.Fatalf("expected 1 record, got %d", len(batch))
	}
	if _, ok := batch[0].Tuple["a"]; !ok {
		t.Fatalf("expected field a")
	}
	if _, ok := batch[0].Tuple["b"]; ok {
		t.Fatalf("did not expect schema-only field b")
	}
}

func TestParseRequestBodyReaderKeepsNestedJSONWithoutRawRedecode(t *testing.T) {
	s := &HTTPSource{
		schema: map[string]string{
			"meta": "json",
		},
		requiredFields: map[string]struct{}{
			"meta": {},
		},
	}
	body := `[{"meta":{"nested":[1,2,true]}}]`
	batch, err := s.parseRequestBodyReader(strings.NewReader(body))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(batch) != 1 {
		t.Fatalf("expected 1 record, got %d", len(batch))
	}
	meta, ok := batch[0].Tuple["meta"].(map[string]any)
	if !ok {
		t.Fatalf("expected meta object, got %T", batch[0].Tuple["meta"])
	}
	nested, ok := meta["nested"].([]any)
	if !ok {
		t.Fatalf("expected nested array, got %T", meta["nested"])
	}
	if len(nested) != 3 {
		t.Fatalf("expected nested length 3, got %d", len(nested))
	}
}

func TestMatchFieldUsesLengthBucketFiltering(t *testing.T) {
	s := &HTTPSource{
		schema: map[string]string{
			"panel_position": "string",
			"plant_id":       "string",
			"local_date":     "string",
		},
		requiredFields: map[string]struct{}{
			"panel_position": {},
			"plant_id":       {},
			"local_date":     {},
		},
	}
	s.fieldSpecs = buildFieldSpecs(s.requiredFields, s.schema)
	s.fieldSpecsByLen = buildFieldSpecsByLen(s.requiredFields, s.schema)

	spec, ok := s.matchField([]byte("plant_id"))
	if !ok {
		t.Fatal("expected plant_id to match")
	}
	if spec.name != "plant_id" || spec.typeKind != fieldTypeString {
		t.Fatalf("unexpected spec: %+v", spec)
	}
	if _, ok := s.matchField([]byte("v_out")); ok {
		t.Fatal("did not expect unrelated field to match")
	}
}

func TestReplayWALQueuesBatchesWithoutUsingLiveBuffer(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "http_source_replay_queue")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	writer, err := wal.NewLogWriter(tmpDir, 1024, 4096)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()

	payload, err := wal.EncodeBatchGobV1(types.Batch{{Tuple: types.Tuple{"a": int64(1)}, Count: 1}})
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.Append(&wal.Record{Type: wal.RecordTypeBatch, Sequence: 10, Payload: payload}); err != nil {
		t.Fatal(err)
	}
	payload, err = wal.EncodeBatchGobV1(types.Batch{{Tuple: types.Tuple{"a": int64(2)}, Count: 1}})
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.Append(&wal.Record{Type: wal.RecordTypeBatch, Sequence: 20, Payload: payload}); err != nil {
		t.Fatal(err)
	}

	s := &HTTPSource{
		schema:        map[string]string{"a": "int"},
		done:          make(chan struct{}),
		timestampUnit: "auto",
		walAvailable:  make(chan struct{}, 1),
	}
	if err := s.ReplayWAL(tmpDir); err != nil {
		t.Fatalf("replay wal: %v", err)
	}
	if got := len(s.walQueue); got != 2 {
		t.Fatalf("expected 2 replay batches queued, got %d", got)
	}
	for _, item := range s.walQueue {
		if item.batch != nil {
			t.Fatal("expected replay preload to queue disk refs only")
		}
	}

	batch, err := s.NextBatch()
	if err != nil {
		t.Fatalf("next batch: %v", err)
	}
	if len(batch) != 1 || types.ToInt64(batch[0].Tuple["a"]) != 1 {
		t.Fatalf("expected first replay batch with a=1, got %v", batch)
	}
}

func TestReplayWALAckPersistsCursorAndSkipsProcessedRecords(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "http_source_replay_cursor")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	writer, err := wal.NewLogWriter(tmpDir, 1024, 4096)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()

	payload, err := wal.EncodeBatchGobV1(types.Batch{{Tuple: types.Tuple{"a": int64(1)}, Count: 1}})
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.Append(&wal.Record{Type: wal.RecordTypeBatch, Sequence: 10, Payload: payload}); err != nil {
		t.Fatal(err)
	}
	payload, err = wal.EncodeBatchGobV1(types.Batch{{Tuple: types.Tuple{"a": int64(2)}, Count: 1}})
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.Append(&wal.Record{Type: wal.RecordTypeBatch, Sequence: 20, Payload: payload}); err != nil {
		t.Fatal(err)
	}

	s1 := &HTTPSource{
		schema:        map[string]string{"a": "int"},
		done:          make(chan struct{}),
		timestampUnit: "auto",
		walAvailable:  make(chan struct{}, 1),
		replayDir:     tmpDir,
	}
	if err := s1.ReplayWAL(tmpDir); err != nil {
		t.Fatalf("replay wal first source: %v", err)
	}
	batch, err := s1.NextBatch()
	if err != nil {
		t.Fatalf("next batch first source: %v", err)
	}
	if len(batch) != 1 || types.ToInt64(batch[0].Tuple["a"]) != 1 {
		t.Fatalf("expected first replayed row a=1, got %v", batch)
	}
	if err := s1.AckBatchProcessed(batch); err != nil {
		t.Fatalf("ack batch: %v", err)
	}

	seq, err := wal.LoadReplayCursor(tmpDir)
	if err != nil {
		t.Fatalf("load replay cursor: %v", err)
	}
	if seq != 10 {
		t.Fatalf("expected replay cursor 10, got %d", seq)
	}

	s2 := &HTTPSource{
		schema:        map[string]string{"a": "int"},
		done:          make(chan struct{}),
		timestampUnit: "auto",
		walAvailable:  make(chan struct{}, 1),
	}
	if err := s2.ReplayWAL(tmpDir); err != nil {
		t.Fatalf("replay wal second source: %v", err)
	}
	if got := len(s2.walQueue); got != 1 {
		t.Fatalf("expected only unacked record to replay, got %d batches", got)
	}
	batch, err = s2.NextBatch()
	if err != nil {
		t.Fatalf("next batch second source: %v", err)
	}
	if len(batch) != 1 || types.ToInt64(batch[0].Tuple["a"]) != 2 {
		t.Fatalf("expected remaining replayed row a=2, got %v", batch)
	}
}

func TestReplayWALDecodesNativeBatchRecords(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "http_source_native_batch_replay")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	writer, err := wal.NewLogWriter(tmpDir, 1024, 4096)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()

	payload, err := wal.EncodeBatchGobV1(types.Batch{{Tuple: types.Tuple{"a": int64(7)}, Count: 1}})
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.Append(&wal.Record{Type: wal.RecordTypeBatch, Sequence: 10, Payload: payload}); err != nil {
		t.Fatal(err)
	}

	s := &HTTPSource{
		schema:        map[string]string{"a": "int"},
		done:          make(chan struct{}),
		timestampUnit: "auto",
		walAvailable:  make(chan struct{}, 1),
	}
	if err := s.ReplayWAL(tmpDir); err != nil {
		t.Fatalf("replay wal: %v", err)
	}
	batch, err := s.NextBatch()
	if err != nil {
		t.Fatalf("next batch: %v", err)
	}
	if len(batch) != 1 || types.ToInt64(batch[0].Tuple["a"]) != 7 {
		t.Fatalf("expected decoded native batch record, got %v", batch)
	}
}

func TestHandleIngestWritesNativeBatchWALRecord(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "http_source_native_batch_write")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	writer, err := wal.NewLogWriter(tmpDir, 1024, 4096)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()

	s := &HTTPSource{
		schema:         map[string]string{"a": "int"},
		done:           make(chan struct{}),
		timestampUnit:  "auto",
		wal:            writer,
		walAvailable:   make(chan struct{}, 1),
		walBufferLimit: 1,
	}

	req := httptest.NewRequest(http.MethodPost, "/ingest", strings.NewReader(`[{"a":5}]`))
	rr := httptest.NewRecorder()
	s.handleIngest(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rr.Code)
	}

	reader, err := wal.NewLogReader(tmpDir)
	if err != nil {
		t.Fatal(err)
	}
	recordCount := 0
	err = reader.Replay(func(rec *wal.Record) error {
		recordCount++
		if rec.Type != wal.RecordTypeBatch {
			t.Fatalf("expected native batch record type, got %d", rec.Type)
		}
		batch, err := wal.DecodeBatchGobV1(rec.Payload)
		if err != nil {
			t.Fatalf("decode native batch payload: %v", err)
		}
		if len(batch) != 1 || types.ToInt64(batch[0].Tuple["a"]) != 5 {
			t.Fatalf("unexpected wal batch payload: %v", batch)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if recordCount != 1 {
		t.Fatalf("expected 1 wal record, got %d", recordCount)
	}

	if got := len(s.walQueue); got != 1 {
		t.Fatalf("expected live wal queue item, got %d", got)
	}
	batch, err := s.NextBatch()
	if err != nil {
		t.Fatalf("next batch: %v", err)
	}
	if len(batch) != 1 || types.ToInt64(batch[0].Tuple["a"]) != 5 {
		t.Fatalf("unexpected next batch: %v", batch)
	}
}

func TestWALNextBatchRespectsMaxBatchSizeAndDefersAckUntilFinalChunk(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "http_source_wal_chunking")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	s := &HTTPSource{
		done:          make(chan struct{}),
		walAvailable:  make(chan struct{}, 1),
		replayDir:     tmpDir,
		maxBatchSize:  2,
		timestampUnit: "auto",
	}
	s.walQueue = []queuedWALBatch{{
		seq: 10,
		batch: types.Batch{
			{Tuple: types.Tuple{"a": int64(1)}, Count: 1},
			{Tuple: types.Tuple{"a": int64(2)}, Count: 1},
			{Tuple: types.Tuple{"a": int64(3)}, Count: 1},
			{Tuple: types.Tuple{"a": int64(4)}, Count: 1},
			{Tuple: types.Tuple{"a": int64(5)}, Count: 1},
		},
	}}

	batch, err := s.NextBatch()
	if err != nil {
		t.Fatalf("first next batch: %v", err)
	}
	if len(batch) != 2 || types.ToInt64(batch[0].Tuple["a"]) != 1 || types.ToInt64(batch[1].Tuple["a"]) != 2 {
		t.Fatalf("unexpected first chunk: %v", batch)
	}
	if err := s.AckBatchProcessed(batch); err != nil {
		t.Fatalf("ack first chunk: %v", err)
	}
	if seq, err := wal.LoadReplayCursor(tmpDir); err != nil || seq != 0 {
		t.Fatalf("expected replay cursor to remain 0 after first chunk, got seq=%d err=%v", seq, err)
	}

	batch, err = s.NextBatch()
	if err != nil {
		t.Fatalf("second next batch: %v", err)
	}
	if len(batch) != 2 || types.ToInt64(batch[0].Tuple["a"]) != 3 || types.ToInt64(batch[1].Tuple["a"]) != 4 {
		t.Fatalf("unexpected second chunk: %v", batch)
	}
	if err := s.AckBatchProcessed(batch); err != nil {
		t.Fatalf("ack second chunk: %v", err)
	}
	if seq, err := wal.LoadReplayCursor(tmpDir); err != nil || seq != 0 {
		t.Fatalf("expected replay cursor to remain 0 after second chunk, got seq=%d err=%v", seq, err)
	}

	batch, err = s.NextBatch()
	if err != nil {
		t.Fatalf("third next batch: %v", err)
	}
	if len(batch) != 1 || types.ToInt64(batch[0].Tuple["a"]) != 5 {
		t.Fatalf("unexpected third chunk: %v", batch)
	}
	if err := s.AckBatchProcessed(batch); err != nil {
		t.Fatalf("ack third chunk: %v", err)
	}
	seq, err := wal.LoadReplayCursor(tmpDir)
	if err != nil {
		t.Fatalf("load replay cursor after final chunk: %v", err)
	}
	if seq != 10 {
		t.Fatalf("expected replay cursor 10 after final chunk, got %d", seq)
	}
}

func TestHandleIngestSpillsToWALWhenMemoryQueueIsFull(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "http_source_runtime_spill")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	writer, err := wal.NewLogWriter(tmpDir, 1024, 4096)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()

	s := &HTTPSource{
		schema:         map[string]string{"a": "int"},
		done:           make(chan struct{}),
		timestampUnit:  "auto",
		wal:            writer,
		walAvailable:   make(chan struct{}, 1),
		walBufferLimit: 1,
		replayDir:      tmpDir,
	}

	for _, body := range []string{`[{"a":1}]`, `[{"a":2}]`} {
		req := httptest.NewRequest(http.MethodPost, "/ingest", strings.NewReader(body))
		rr := httptest.NewRecorder()
		s.handleIngest(rr, req)
		if rr.Code != http.StatusOK {
			t.Fatalf("expected status 200, got %d", rr.Code)
		}
	}

	if got := len(s.walQueue); got != 2 {
		t.Fatalf("expected 2 queued items, got %d", got)
	}
	if s.walQueue[0].batch == nil {
		t.Fatal("expected first item to stay in memory")
	}
	if s.walQueue[1].ref == nil {
		t.Fatal("expected second item to spill to wal ref")
	}

	batch, err := s.NextBatch()
	if err != nil {
		t.Fatalf("next batch first: %v", err)
	}
	if len(batch) != 1 || types.ToInt64(batch[0].Tuple["a"]) != 1 {
		t.Fatalf("unexpected first batch: %v", batch)
	}
	batch, err = s.NextBatch()
	if err != nil {
		t.Fatalf("next batch second: %v", err)
	}
	if len(batch) != 1 || types.ToInt64(batch[0].Tuple["a"]) != 2 {
		t.Fatalf("unexpected second batch: %v", batch)
	}
}
