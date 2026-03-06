package source

import (
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/ariyn/dbsp/cmd/dbsp/config"
	"github.com/ariyn/dbsp/internal/dbsp/ir"
	"github.com/ariyn/dbsp/internal/dbsp/op"
	sqlconv "github.com/ariyn/dbsp/internal/dbsp/sql"
	"github.com/ariyn/dbsp/internal/dbsp/types"
	"github.com/ariyn/dbsp/internal/dbsp/wal"
)

const packedWindowQuery = `
WITH lagged_data AS (
	SELECT
		timestamp,
		panel_position,
		plant_id,
		local_date,
		v_out,
		i_out,
		v_in,
		temp,
		LAG(timestamp) OVER (PARTITION BY panel_position ORDER BY timestamp) AS timestamp_last,
		LAG(v_out) OVER (PARTITION BY panel_position ORDER BY timestamp) AS v_out_last,
		LAG(i_out) OVER (PARTITION BY panel_position ORDER BY timestamp) AS i_out_last
	FROM events
),
power_calc AS (
	SELECT
		timestamp_last AS timestamp_start,
		timestamp AS timestamp_end,
		timestamp,
		panel_position,
		plant_id,
		local_date,
		v_out,
		i_out,
		v_in,
		temp,
		v_out * i_out AS p_out,
		v_out_last * i_out_last AS p_out_last,
		(timestamp::DOUBLE / 1000000000.0) - (timestamp_last::DOUBLE / 1000000000.0) AS timedelta_second
	FROM lagged_data
	WHERE timestamp_last IS NOT NULL AND timestamp IS NOT NULL
),
combined_data AS (
	SELECT
		panel_position AS id,
		plant_id,
		local_date,
		TIME_BUCKET(INTERVAL '5 min', timestamp::TIMESTAMP) AS binned_date,
		ROUND(AVG(i_out), 2) AS i_out,
		ROUND(AVG(i_out * v_out), 2) AS p,
		ROUND(AVG(v_in), 2) AS v_in,
		ROUND(AVG(v_out), 2) AS v_out,
		ROUND(AVG(temp), 2) AS temp,
		SUM((p_out + p_out_last) * timedelta_second / 2.0 / 3600.0) AS energy
	FROM power_calc
	GROUP BY id, plant_id, local_date, binned_date
),
final_data AS (
	SELECT
		i_out,
		p,
		v_in,
		v_out,
		temp,
		energy,
		SUM(energy) OVER (PARTITION BY id ORDER BY binned_date) AS cumulative_energy,
		id,
		plant_id,
		local_date,
		STRFTIME(binned_date, '%H:%M:%S') AS date,
		binned_date AS timestamp
	FROM combined_data
)
SELECT *
FROM final_data
WHERE id = '0e02e183-c1b2-4492-9eda-26b08892e427.0.0'
ORDER BY date
PARTITION BY plant_id, local_date
`

func tupleForTest(td *types.TupleDelta) types.Tuple {
	return td.EnsureTuple()
}

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
	tuple := tupleForTest(&batch[0])
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
	if _, ok := tupleForTest(&batch[0])["b"]; !ok {
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
	if got := types.ToInt64(tupleForTest(&batch[0])["a"]); got != 2 {
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
	tuple := tupleForTest(&batch[0])
	if _, ok := tuple["v_out"]; ok {
		t.Fatalf("did not expect v_out when filtering is enabled")
	}
	if _, ok := tuple["plant_id"]; !ok {
		t.Fatalf("expected plant_id to be kept")
	}
	if _, ok := tuple["local_date"]; !ok {
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
	tuple := tupleForTest(&batch[0])
	if _, ok := tuple["a"]; !ok {
		t.Fatalf("expected field a")
	}
	if _, ok := tuple["b"]; ok {
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
	tuple := tupleForTest(&batch[0])
	meta, ok := tuple["meta"].(map[string]any)
	if !ok {
		t.Fatalf("expected meta object, got %T", tuple["meta"])
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

func TestParseLargePackedBatchKeepsTimestampPresent(t *testing.T) {
	required := map[string]struct{}{
		"timestamp":      {},
		"panel_position": {},
		"plant_id":       {},
		"local_date":     {},
		"v_out":          {},
		"i_out":          {},
		"v_in":           {},
		"temp":           {},
	}
	s, err := NewHTTPSource(config.HTTPSourceConfig{TimestampUnit: "ns"}, required)
	if err != nil {
		t.Fatalf("NewHTTPSource: %v", err)
	}
	defer s.Close()

	var body strings.Builder
	body.WriteByte('[')
	for idx := 0; idx < 2000; idx++ {
		if idx > 0 {
			body.WriteByte(',')
		}
		body.WriteString(fmt.Sprintf(`{"timestamp":%d,"panel_position":"panel-1","plant_id":"plant-a","local_date":"2026-02-27","v_out":10.0,"i_out":2.0,"v_in":100.0,"temp":25.0}`,
			(1772175600+idx*60)*1_000_000_000,
		))
	}
	body.WriteByte(']')

	batch, err := s.parseRequestBodyReader(strings.NewReader(body.String()))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(batch) != 2000 {
		t.Fatalf("expected 2000 records, got %d", len(batch))
	}
	for idx := range batch {
		if _, ok := batch[idx].Get("timestamp"); !ok {
			t.Fatalf("expected timestamp in parsed row %d", idx)
		}
	}
}

func TestWALQueueChunkingKeepsPackedTimestampPresent(t *testing.T) {
	required := map[string]struct{}{
		"timestamp":      {},
		"panel_position": {},
		"plant_id":       {},
		"local_date":     {},
		"v_out":          {},
		"i_out":          {},
		"v_in":           {},
		"temp":           {},
	}
	s, err := NewHTTPSource(config.HTTPSourceConfig{TimestampUnit: "ns", MaxBatchSize: 1000}, required)
	if err != nil {
		t.Fatalf("NewHTTPSource: %v", err)
	}
	defer s.Close()
	s.replayDir = "test-replay"

	batch := make(types.Batch, 0, 2000)
	for idx := 0; idx < 2000; idx++ {
		batch = append(batch, types.TupleDelta{Packed: types.NewPackedTupleWithPresence(
			s.packedSchema,
			[]any{time.Unix(0, int64((1772175600+idx*60)*1_000_000_000)).UTC(), "panel-1", "plant-a", "2026-02-27", 10.0, 2.0, 100.0, 25.0},
			[]bool{true, true, true, true, true, true, true, true},
		), Count: 1})
	}
	s.enqueueWALBatch(1, batch, nil)

	first, err := s.NextBatch()
	if err != nil {
		t.Fatalf("first NextBatch: %v", err)
	}
	if len(first) != 1000 {
		t.Fatalf("expected first chunk size 1000, got %d", len(first))
	}
	for idx := range first {
		if _, ok := first[idx].Get("timestamp"); !ok {
			t.Fatalf("expected timestamp in first chunk row %d", idx)
		}
	}

	second, err := s.NextBatch()
	if err != nil {
		t.Fatalf("second NextBatch: %v", err)
	}
	if len(second) != 1000 {
		t.Fatalf("expected second chunk size 1000, got %d", len(second))
	}
	for idx := range second {
		if _, ok := second[idx].Get("timestamp"); !ok {
			t.Fatalf("expected timestamp in second chunk row %d", idx)
		}
	}
}

func TestPackedLargeBatchExecutesWindowChain(t *testing.T) {
	logicalPlan, err := sqlconv.ParseQueryToLogicalPlan(packedWindowQuery)
	if err != nil {
		t.Fatalf("ParseQueryToLogicalPlan: %v", err)
	}
	required := ir.CollectRequiredInputColumns(logicalPlan)
	hints := ir.CollectRequiredInputTypeHints(logicalPlan)
	root, err := sqlconv.ParseQueryToIncrementalDBSP(packedWindowQuery)
	if err != nil {
		t.Fatalf("ParseQueryToIncrementalDBSP: %v", err)
	}
	httpCfg := config.HTTPSourceConfig{TimestampUnit: "ns", Schema: hints}
	s, err := NewHTTPSource(httpCfg, required)
	if err != nil {
		t.Fatalf("NewHTTPSource: %v", err)
	}
	defer s.Close()

	var body strings.Builder
	body.WriteByte('[')
	for idx := 0; idx < 1000; idx++ {
		if idx > 0 {
			body.WriteByte(',')
		}
		body.WriteString(fmt.Sprintf(`{"timestamp":%d,"panel_position":"0e02e183-c1b2-4492-9eda-26b08892e427.0.0","plant_id":"plant-a","local_date":"2026-02-27","v_out":10.0,"i_out":2.0,"v_in":100.0,"temp":25.0}`,
			(1772175600+idx*60)*1_000_000_000,
		))
	}
	body.WriteByte(']')

	batch, err := s.parseRequestBodyReader(strings.NewReader(body.String()))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(batch) != 1000 {
		t.Fatalf("expected 1000 records, got %d", len(batch))
	}
	if _, err := op.Execute(root, batch[:1]); err != nil {
		t.Fatalf("warmup Execute failed: %v", err)
	}
	if _, err := op.Execute(root, batch); err != nil {
		t.Fatalf("Execute failed: %v", err)
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
	if len(batch) != 1 || types.ToInt64(tupleForTest(&batch[0])["a"]) != 1 {
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
	if len(batch) != 1 || types.ToInt64(tupleForTest(&batch[0])["a"]) != 1 {
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
	if len(batch) != 1 || types.ToInt64(tupleForTest(&batch[0])["a"]) != 2 {
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
	if len(batch) != 1 || types.ToInt64(tupleForTest(&batch[0])["a"]) != 7 {
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
		if len(batch) != 1 || types.ToInt64(tupleForTest(&batch[0])["a"]) != 5 {
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
	if len(batch) != 1 || types.ToInt64(tupleForTest(&batch[0])["a"]) != 5 {
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
	if len(batch) != 2 || types.ToInt64(tupleForTest(&batch[0])["a"]) != 1 || types.ToInt64(tupleForTest(&batch[1])["a"]) != 2 {
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
	if len(batch) != 2 || types.ToInt64(tupleForTest(&batch[0])["a"]) != 3 || types.ToInt64(tupleForTest(&batch[1])["a"]) != 4 {
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
	if len(batch) != 1 || types.ToInt64(tupleForTest(&batch[0])["a"]) != 5 {
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
	if len(batch) != 1 || types.ToInt64(tupleForTest(&batch[0])["a"]) != 1 {
		t.Fatalf("unexpected first batch: %v", batch)
	}
	batch, err = s.NextBatch()
	if err != nil {
		t.Fatalf("next batch second: %v", err)
	}
	if len(batch) != 1 || types.ToInt64(tupleForTest(&batch[0])["a"]) != 2 {
		t.Fatalf("unexpected second batch: %v", batch)
	}
}
