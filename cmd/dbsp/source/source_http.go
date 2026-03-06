package source

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/ariyn/dbsp/cmd/dbsp/config"
	"github.com/ariyn/dbsp/internal/dbsp/types"
	"github.com/ariyn/dbsp/internal/dbsp/wal"
	"github.com/buger/jsonparser"
)

var requestBodyBufferPool = sync.Pool{
	New: func() any {
		return new(bytes.Buffer)
	},
}

var errStopObjectDecode = errors.New("stop object decode")

type HTTPSource struct {
	server          *http.Server
	buffer          chan types.Batch
	pending         types.Batch
	schema          map[string]string
	requiredFields  map[string]struct{}
	fieldSpecs      []fieldSpec
	fieldSpecsByLen map[int][]fieldSpec
	fieldSpecMap    map[string]fieldSpec
	packedSchema    *types.PackedSchema
	done            chan struct{}
	once            sync.Once
	queueMu         sync.Mutex
	walQueue        []queuedWALBatch
	walDelivered    []uint64
	walAvailable    chan struct{}
	walBuffered     int
	walBufferLimit  int

	wal        *wal.LogWriter
	pendingSeq uint64

	maxBatchSize    int
	maxBatchDelay   time.Duration
	maxRequestBytes int64
	timestampUnit   string

	replayDir string
}

type queuedWALBatch struct {
	seq   uint64
	batch types.Batch
	ref   *wal.RecordRef
}

type fieldSpec struct {
	name     string
	keyBytes []byte
	typeName string
	typeKind fieldTypeKind
	slot     int
}

func NewHTTPSource(httpConfig config.HTTPSourceConfig, requiredFields map[string]struct{}) (*HTTPSource, error) {
	if httpConfig.Port == 0 {
		httpConfig.Port = 8080
	}
	if httpConfig.Path == "" {
		httpConfig.Path = "/ingest"
	}
	if httpConfig.BufferSize <= 0 {
		httpConfig.BufferSize = 1000
	}
	if httpConfig.MaxBatchSize <= 0 {
		httpConfig.MaxBatchSize = 100
	}
	if httpConfig.MaxBatchDelayMS < 0 {
		httpConfig.MaxBatchDelayMS = 0
	}
	if httpConfig.MaxRequestBytes < 0 {
		httpConfig.MaxRequestBytes = 0
	}
	if httpConfig.TimestampUnit == "" {
		httpConfig.TimestampUnit = "auto"
	}

	effectiveRequired := requiredFields
	if strings.TrimSpace(os.Getenv("DBSP_DEBUG_FIELDS")) != "" {
		if effectiveRequired == nil {
			fmt.Println("DEBUG http source requiredFields: <all>")
		} else {
			fmt.Printf("DEBUG http source requiredFields (%d): %v\n", len(effectiveRequired), effectiveRequired)
		}
		if len(httpConfig.Schema) == 0 {
			fmt.Println("DEBUG http source schema: <empty>")
		} else {
			fmt.Printf("DEBUG http source schema (%d): %v\n", len(httpConfig.Schema), httpConfig.Schema)
		}
	}

	s := &HTTPSource{
		buffer:          make(chan types.Batch, httpConfig.BufferSize),
		schema:          httpConfig.Schema,
		requiredFields:  effectiveRequired,
		fieldSpecs:      buildFieldSpecs(effectiveRequired, httpConfig.Schema),
		fieldSpecsByLen: buildFieldSpecsByLen(effectiveRequired, httpConfig.Schema),
		fieldSpecMap:    buildFieldSpecMap(effectiveRequired, httpConfig.Schema),
		done:            make(chan struct{}),
		maxBatchSize:    httpConfig.MaxBatchSize,
		maxBatchDelay:   time.Duration(httpConfig.MaxBatchDelayMS) * time.Millisecond,
		maxRequestBytes: httpConfig.MaxRequestBytes,
		timestampUnit:   httpConfig.TimestampUnit,
		walAvailable:    make(chan struct{}, 1),
		walBufferLimit:  httpConfig.BufferSize,
	}
	if shouldUsePackedSchema(effectiveRequired) {
		columns := make([]string, 0, len(s.fieldSpecs))
		for _, spec := range s.fieldSpecs {
			columns = append(columns, spec.name)
		}
		s.packedSchema = types.NewPackedSchema(columns)
	}

	if httpConfig.WALDir != "" {
		segSize, _ := config.ParseHumanBytes(httpConfig.WALSegmentSize)
		if segSize <= 0 {
			segSize = 128 * 1024 * 1024 // 128MB default
		}
		maxTotal, _ := config.ParseHumanBytes(httpConfig.WALMaxTotalSize)
		// No default for maxTotal; if 0, it means unlimited

		lw, err := wal.NewLogWriter(httpConfig.WALDir, segSize, maxTotal)
		if err != nil {
			return nil, fmt.Errorf("failed to init wal: %w", err)
		}
		s.wal = lw
		s.replayDir = httpConfig.WALDir

	}

	mux := http.NewServeMux()
	mux.HandleFunc(httpConfig.Path, s.handleIngest)
	s.server = &http.Server{Addr: fmt.Sprintf(":%d", httpConfig.Port), Handler: mux}

	go func() {
		fmt.Printf("Starting HTTP Source on port %d path %s\n", httpConfig.Port, httpConfig.Path)
		if err := s.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			fmt.Printf("HTTP Source error: %v\n", err)
			s.signalDone()
		}
	}()

	return s, nil
}

func (s *HTTPSource) handleIngest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if s.maxRequestBytes > 0 {
		r.Body = http.MaxBytesReader(w, r.Body, s.maxRequestBytes)
	}
	defer r.Body.Close()

	body, releaseBody, err := s.readRequestBody(r.Body, r.ContentLength)
	if err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			http.Error(w, "Request body too large", http.StatusRequestEntityTooLarge)
			return
		}
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}
	defer releaseBody()

	batch, err := s.parseRequestBody(body)
	if err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			http.Error(w, "Request body too large", http.StatusRequestEntityTooLarge)
			return
		}
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if len(batch) == 0 {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK (empty)"))
		return
	}

	if s.wal != nil {
		payload, err := wal.EncodeBatchGobV1(batch)
		if err != nil {
			http.Error(w, "WAL encode failure", http.StatusInternalServerError)
			return
		}
		rec := &wal.Record{
			Type:     wal.RecordTypeBatch,
			Sequence: uint64(time.Now().UnixNano()),
			Payload:  payload,
		}
		ref, err := s.wal.AppendRef(rec)
		if err != nil {
			http.Error(w, "WAL failure", http.StatusInternalServerError)
			return
		}
		s.enqueueWALBatch(rec.Sequence, batch, ref)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK"))
		return
	}

	select {
	case s.buffer <- batch:
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK"))
	default:
		http.Error(w, "Source buffer full", http.StatusServiceUnavailable)
	}
}

func (s *HTTPSource) parseRequestBody(body []byte) (types.Batch, error) {
	return s.parseRequestBodyBytes(body)
}

func (s *HTTPSource) parseRequestBodyReader(reader io.Reader) (types.Batch, error) {
	body, releaseBody, err := s.readRequestBody(reader, 0)
	if err != nil {
		return nil, err
	}
	defer releaseBody()
	return s.parseRequestBodyBytes(body)
}

func (s *HTTPSource) readRequestBody(reader io.Reader, contentLength int64) ([]byte, func(), error) {
	buf := requestBodyBufferPool.Get().(*bytes.Buffer)
	buf.Reset()
	if contentLength > 0 {
		if s.maxRequestBytes > 0 && contentLength > s.maxRequestBytes {
			requestBodyBufferPool.Put(buf)
			return nil, func() {}, &http.MaxBytesError{Limit: s.maxRequestBytes}
		}
		if contentLength <= int64(^uint(0)>>1) {
			buf.Grow(int(contentLength))
		}
	}
	if _, err := buf.ReadFrom(reader); err != nil {
		requestBodyBufferPool.Put(buf)
		return nil, func() {}, err
	}
	release := func() {
		if buf.Cap() > 4*1024*1024 {
			return
		}
		buf.Reset()
		requestBodyBufferPool.Put(buf)
	}
	return buf.Bytes(), release, nil
}

func (s *HTTPSource) parseRequestBodyBytes(body []byte) (types.Batch, error) {
	body = bytes.TrimSpace(body)
	if len(body) == 0 {
		return nil, nil
	}

	switch body[0] {
	case '[':
		capacity := s.maxBatchSize
		if capacity <= 0 {
			capacity = 16
		}
		out := make(types.Batch, 0, capacity)
		var parseErr error
		_, err := jsonparser.ArrayEach(body, func(value []byte, dataType jsonparser.ValueType, offset int, err error) {
			if parseErr != nil || err != nil {
				if err != nil {
					parseErr = err
				}
				return
			}
			if dataType != jsonparser.Object {
				parseErr = fmt.Errorf("Invalid JSON")
				return
			}
			td, ok, objErr := s.decodeObjectBytes(value)
			if objErr != nil {
				parseErr = objErr
				return
			}
			if ok {
				out = append(out, td)
			}
		})
		if parseErr != nil {
			return nil, parseErr
		}
		if err != nil {
			return nil, fmt.Errorf("Invalid JSON")
		}
		return out, nil
	case '{':
		td, ok, err := s.decodeObjectBytes(body)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, nil
		}
		return types.Batch{td}, nil
	default:
		return nil, fmt.Errorf("Invalid JSON")
	}
}

func (s *HTTPSource) decodeObjectBytes(body []byte) (types.TupleDelta, bool, error) {
	var tuple types.Tuple
	var packedValues []any
	var packedPresent []bool
	if s.packedSchema != nil {
		packedValues = make([]any, len(s.packedSchema.Columns))
		packedPresent = make([]bool, len(s.packedSchema.Columns))
	}
	remaining := len(s.requiredFields)
	err := jsonparser.ObjectEach(body, func(key []byte, value []byte, dataType jsonparser.ValueType, offset int) error {
		spec, ok := s.matchField(key)
		if !ok {
			return nil
		}
		if tuple == nil && packedValues == nil {
			tuple = make(types.Tuple, s.estimatedTupleCapacity())
		}
		decoded, err := s.decodeFieldBytes(spec, value, dataType)
		if err != nil {
			return err
		}
		if packedValues != nil {
			packedValues[spec.slot] = decoded
			packedPresent[spec.slot] = true
		} else {
			tuple[spec.name] = decoded
		}
		if remaining > 0 {
			remaining--
			if remaining == 0 {
				return errStopObjectDecode
			}
		}
		return nil
	})
	if err != nil {
		if errors.Is(err, errStopObjectDecode) {
			if packedValues != nil {
				return types.TupleDelta{Packed: types.NewPackedTupleWithPresence(s.packedSchema, packedValues, packedPresent), Count: 1}, true, nil
			}
			return types.TupleDelta{Tuple: tuple, Count: 1}, true, nil
		}
		return types.TupleDelta{}, false, fmt.Errorf("Invalid JSON")
	}
	if tuple == nil && packedValues == nil {
		return types.TupleDelta{}, false, nil
	}
	if packedValues != nil {
		return types.TupleDelta{Packed: types.NewPackedTupleWithPresence(s.packedSchema, packedValues, packedPresent), Count: 1}, true, nil
	}
	return types.TupleDelta{Tuple: tuple, Count: 1}, true, nil
}

func (s *HTTPSource) estimatedTupleCapacity() int {
	if len(s.requiredFields) > 0 {
		return len(s.requiredFields)
	}
	if len(s.schema) > 0 {
		return len(s.schema)
	}
	if len(s.fieldSpecMap) > 0 {
		return len(s.fieldSpecMap)
	}
	return 8
}

func buildFieldSpecs(requiredFields map[string]struct{}, schema map[string]string) []fieldSpec {
	if len(requiredFields) == 0 {
		return nil
	}
	names := make([]string, 0, len(requiredFields))
	for name := range requiredFields {
		names = append(names, name)
	}
	sort.Strings(names)
	specs := make([]fieldSpec, 0, len(requiredFields))
	for idx, name := range names {
		specs = append(specs, fieldSpec{
			name:     name,
			keyBytes: []byte(name),
			typeName: schema[name],
			typeKind: parseFieldTypeKind(schema[name]),
			slot:     idx,
		})
	}
	return specs
}

func shouldUsePackedSchema(requiredFields map[string]struct{}) bool {
	return len(requiredFields) > 0
}

func buildFieldSpecsByLen(requiredFields map[string]struct{}, schema map[string]string) map[int][]fieldSpec {
	if len(requiredFields) == 0 {
		return nil
	}
	byLen := make(map[int][]fieldSpec, len(requiredFields))
	for _, spec := range buildFieldSpecs(requiredFields, schema) {
		keyLen := len(spec.keyBytes)
		byLen[keyLen] = append(byLen[keyLen], spec)
	}
	return byLen
}

func buildFieldSpecMap(requiredFields map[string]struct{}, schema map[string]string) map[string]fieldSpec {
	if len(requiredFields) == 0 {
		return nil
	}
	out := make(map[string]fieldSpec, len(requiredFields))
	for _, spec := range buildFieldSpecs(requiredFields, schema) {
		out[spec.name] = spec
	}
	return out
}

func (s *HTTPSource) matchField(key []byte) (fieldSpec, bool) {
	if s.requiredFields == nil {
		name := string(key)
		return fieldSpec{name: name, typeName: s.schema[name]}, true
	}
	if len(s.fieldSpecs) == 0 {
		s.fieldSpecs = buildFieldSpecs(s.requiredFields, s.schema)
	}
	if len(s.fieldSpecsByLen) == 0 {
		s.fieldSpecsByLen = buildFieldSpecsByLen(s.requiredFields, s.schema)
	}
	for _, spec := range s.fieldSpecsByLen[len(key)] {
		if len(spec.keyBytes) == 0 || len(key) == 0 || spec.keyBytes[0] != key[0] {
			continue
		}
		if bytes.Equal(spec.keyBytes, key) {
			return spec, true
		}
	}
	return fieldSpec{}, false
}

func (s *HTTPSource) decodeFieldBytes(spec fieldSpec, value []byte, valueType jsonparser.ValueType) (any, error) {
	if spec.typeKind != fieldTypeUnknown {
		converted, err := parseValueByFieldKind(value, valueType, spec.typeKind, s.timestampUnit)
		if err != nil {
			return nil, fmt.Errorf("Invalid value for field %s: %v", spec.name, err)
		}
		return converted, nil
	}
	return parseJSONValueBytes(value, valueType)
}

func (s *HTTPSource) NextBatch() (types.Batch, error) {
	if s.usesWALQueue() {
		return s.nextWALBatch()
	}

	select {
	case <-s.done:
		return nil, nil
	default:
	}

	maxSize := s.maxBatchSize
	if maxSize <= 0 {
		maxSize = 1
	}

	batch := make(types.Batch, 0, maxSize)
	batch = s.takePending(batch, maxSize)
	if len(batch) >= maxSize {
		return batch, nil
	}

	if len(batch) == 0 {
		select {
		case <-s.done:
			return nil, nil
		case incoming := <-s.buffer:
			batch = s.appendIncoming(batch, incoming, maxSize)
		}
	}

	if s.maxBatchDelay <= 0 {
		for len(batch) < maxSize {
			select {
			case incoming := <-s.buffer:
				batch = s.appendIncoming(batch, incoming, maxSize)
			default:
				return batch, nil
			}
		}
		return batch, nil
	}

	timer := time.NewTimer(s.maxBatchDelay)
	defer timer.Stop()
	for len(batch) < maxSize {
		select {
		case <-s.done:
			return nil, nil
		case incoming := <-s.buffer:
			batch = s.appendIncoming(batch, incoming, maxSize)
		case <-timer.C:
			return batch, nil
		}
	}
	return batch, nil
}

// ReplayWAL repopulates the source from existing WAL log entries.
func (s *HTTPSource) ReplayWAL(dir string) error {
	afterSeq, err := wal.LoadReplayCursor(dir)
	if err != nil {
		return fmt.Errorf("load replay cursor: %w", err)
	}
	reader, err := wal.NewLogReader(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	s.replayDir = dir

	return reader.ReplayRefsFrom(afterSeq, func(ref *wal.RecordRef) error {
		if ref.Type != wal.RecordTypeBatch {
			return fmt.Errorf("unsupported wal record type: %d", ref.Type)
		}
		s.enqueueWALReplayRef(ref)
		return nil
	})
}

func (s *HTTPSource) AckBatchProcessed(types.Batch) error {
	if s.replayDir == "" {
		return nil
	}
	seq, ok := s.popDeliveredSeq()
	if !ok || seq == 0 {
		return nil
	}
	if err := wal.SaveReplayCursor(s.replayDir, seq); err != nil {
		return fmt.Errorf("save replay cursor: %w", err)
	}
	return nil
}

func (s *HTTPSource) usesWALQueue() bool {
	return s.wal != nil || s.replayDir != ""
}

func (s *HTTPSource) enqueueWALBatch(seq uint64, batch types.Batch, ref *wal.RecordRef) {
	item := queuedWALBatch{seq: seq}
	s.queueMu.Lock()
	if s.walBufferLimit > 0 && s.walBuffered < s.walBufferLimit {
		item.batch = batch
		s.walBuffered++
	} else {
		item.ref = ref
	}
	s.walQueue = append(s.walQueue, item)
	s.queueMu.Unlock()
	s.signalWALAvailable()
}

func (s *HTTPSource) enqueueWALReplayRef(ref *wal.RecordRef) {
	s.queueMu.Lock()
	s.walQueue = append(s.walQueue, queuedWALBatch{seq: ref.Sequence, ref: ref})
	s.queueMu.Unlock()
	s.signalWALAvailable()
}

func (s *HTTPSource) nextWALBatch() (types.Batch, error) {
	maxSize := s.maxBatchSize
	if maxSize <= 0 {
		maxSize = 1
	}
	if len(s.pending) > 0 {
		batch := s.takePending(nil, maxSize)
		if len(s.pending) == 0 && s.pendingSeq != 0 {
			s.recordDeliveredSeq(s.pendingSeq)
			s.pendingSeq = 0
		}
		return batch, nil
	}
	for {
		item, ok := s.takeNextWALBatch()
		if ok {
			batch, err := s.materializeWALBatch(item)
			if err != nil {
				return nil, err
			}
			if len(batch) == 0 {
				continue
			}
			if len(batch) <= maxSize {
				s.recordDeliveredSeq(item.seq)
				return batch, nil
			}
			out := append(types.Batch(nil), batch[:maxSize]...)
			s.pending = append(types.Batch(nil), batch[maxSize:]...)
			s.pendingSeq = item.seq
			return out, nil
		}
		select {
		case <-s.done:
			return nil, nil
		case <-s.walAvailable:
		}
	}
}

func (s *HTTPSource) takeNextWALBatch() (queuedWALBatch, bool) {
	s.queueMu.Lock()
	defer s.queueMu.Unlock()
	if len(s.walQueue) == 0 {
		return queuedWALBatch{}, false
	}
	item := s.walQueue[0]
	s.walQueue = s.walQueue[1:]
	if item.batch != nil && s.walBuffered > 0 {
		s.walBuffered--
	}
	return item, true
}

func (s *HTTPSource) materializeWALBatch(item queuedWALBatch) (types.Batch, error) {
	if item.batch != nil {
		return item.batch, nil
	}
	if item.ref == nil {
		return nil, nil
	}
	rec, err := wal.LoadRecord(item.ref)
	if err != nil {
		return nil, fmt.Errorf("load wal record: %w", err)
	}
	if rec.Type != wal.RecordTypeBatch {
		return nil, fmt.Errorf("unsupported wal record type: %d", rec.Type)
	}
	batch, err := wal.DecodeBatchGobV1(rec.Payload)
	if err != nil {
		return nil, fmt.Errorf("decode wal record: %w", err)
	}
	return batch, nil
}

func (s *HTTPSource) recordDeliveredSeq(seq uint64) {
	if seq == 0 {
		return
	}
	s.queueMu.Lock()
	s.walDelivered = append(s.walDelivered, seq)
	s.queueMu.Unlock()
}

func (s *HTTPSource) popDeliveredSeq() (uint64, bool) {
	s.queueMu.Lock()
	defer s.queueMu.Unlock()
	if len(s.walDelivered) == 0 {
		return 0, false
	}
	seq := s.walDelivered[0]
	s.walDelivered = s.walDelivered[1:]
	return seq, true
}

func (s *HTTPSource) signalWALAvailable() {
	select {
	case s.walAvailable <- struct{}{}:
	default:
	}
}

func (s *HTTPSource) takePending(batch types.Batch, maxSize int) types.Batch {
	space := maxSize - len(batch)
	if space <= 0 || len(s.pending) == 0 {
		return batch
	}
	if len(s.pending) <= space {
		batch = append(batch, s.pending...)
		s.pending = nil
		return batch
	}
	batch = append(batch, s.pending[:space]...)
	s.pending = s.pending[space:]
	return batch
}

func (s *HTTPSource) appendIncoming(batch types.Batch, incoming types.Batch, maxSize int) types.Batch {
	if len(incoming) == 0 {
		return batch
	}
	space := maxSize - len(batch)
	if space <= 0 {
		s.pending = append(s.pending, incoming...)
		return batch
	}
	if len(incoming) <= space {
		return append(batch, incoming...)
	}
	batch = append(batch, incoming[:space]...)
	s.pending = append(s.pending, incoming[space:]...)
	return batch
}

func (s *HTTPSource) Close() error {
	s.signalDone()
	if s.server == nil {
		if s.wal != nil {
			_ = s.wal.Close()
		}
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := s.server.Shutdown(ctx)
	if s.wal != nil {
		_ = s.wal.Close()
	}
	return err
}

func (s *HTTPSource) signalDone() {
	s.once.Do(func() {
		close(s.done)
	})
}
