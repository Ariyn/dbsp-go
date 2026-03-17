package source

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unsafe"

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
	buffer          chan queuedInputBatch
	pending         types.Batch
	schemaMu        sync.RWMutex
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

	stringIntern    map[string]string
	stringInternMu  sync.RWMutex
	stringInternMax int

	maxBatchSize    int
	maxBatchDelay   time.Duration
	maxRequestBytes int64
	timestampUnit   string
	sortEnabled     bool
	sortBy          []string
	sortSpillPath   string

	replayDir string
}

type queuedInputBatch struct {
	batch     types.Batch
	spillPath string
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
		buffer:          make(chan queuedInputBatch, httpConfig.BufferSize),
		schema:          cloneSchemaMap(httpConfig.Schema),
		requiredFields:  effectiveRequired,
		fieldSpecs:      buildFieldSpecs(effectiveRequired, httpConfig.Schema),
		fieldSpecsByLen: buildFieldSpecsByLen(effectiveRequired, httpConfig.Schema),
		fieldSpecMap:    buildFieldSpecMap(effectiveRequired, httpConfig.Schema),
		done:            make(chan struct{}),
		maxBatchSize:    httpConfig.MaxBatchSize,
		maxBatchDelay:   time.Duration(httpConfig.MaxBatchDelayMS) * time.Millisecond,
		maxRequestBytes: httpConfig.MaxRequestBytes,
		timestampUnit:   httpConfig.TimestampUnit,
		sortEnabled:     httpConfig.SortEnabled && len(httpConfig.SortBy) > 0,
		sortBy:          append([]string(nil), httpConfig.SortBy...),
		sortSpillPath:   strings.TrimSpace(httpConfig.SortSpillPath),
		walAvailable:    make(chan struct{}, 1),
		walBufferLimit:  httpConfig.BufferSize,
		stringIntern:    make(map[string]string),
		stringInternMax: 65536,
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
	if s.sortEnabled {
		batch = s.sortBatch(batch)
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

	item := queuedInputBatch{batch: batch}
	if s.sortEnabled && s.sortSpillPath != "" {
		spillPath, err := s.spillBatch(batch)
		if err != nil {
			http.Error(w, "sort spill failure", http.StatusInternalServerError)
			return
		}
		item = queuedInputBatch{spillPath: spillPath}
	}

	select {
	case s.buffer <- item:
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
		var valuesArena []any
		var presentArena []bool
		var arenaOffset int
		if s.packedSchema != nil {
			cols := len(s.packedSchema.Columns)
			arenaSize := cols * capacity
			valuesArena = make([]any, arenaSize)
			presentArena = make([]bool, arenaSize)
		}
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
			var pv []any
			var pp []bool
			if valuesArena != nil {
				cols := len(s.packedSchema.Columns)
				if arenaOffset+cols <= len(valuesArena) {
					pv = valuesArena[arenaOffset : arenaOffset+cols : arenaOffset+cols]
					pp = presentArena[arenaOffset : arenaOffset+cols : arenaOffset+cols]
					arenaOffset += cols
				} else {
					pv = make([]any, cols)
					pp = make([]bool, cols)
				}
			}
			td, ok, objErr := s.decodeObjectBytesWithArena(value, pv, pp)
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
	return s.decodeObjectBytesWithArena(body, nil, nil)
}

func (s *HTTPSource) decodeObjectBytesWithArena(body []byte, arenaValues []any, arenaPresent []bool) (types.TupleDelta, bool, error) {
	var tuple types.Tuple
	var packedValues []any
	var packedPresent []bool
	if s.packedSchema != nil {
		if arenaValues != nil {
			packedValues = arenaValues
			packedPresent = arenaPresent
		} else {
			packedValues = make([]any, len(s.packedSchema.Columns))
			packedPresent = make([]bool, len(s.packedSchema.Columns))
		}
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
				return types.TupleDelta{Packed: types.AdoptPackedTupleWithPresence(s.packedSchema, packedValues, packedPresent), Count: 1}, true, nil
			}
			return types.TupleDelta{Tuple: tuple, Count: 1}, true, nil
		}
		return types.TupleDelta{}, false, fmt.Errorf("Invalid JSON")
	}
	if tuple == nil && packedValues == nil {
		return types.TupleDelta{}, false, nil
	}
	if packedValues != nil {
		return types.TupleDelta{Packed: types.AdoptPackedTupleWithPresence(s.packedSchema, packedValues, packedPresent), Count: 1}, true, nil
	}
	return types.TupleDelta{Tuple: tuple, Count: 1}, true, nil
}

func (s *HTTPSource) estimatedTupleCapacity() int {
	s.schemaMu.RLock()
	defer s.schemaMu.RUnlock()
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

func cloneSchemaMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
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
	s.ensureFieldSpecs()
	s.schemaMu.RLock()
	defer s.schemaMu.RUnlock()
	if s.requiredFields == nil {
		name := string(key)
		typeName := s.schema[name]
		return fieldSpec{name: name, typeName: typeName, typeKind: parseFieldTypeKind(typeName)}, true
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

// internString returns a deduplicated string for raw bytes.
// Lookup is zero-copy via unsafe.String; allocation only happens on cache miss.
// Strings longer than 256 bytes are returned as-is without interning.
func (s *HTTPSource) internString(raw []byte) string {
	if len(raw) > 256 {
		return string(raw)
	}
	// Zero-copy key for map lookup — valid only within this scope, never stored.
	key := unsafe.String(unsafe.SliceData(raw), len(raw))
	s.stringInternMu.RLock()
	if existing, ok := s.stringIntern[key]; ok {
		s.stringInternMu.RUnlock()
		return existing
	}
	s.stringInternMu.RUnlock()

	s.stringInternMu.Lock()
	defer s.stringInternMu.Unlock()
	if existing, ok := s.stringIntern[key]; ok {
		return existing
	}
	if len(s.stringIntern) >= s.stringInternMax {
		return string(raw)
	}
	str := string(raw)
	s.stringIntern[str] = str
	return str
}

func (s *HTTPSource) decodeFieldBytes(spec fieldSpec, value []byte, valueType jsonparser.ValueType) (any, error) {
	activeSpec := spec
	if activeSpec.typeKind == fieldTypeUnknown {
		if inferred := inferFieldTypeFromObservedValue(activeSpec.name, value, valueType); inferred != fieldTypeUnknown {
			activeSpec = s.updateFieldSpecType(activeSpec.name, inferred)
		}
	}
	if shouldPromoteToFloat(activeSpec.typeKind, value, valueType) {
		activeSpec = s.updateFieldSpecType(activeSpec.name, fieldTypeFloat)
	}
	if activeSpec.typeKind == fieldTypeUnknown {
		return parseJSONValueBytes(value, valueType)
	}
	if activeSpec.typeKind == fieldTypeString && valueType == jsonparser.String {
		return s.internString(value), nil
	}
	converted, err := parseValueByFieldKind(value, valueType, activeSpec.typeKind, s.timestampUnit)
	if err == nil {
		return converted, nil
	}
	if activeSpec.typeKind == fieldTypeInt {
		promoted := s.updateFieldSpecType(activeSpec.name, fieldTypeFloat)
		converted, promotedErr := parseValueByFieldKind(value, valueType, promoted.typeKind, s.timestampUnit)
		if promotedErr == nil {
			return converted, nil
		}
	}
	return nil, fmt.Errorf("Invalid value for field %s: %v", activeSpec.name, err)
}

func (s *HTTPSource) ensureFieldSpecs() {
	if s.requiredFields == nil {
		return
	}
	s.schemaMu.RLock()
	ready := len(s.fieldSpecsByLen) > 0
	s.schemaMu.RUnlock()
	if ready {
		return
	}
	s.schemaMu.Lock()
	defer s.schemaMu.Unlock()
	if len(s.fieldSpecsByLen) == 0 {
		s.refreshFieldSpecsLocked()
	}
}

func inferFieldTypeFromObservedValue(name string, raw []byte, valueType jsonparser.ValueType) fieldTypeKind {
	switch valueType {
	case jsonparser.Number:
		if looksLikeTimestampField(name) {
			return fieldTypeTimestamp
		}
		if _, err := jsonparser.ParseInt(raw); err == nil {
			return fieldTypeInt
		}
		if _, err := jsonparser.ParseFloat(raw); err == nil {
			return fieldTypeFloat
		}
	case jsonparser.String:
		trimmed := strings.TrimSpace(string(raw))
		if trimmed == "" {
			return fieldTypeString
		}
		if _, err := time.Parse(time.RFC3339, trimmed); err == nil {
			return fieldTypeTimestamp
		}
		if looksLikeTimestampField(name) {
			if _, err := strconv.ParseInt(trimmed, 10, 64); err == nil {
				return fieldTypeTimestamp
			}
		}
		return fieldTypeString
	case jsonparser.Boolean:
		return fieldTypeBool
	case jsonparser.Object, jsonparser.Array:
		return fieldTypeJSON
	case jsonparser.Null:
		return fieldTypeUnknown
	}
	return fieldTypeUnknown
}

func looksLikeTimestampField(name string) bool {
	trimmed := strings.ToLower(strings.TrimSpace(name))
	if trimmed == "timestamp" || trimmed == "ts" {
		return true
	}
	return strings.HasSuffix(trimmed, "_timestamp") || strings.HasSuffix(trimmed, "_ts")
}

func shouldPromoteToFloat(kind fieldTypeKind, raw []byte, valueType jsonparser.ValueType) bool {
	if kind != fieldTypeInt {
		return false
	}
	switch valueType {
	case jsonparser.Number:
		if _, err := jsonparser.ParseInt(raw); err == nil {
			return false
		}
		_, err := jsonparser.ParseFloat(raw)
		return err == nil
	case jsonparser.String:
		trimmed := strings.TrimSpace(string(raw))
		if trimmed == "" {
			return false
		}
		if _, err := strconv.ParseInt(trimmed, 10, 64); err == nil {
			return false
		}
		_, err := strconv.ParseFloat(trimmed, 64)
		return err == nil
	default:
		return false
	}
}

func fieldTypeName(kind fieldTypeKind) string {
	switch kind {
	case fieldTypeInt:
		return "int"
	case fieldTypeFloat:
		return "float"
	case fieldTypeBool:
		return "bool"
	case fieldTypeJSON:
		return "json"
	case fieldTypeTimestamp:
		return "timestamp"
	case fieldTypeString:
		return "string"
	default:
		return ""
	}
}

func (s *HTTPSource) updateFieldSpecType(name string, kind fieldTypeKind) fieldSpec {
	if kind == fieldTypeUnknown || name == "" {
		return fieldSpec{name: name}
	}
	s.schemaMu.Lock()
	defer s.schemaMu.Unlock()
	if s.requiredFields == nil {
		typeName := fieldTypeName(kind)
		if typeName != "" {
			if s.schema == nil {
				s.schema = make(map[string]string)
			}
			if existing := parseFieldTypeKind(s.schema[name]); existing == fieldTypeUnknown || (existing == fieldTypeInt && kind == fieldTypeFloat) {
				s.schema[name] = typeName
			}
		}
		resolvedName := s.schema[name]
		return fieldSpec{name: name, typeName: resolvedName, typeKind: parseFieldTypeKind(resolvedName)}
	}
	current := s.fieldSpecMap[name]
	merged := mergeFieldTypes(current.typeKind, kind)
	if merged == fieldTypeUnknown {
		return current
	}
	typeName := fieldTypeName(merged)
	if typeName == "" {
		return current
	}
	if s.schema == nil {
		s.schema = make(map[string]string, len(s.requiredFields))
	}
	if current.typeKind == merged && s.schema[name] == typeName {
		return current
	}
	s.schema[name] = typeName
	s.refreshFieldSpecsLocked()
	return s.fieldSpecMap[name]
}

func mergeFieldTypes(current fieldTypeKind, next fieldTypeKind) fieldTypeKind {
	if current == fieldTypeUnknown {
		return next
	}
	if current == next || next == fieldTypeUnknown {
		return current
	}
	if (current == fieldTypeInt && next == fieldTypeFloat) || (current == fieldTypeFloat && next == fieldTypeInt) {
		return fieldTypeFloat
	}
	return current
}

func (s *HTTPSource) refreshFieldSpecsLocked() {
	s.fieldSpecs = buildFieldSpecs(s.requiredFields, s.schema)
	s.fieldSpecsByLen = buildFieldSpecsByLen(s.requiredFields, s.schema)
	s.fieldSpecMap = buildFieldSpecMap(s.requiredFields, s.schema)
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
			var err error
			batch, err = s.appendIncoming(batch, incoming, maxSize)
			if err != nil {
				return nil, err
			}
		}
	}

	if s.maxBatchDelay <= 0 {
		for len(batch) < maxSize {
			select {
			case incoming := <-s.buffer:
				var err error
				batch, err = s.appendIncoming(batch, incoming, maxSize)
				if err != nil {
					return nil, err
				}
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
			var err error
			batch, err = s.appendIncoming(batch, incoming, maxSize)
			if err != nil {
				return nil, err
			}
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

func (s *HTTPSource) appendIncoming(batch types.Batch, incoming queuedInputBatch, maxSize int) (types.Batch, error) {
	resolved, err := s.materializeQueuedInput(incoming)
	if err != nil {
		return nil, err
	}
	if len(resolved) == 0 {
		return batch, nil
	}
	space := maxSize - len(batch)
	if space <= 0 {
		s.pending = append(s.pending, resolved...)
		return batch, nil
	}
	if len(resolved) <= space {
		return append(batch, resolved...), nil
	}
	batch = append(batch, resolved[:space]...)
	s.pending = append(s.pending, resolved[space:]...)
	return batch, nil
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

func (s *HTTPSource) materializeQueuedInput(incoming queuedInputBatch) (types.Batch, error) {
	if incoming.batch != nil {
		return incoming.batch, nil
	}
	if incoming.spillPath == "" {
		return nil, nil
	}
	payload, err := os.ReadFile(incoming.spillPath)
	if err != nil {
		return nil, fmt.Errorf("read sort spill: %w", err)
	}
	batch, err := wal.DecodeBatchGobV1(payload)
	if err != nil {
		return nil, fmt.Errorf("decode sort spill: %w", err)
	}
	if removeErr := os.Remove(incoming.spillPath); removeErr != nil && !os.IsNotExist(removeErr) {
		return nil, fmt.Errorf("cleanup sort spill: %w", removeErr)
	}
	return batch, nil
}

func (s *HTTPSource) spillBatch(batch types.Batch) (string, error) {
	if strings.TrimSpace(s.sortSpillPath) == "" {
		return "", fmt.Errorf("sort spill path is empty")
	}
	if err := os.MkdirAll(s.sortSpillPath, 0o755); err != nil {
		return "", fmt.Errorf("create sort spill dir: %w", err)
	}
	payload, err := wal.EncodeBatchGobV1(batch)
	if err != nil {
		return "", fmt.Errorf("encode sort spill: %w", err)
	}
	name := fmt.Sprintf("%d-%d.batch", time.Now().UnixNano(), len(batch))
	path := filepath.Join(s.sortSpillPath, name)
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, payload, 0o644); err != nil {
		return "", fmt.Errorf("write sort spill: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return "", fmt.Errorf("finalize sort spill: %w", err)
	}
	return path, nil
}

func (s *HTTPSource) sortBatch(batch types.Batch) types.Batch {
	if len(batch) <= 1 || len(s.sortBy) == 0 {
		return batch
	}
	out := append(types.Batch(nil), batch...)
	sort.SliceStable(out, func(i, j int) bool {
		return s.lessTupleDelta(out[i], out[j])
	})
	return out
}

func (s *HTTPSource) lessTupleDelta(left, right types.TupleDelta) bool {
	for _, col := range s.sortBy {
		lv, _ := left.Get(col)
		rv, _ := right.Get(col)
		cmp := compareSortValues(lv, rv)
		if cmp < 0 {
			return true
		}
		if cmp > 0 {
			return false
		}
	}
	return false
}

func compareSortValues(a, b any) int {
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		return 1
	}
	if b == nil {
		return -1
	}
	switch av := a.(type) {
	case int:
		switch bv := b.(type) {
		case int:
			if av < bv {
				return -1
			}
			if av > bv {
				return 1
			}
			return 0
		case int64:
			return compareSortValues(int64(av), bv)
		case float64:
			return compareSortValues(float64(av), bv)
		}
	case int64:
		switch bv := b.(type) {
		case int:
			return compareSortValues(av, int64(bv))
		case int64:
			if av < bv {
				return -1
			}
			if av > bv {
				return 1
			}
			return 0
		case float64:
			return compareSortValues(float64(av), bv)
		}
	case float64:
		switch bv := b.(type) {
		case int:
			return compareSortValues(av, float64(bv))
		case int64:
			return compareSortValues(av, float64(bv))
		case float64:
			if av < bv {
				return -1
			}
			if av > bv {
				return 1
			}
			return 0
		}
	case string:
		if bv, ok := b.(string); ok {
			if av < bv {
				return -1
			}
			if av > bv {
				return 1
			}
			return 0
		}
	case time.Time:
		if bv, ok := b.(time.Time); ok {
			if av.Before(bv) {
				return -1
			}
			if av.After(bv) {
				return 1
			}
			return 0
		}
	}
	as := fmt.Sprint(a)
	bs := fmt.Sprint(b)
	if as < bs {
		return -1
	}
	if as > bs {
		return 1
	}
	return 0
}
