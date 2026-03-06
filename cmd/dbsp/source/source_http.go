package source

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/ariyn/dbsp/cmd/dbsp/config"
	"github.com/ariyn/dbsp/internal/dbsp/types"
	"github.com/ariyn/dbsp/internal/dbsp/wal"
)

type HTTPSource struct {
	server         *http.Server
	buffer         chan types.Batch
	pending        types.Batch
	schema         map[string]string
	requiredFields map[string]struct{}
	done           chan struct{}
	once           sync.Once

	wal *wal.LogWriter

	maxBatchSize    int
	maxBatchDelay   time.Duration
	maxRequestBytes int64
	timestampUnit   string
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
	if requiredFields != nil && len(httpConfig.Schema) == 0 {
		effectiveRequired = nil
	}
	if requiredFields != nil && httpConfig.Schema != nil && len(httpConfig.Schema) > 0 {
		needsSchema := false
		for key := range requiredFields {
			if _, ok := httpConfig.Schema[key]; !ok {
				needsSchema = true
				break
			}
		}
		if needsSchema {
			effectiveRequired = make(map[string]struct{}, len(requiredFields)+len(httpConfig.Schema))
			for key := range requiredFields {
				effectiveRequired[key] = struct{}{}
			}
			for key := range httpConfig.Schema {
				effectiveRequired[key] = struct{}{}
			}
		}
	}
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
		done:            make(chan struct{}),
		maxBatchSize:    httpConfig.MaxBatchSize,
		maxBatchDelay:   time.Duration(httpConfig.MaxBatchDelayMS) * time.Millisecond,
		maxRequestBytes: httpConfig.MaxRequestBytes,
		timestampUnit:   httpConfig.TimestampUnit,
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

	var walBuf bytes.Buffer
	reader := io.Reader(r.Body)
	if s.wal != nil {
		reader = io.TeeReader(r.Body, &walBuf)
	}

	batch, err := s.parseRequestBodyReader(reader)
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
		if strings.TrimSpace(os.Getenv("DBSP_DEBUG_INGEST")) != "" {
			fmt.Println("DEBUG ingest: empty batch")
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK (empty)"))
		return
	}
	if strings.TrimSpace(os.Getenv("DBSP_DEBUG_INGEST")) != "" {
		fmt.Printf("DEBUG ingest: batch size=%d\n", len(batch))
		if len(batch) > 0 {
			fmt.Printf("DEBUG ingest: first tuple keys=%v\n", tupleKeysLocal(batch[0].Tuple))
		}
	}

	if s.wal != nil {
		body := walBuf.Bytes()
		rec := &wal.Record{
			Type:     wal.RecordTypeData,
			Sequence: uint64(time.Now().UnixNano()),
			Payload:  body,
		}
		if err := s.wal.Append(rec); err != nil {
			http.Error(w, "WAL failure", http.StatusInternalServerError)
			return
		}
	}

	select {
	case s.buffer <- batch:
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK"))
	default:
		if strings.TrimSpace(os.Getenv("DBSP_DEBUG_INGEST")) != "" {
			fmt.Println("DEBUG ingest: buffer full")
		}
		http.Error(w, "Source buffer full", http.StatusServiceUnavailable)
	}
}

func (s *HTTPSource) parseRequestBody(body []byte) (types.Batch, error) {
	return s.parseRequestBodyReader(bytes.NewReader(body))
}

func (s *HTTPSource) parseRequestBodyReader(reader io.Reader) (types.Batch, error) {
	dec := json.NewDecoder(reader)
	dec.UseNumber()

	first, err := dec.Token()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return nil, nil
		}
		return nil, fmt.Errorf("Invalid JSON")
	}

	var out types.Batch
	switch tok := first.(type) {
	case json.Delim:
		switch tok {
		case '[':
			for dec.More() {
				td, err := s.decodeObject(dec)
				if err != nil {
					return nil, err
				}
				if td != nil {
					out = append(out, *td)
				}
			}
			if _, err := dec.Token(); err != nil {
				return nil, fmt.Errorf("Invalid JSON")
			}
		case '{':
			td, err := s.decodeObjectFromOpen(dec)
			if err != nil {
				return nil, err
			}
			if td != nil {
				out = append(out, *td)
			}
		default:
			return nil, fmt.Errorf("Invalid JSON")
		}
	default:
		return nil, fmt.Errorf("Invalid JSON")
	}

	return out, nil
}

func tupleKeysLocal(t types.Tuple) []string {
	if t == nil {
		return nil
	}
	keys := make([]string, 0, len(t))
	for k := range t {
		keys = append(keys, k)
	}
	return keys
}

func (s *HTTPSource) decodeObject(dec *json.Decoder) (*types.TupleDelta, error) {
	if tok, err := dec.Token(); err != nil {
		return nil, fmt.Errorf("Invalid JSON")
	} else if d, ok := tok.(json.Delim); !ok || d != '{' {
		return nil, fmt.Errorf("Invalid JSON")
	}
	return s.decodeObjectFromOpen(dec)
}

func (s *HTTPSource) decodeObjectFromOpen(dec *json.Decoder) (*types.TupleDelta, error) {
	tuple := make(types.Tuple)
	for dec.More() {
		keyTok, err := dec.Token()
		if err != nil {
			return nil, fmt.Errorf("Invalid JSON")
		}
		key, ok := keyTok.(string)
		if !ok {
			return nil, fmt.Errorf("Invalid JSON")
		}
		if !s.shouldKeepField(key) {
			if err := skipJSONValue(dec); err != nil {
				return nil, err
			}
			continue
		}
		value, err := s.decodeFieldValue(dec, key)
		if err != nil {
			return nil, err
		}
		tuple[key] = value
	}
	if _, err := dec.Token(); err != nil {
		return nil, fmt.Errorf("Invalid JSON")
	}
	return &types.TupleDelta{Tuple: tuple, Count: 1}, nil
}

func (s *HTTPSource) shouldKeepField(key string) bool {
	if s.requiredFields == nil {
		return true
	}
	if s.schema != nil {
		if _, ok := s.schema[key]; ok {
			return true
		}
	}
	_, ok := s.requiredFields[key]
	return ok
}

func (s *HTTPSource) decodeFieldValue(dec *json.Decoder, key string) (any, error) {
	var raw json.RawMessage
	if err := dec.Decode(&raw); err != nil {
		return nil, fmt.Errorf("Invalid JSON")
	}
	val, err := decodeRawValue(raw)
	if err != nil {
		return nil, fmt.Errorf("Invalid JSON")
	}
	if typeName, ok := s.schema[key]; ok {
		converted, err := parseValueByType(val, typeName, s.timestampUnit)
		if err != nil {
			return nil, fmt.Errorf("Invalid value for field %s: %v", key, err)
		}
		return converted, nil
	}
	return val, nil
}

func decodeRawValue(raw json.RawMessage) (any, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var out any
	if err := dec.Decode(&out); err != nil {
		return nil, err
	}
	return out, nil
}

func skipJSONValue(dec *json.Decoder) error {
	tok, err := dec.Token()
	if err != nil {
		return fmt.Errorf("Invalid JSON")
	}
	d, ok := tok.(json.Delim)
	if !ok {
		return nil
	}
	switch d {
	case '{':
		for dec.More() {
			if _, err := dec.Token(); err != nil {
				return fmt.Errorf("Invalid JSON")
			}
			if err := skipJSONValue(dec); err != nil {
				return err
			}
		}
		if _, err := dec.Token(); err != nil {
			return fmt.Errorf("Invalid JSON")
		}
	case '[':
		for dec.More() {
			if err := skipJSONValue(dec); err != nil {
				return err
			}
		}
		if _, err := dec.Token(); err != nil {
			return fmt.Errorf("Invalid JSON")
		}
	}
	return nil
}

func (s *HTTPSource) NextBatch() (types.Batch, error) {
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
	reader, err := wal.NewLogReader(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	return reader.Replay(func(rec *wal.Record) error {
		if rec.Type != wal.RecordTypeData {
			return nil
		}
		batch, err := s.parseRequestBody(rec.Payload)
		if err != nil {
			return fmt.Errorf("failed to parse wal record: %w", err)
		}
		if len(batch) > 0 {
			// In replay mode, we block if buffer is full to preserve order
			s.buffer <- batch
		}
		return nil
	})
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
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return s.server.Shutdown(ctx)
}

func (s *HTTPSource) signalDone() {
	s.once.Do(func() {
		close(s.done)
	})
}
