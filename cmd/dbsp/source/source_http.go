package source

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/ariyn/dbsp/cmd/dbsp/config"
	"github.com/ariyn/dbsp/internal/dbsp/types"
	"gopkg.in/yaml.v3"
)

type HTTPSource struct {
	server   *http.Server
	buffer   chan bufferedBatch
	pending  types.Batch
	pendingSize int64
	schema   map[string]string
	done     chan struct{}
	doneOnce sync.Once

	maxBatchSize  int
	maxBatchDelay time.Duration
	maxRequestBytes int64
	maxBufferBytes  int64
	bufferedBytes   int64
	bufferMu        sync.Mutex

	autoConvert   bool
	timestampUnit string
}

type bufferedBatch struct {
	batch     types.Batch
	sizeBytes int64
}

func NewHTTPSource(cfg map[string]interface{}) (*HTTPSource, error) {
	// Parse config
	yamlBytes, err := yaml.Marshal(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal config: %w", err)
	}
	var httpConfig config.HTTPSourceConfig
	if err := yaml.Unmarshal(yamlBytes, &httpConfig); err != nil {
		return nil, fmt.Errorf("failed to parse http config: %w", err)
	}

	// Set defaults
	if httpConfig.Port == 0 {
		httpConfig.Port = 8080
	}
	if httpConfig.Path == "" {
		httpConfig.Path = "/ingest"
	}
	if httpConfig.BufferSize == 0 {
		httpConfig.BufferSize = 1000
	}
	if httpConfig.MaxBatchSize == 0 {
		httpConfig.MaxBatchSize = 100
	}
	if httpConfig.MaxBatchDelayMS < 0 {
		httpConfig.MaxBatchDelayMS = 0
	}
	if httpConfig.MaxRequestBytes < 0 {
		httpConfig.MaxRequestBytes = 0
	}
	if httpConfig.MaxBufferBytes < 0 {
		httpConfig.MaxBufferBytes = 0
	}
	if httpConfig.TimestampUnit == "" {
		httpConfig.TimestampUnit = "auto"
	}

	s := &HTTPSource{
		buffer:        make(chan bufferedBatch, httpConfig.BufferSize),
		schema:        httpConfig.Schema,
		done:          make(chan struct{}),
		maxBatchSize:  httpConfig.MaxBatchSize,
		maxBatchDelay: time.Duration(httpConfig.MaxBatchDelayMS) * time.Millisecond,
		maxRequestBytes: httpConfig.MaxRequestBytes,
		maxBufferBytes:  httpConfig.MaxBufferBytes,
		autoConvert:   httpConfig.AutoConvert,
		timestampUnit: httpConfig.TimestampUnit,
	}

	mux := http.NewServeMux()
	mux.HandleFunc(httpConfig.Path, s.handleIngest)

	s.server = &http.Server{
		Addr:    fmt.Sprintf(":%d", httpConfig.Port),
		Handler: mux,
	}

	go func() {
		fmt.Printf("Starting HTTP Source on port %d path %s\n", httpConfig.Port, httpConfig.Path)
		if err := s.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			fmt.Printf("HTTP Server error: %v\n", err)
			s.signalDone() // Signal error/shutdown
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

	body, err := io.ReadAll(r.Body)
	if err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			http.Error(w, "Request body too large", http.StatusRequestEntityTooLarge)
			return
		}
		http.Error(w, "Failed to read body", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	reserved := false
	enqueued := false
	if s.maxBufferBytes > 0 {
		if !s.tryReserveBuffer(int64(len(body))) {
			http.Error(w, "Server busy", http.StatusTooManyRequests)
			return
		}
		reserved = true
		defer func() {
			if reserved && !enqueued {
				s.releaseBuffer(int64(len(body)))
			}
		}()
	}

	// Try parsing as array first, then single object
	var records []map[string]interface{}
	if err := json.Unmarshal(body, &records); err != nil {
		// Try single object
		var record map[string]interface{}
		if err := json.Unmarshal(body, &record); err != nil {
			http.Error(w, "Invalid JSON", http.StatusBadRequest)
			return
		}
		records = []map[string]interface{}{record}
	}

	deltas := make(types.Batch, 0, len(records))
	for _, record := range records {
		tuple := make(types.Tuple)
		for k, v := range record {
			// Type conversion based on schema
			if typeName, ok := s.schema[k]; ok {
				val, err := s.parseValue(v, typeName)
				if err != nil {
					http.Error(w, fmt.Sprintf("Invalid value for field %s: %v", k, err), http.StatusBadRequest)
					return
				}
				tuple[k] = val
			} else {
				tuple[k] = v
			}
		}
		deltas = append(deltas, types.TupleDelta{
			Tuple: tuple,
			Count: 1,
		})
	}

	if len(deltas) > 0 {
		s.buffer <- bufferedBatch{batch: deltas, sizeBytes: int64(len(body))}
		enqueued = true
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK"))
}

func (s *HTTPSource) NextBatch() (types.Batch, error) {
	select {
	case <-s.done:
		return nil, nil // Server closed
	default:
	}

	maxSize := s.maxBatchSize
	if maxSize <= 0 {
		maxSize = 1
	}

	batch := make(types.Batch, 0, maxSize)
	if len(s.pending) > 0 {
		batch = s.takePending(batch, maxSize)
		if len(batch) >= maxSize {
			return batch, nil
		}
	} else {
		select {
		case <-s.done:
			return nil, nil
		case incoming := <-s.buffer:
				s.releaseBuffer(incoming.sizeBytes)
				batch = s.appendIncoming(batch, incoming.batch, incoming.sizeBytes, maxSize)
			if len(batch) >= maxSize {
				return batch, nil
			}
		}
	}

	// Fast path: no delay -> drain what's available now up to maxSize.
	if s.maxBatchDelay <= 0 {
		for len(batch) < maxSize {
			select {
			case incoming := <-s.buffer:
				s.releaseBuffer(incoming.sizeBytes)
				batch = s.appendIncoming(batch, incoming.batch, incoming.sizeBytes, maxSize)
				if len(batch) >= maxSize {
					return batch, nil
				}
			default:
				return batch, nil
			}
		}
		return batch, nil
	}

	timer := time.NewTimer(s.maxBatchDelay)
	defer func() {
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
	}()

	for len(batch) < maxSize {
		select {
		case <-s.done:
			return nil, nil
		case incoming := <-s.buffer:
			s.releaseBuffer(incoming.sizeBytes)
			batch = s.appendIncoming(batch, incoming.batch, incoming.sizeBytes, maxSize)
			if len(batch) >= maxSize {
				return batch, nil
			}
		case <-timer.C:
			return batch, nil
		}
	}
	return batch, nil
}

func (s *HTTPSource) takePending(batch types.Batch, maxSize int) types.Batch {
	space := maxSize - len(batch)
	if space <= 0 {
		return batch
	}
	if len(s.pending) <= space {
		if s.pendingSize > 0 {
			s.releaseBuffer(s.pendingSize)
		}
		batch = append(batch, s.pending...)
		s.pending = nil
		s.pendingSize = 0
		return batch
	}
	if s.pendingSize > 0 {
		consumed := s.estimatePendingSize(s.pendingSize, len(s.pending), space)
		s.releaseBuffer(consumed)
	}
	batch = append(batch, s.pending[:space]...)
	s.pending = s.pending[space:]
	if s.pendingSize > 0 {
		s.pendingSize = s.estimatePendingSize(s.pendingSize, len(s.pending)+space, len(s.pending))
	}
	return batch
}

func (s *HTTPSource) appendIncoming(batch types.Batch, incoming types.Batch, sizeBytes int64, maxSize int) types.Batch {
	if len(incoming) == 0 {
		return batch
	}
	space := maxSize - len(batch)
	if space <= 0 {
		s.pending = incoming
		s.pendingSize = sizeBytes
		return batch
	}
	if len(incoming) <= space {
		return append(batch, incoming...)
	}
	batch = append(batch, incoming[:space]...)
	s.pending = incoming[space:]
	s.pendingSize = s.estimatePendingSize(sizeBytes, len(incoming), len(s.pending))
	if s.pendingSize > 0 {
		s.reserveBuffer(s.pendingSize)
	}
	return batch
}

func (s *HTTPSource) estimatePendingSize(totalBytes int64, totalItems int, pendingItems int) int64 {
	if totalBytes <= 0 || totalItems <= 0 || pendingItems <= 0 {
		return 0
	}
	perItem := float64(totalBytes) / float64(totalItems)
	return int64(perItem * float64(pendingItems))
}

func (s *HTTPSource) tryReserveBuffer(bytes int64) bool {
	if s.maxBufferBytes <= 0 {
		return true
	}
	if bytes <= 0 {
		return true
	}
	s.bufferMu.Lock()
	defer s.bufferMu.Unlock()
	if s.bufferedBytes+bytes > s.maxBufferBytes {
		return false
	}
	s.bufferedBytes += bytes
	return true
}

func (s *HTTPSource) reserveBuffer(bytes int64) {
	if s.maxBufferBytes <= 0 {
		return
	}
	if bytes <= 0 {
		return
	}
	s.bufferMu.Lock()
	s.bufferedBytes += bytes
	s.bufferMu.Unlock()
}

func (s *HTTPSource) releaseBuffer(bytes int64) {
	if s.maxBufferBytes <= 0 {
		return
	}
	if bytes <= 0 {
		return
	}
	s.bufferMu.Lock()
	s.bufferedBytes -= bytes
	if s.bufferedBytes < 0 {
		s.bufferedBytes = 0
	}
	s.bufferMu.Unlock()
}

func (s *HTTPSource) Close() error {
	s.signalDone()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return s.server.Shutdown(ctx)
}

func (s *HTTPSource) signalDone() {
	s.doneOnce.Do(func() {
		close(s.done)
	})
}

func (s *HTTPSource) parseValue(v interface{}, colType string) (any, error) {
	switch colType {
	case "int":
		switch val := v.(type) {
		case float64:
			return int(val), nil
		case string:
			return strconv.Atoi(val)
		default:
			return nil, fmt.Errorf("expected int, got %T", v)
		}
	case "float":
		switch val := v.(type) {
		case float64:
			return val, nil
		case string:
			return strconv.ParseFloat(val, 64)
		default:
			return nil, fmt.Errorf("expected float, got %T", v)
		}
	case "bool":
		switch val := v.(type) {
		case bool:
			return val, nil
		case float64:
			if val == 1 {
				return true, nil
			} else if val == 0 {
				return false, nil
			}
			return nil, fmt.Errorf("expected bool (0/1), got %v", val)
		case string:
			return strconv.ParseBool(val)
		default:
			return nil, fmt.Errorf("expected bool, got %T", v)
		}
	case "json":
		return v, nil // passthrough
	case "timestamp":
		switch val := v.(type) {
		case string:
			t, err := time.Parse(time.RFC3339, val)
			if err != nil {
				return nil, fmt.Errorf("invalid RFC3339 timestamp: %w", err)
			}
			return t, nil
		case float64:
			return s.parseTimestamp(int64(val))
		default:
			return nil, fmt.Errorf("expected timestamp (string or number), got %T", v)
		}
	case "string":
		return fmt.Sprintf("%v", v), nil
	default:
		return v, nil
	}
}

func (s *HTTPSource) parseTimestamp(val int64) (time.Time, error) {
	unit := s.timestampUnit
	if unit == "" || unit == "auto" {
		// Auto detection
		switch {
		case val > 1e16: // ns
			unit = "ns"
		case val > 1e14: // us
			unit = "us"
		case val > 1e11: // ms
			unit = "ms"
		default: // s
			unit = "s"
		}
	}

	switch unit {
	case "s":
		return time.Unix(val, 0), nil
	case "ms":
		return time.Unix(0, val*1e6), nil
	case "us":
		return time.Unix(0, val*1e3), nil
	case "ns":
		return time.Unix(0, val), nil
	default:
		return time.Unix(val, 0), nil
	}
}
