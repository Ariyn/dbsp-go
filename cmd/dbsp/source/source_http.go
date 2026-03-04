package source

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/ariyn/dbsp/cmd/dbsp/config"
	"github.com/ariyn/dbsp/internal/dbsp/types"
)

type HTTPSource struct {
	server  *http.Server
	buffer  chan types.Batch
	pending types.Batch
	schema  map[string]string
	done    chan struct{}
	once    sync.Once

	maxBatchSize    int
	maxBatchDelay   time.Duration
	maxRequestBytes int64
	timestampUnit   string
}

func NewHTTPSource(httpConfig config.HTTPSourceConfig) (*HTTPSource, error) {
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

	s := &HTTPSource{
		buffer:          make(chan types.Batch, httpConfig.BufferSize),
		schema:          httpConfig.Schema,
		done:            make(chan struct{}),
		maxBatchSize:    httpConfig.MaxBatchSize,
		maxBatchDelay:   time.Duration(httpConfig.MaxBatchDelayMS) * time.Millisecond,
		maxRequestBytes: httpConfig.MaxRequestBytes,
		timestampUnit:   httpConfig.TimestampUnit,
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

	batch, err := s.parseRequestBody(body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if len(batch) == 0 {
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
	var records []map[string]interface{}
	if err := json.Unmarshal(body, &records); err != nil {
		var single map[string]interface{}
		if err2 := json.Unmarshal(body, &single); err2 != nil {
			return nil, fmt.Errorf("Invalid JSON")
		}
		records = []map[string]interface{}{single}
	}

	batch := make(types.Batch, 0, len(records))
	for _, record := range records {
		tuple := make(types.Tuple, len(record))
		for key, value := range record {
			typeName, ok := s.schema[key]
			if !ok {
				tuple[key] = value
				continue
			}
			converted, err := parseValueByType(value, typeName, s.timestampUnit)
			if err != nil {
				return nil, fmt.Errorf("Invalid value for field %s: %v", key, err)
			}
			tuple[key] = converted
		}
		batch = append(batch, types.TupleDelta{Tuple: tuple, Count: 1})
	}
	return batch, nil
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
