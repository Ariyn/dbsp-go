package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/ariyn/dbsp/internal/dbsp/types"
	"gopkg.in/yaml.v3"
)

type HTTPSourceConfig struct {
	Port   int               `yaml:"port"`
	Path   string            `yaml:"path"`
	Schema map[string]string `yaml:"schema"`
}

type HTTPSource struct {
	server *http.Server
	buffer chan types.TupleDelta
	schema map[string]string
	done   chan struct{}
}

func NewHTTPSource(config map[string]interface{}) (*HTTPSource, error) {
	// Parse config
	yamlBytes, err := yaml.Marshal(config)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal config: %w", err)
	}
	var httpConfig HTTPSourceConfig
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

	s := &HTTPSource{
		buffer: make(chan types.TupleDelta, 1000), // Buffer size 1000
		schema: httpConfig.Schema,
		done:   make(chan struct{}),
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
			close(s.done) // Signal error/shutdown
		}
	}()

	return s, nil
}

func (s *HTTPSource) handleIngest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Failed to read body", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

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

	for _, record := range records {
		tuple := make(types.Tuple)
		for k, v := range record {
			// Type conversion based on schema
			if typeName, ok := s.schema[k]; ok {
				val, err := parseValueFromInterface(v, typeName)
				if err != nil {
					http.Error(w, fmt.Sprintf("Invalid value for field %s: %v", k, err), http.StatusBadRequest)
					return
				}
				tuple[k] = val
			} else {
				tuple[k] = v
			}
		}

		// Default to insert (+1)
		s.buffer <- types.TupleDelta{
			Tuple: tuple,
			Count: 1,
		}
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK"))
}

func (s *HTTPSource) NextBatch() (types.Batch, error) {
	select {
	case <-s.done:
		return nil, nil // Server closed
	case first := <-s.buffer:
		batch := types.Batch{first}
		
		// Drain buffer for more items without blocking
		// Limit batch size to avoid blocking too long
		limit := 100
	loop:
		for i := 0; i < limit; i++ {
			select {
			case item := <-s.buffer:
				batch = append(batch, item)
			default:
				break loop
			}
		}
		return batch, nil
	}
}

func (s *HTTPSource) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	close(s.done)
	return s.server.Shutdown(ctx)
}

func parseValueFromInterface(v interface{}, colType string) (any, error) {
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
	case "string":
		return fmt.Sprintf("%v", v), nil
	default:
		return v, nil
	}
}
