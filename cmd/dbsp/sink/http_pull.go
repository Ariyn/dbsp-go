package sink

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"path/filepath"
	"strings"
	"sync"

	"github.com/apache/arrow/go/v15/arrow"
	"github.com/apache/arrow/go/v15/arrow/array"
	"github.com/apache/arrow/go/v15/arrow/memory"
	"github.com/apache/arrow/go/v15/parquet"
	"github.com/apache/arrow/go/v15/parquet/pqarrow"

	"github.com/ariyn/dbsp/cmd/dbsp/config"
	"github.com/ariyn/dbsp/internal/dbsp/op"
	"github.com/ariyn/dbsp/internal/dbsp/types"
)

type partitionEntry struct {
	store *op.ZSetStore
}

type HTTPPullSink struct {
	cfg         *config.HTTPPullSinkConfig
	partitionBy []string
	schema      *config.ParquetSchema
	arrowSchema *arrow.Schema
	mem         memory.Allocator

	// Map of partition key -> partitionEntry
	// For simplicity, we use string representation of partition key.
	partitions map[string]*partitionEntry
	mu         sync.RWMutex

	server *http.Server
}

func NewHTTPPullSink(hcfg config.HTTPPullSinkConfig, partitionBy []string, schema *config.ParquetSchema) (*HTTPPullSink, error) {
	if schema == nil {
		return nil, fmt.Errorf("http_pull sink requires a parquet schema")
	}

	s := &HTTPPullSink{
		cfg:         &hcfg,
		partitionBy: partitionBy,
		schema:      schema,
		mem:         memory.NewGoAllocator(),
		partitions:  make(map[string]*partitionEntry),
	}

	as := BuildArrowSchema(schema, true)
	s.arrowSchema = as

	// Start HTTP server in background
	mux := http.NewServeMux()
	mux.HandleFunc(hcfg.Path, s.handlePull)
	addr := fmt.Sprintf(":%d", hcfg.Port)
	s.server = &http.Server{
		Addr:    addr,
		Handler: mux,
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("binding http_pull sink to %s: %w", addr, err)
	}

	go func() {
		fmt.Printf("HTTP Pull Sink listening on :%d%s\n", hcfg.Port, hcfg.Path)
		if err := s.server.Serve(ln); err != nil && err != http.ErrServerClosed {
			fmt.Printf("HTTP Pull Sink server error: %v\n", err)
		}
	}()

	return s, nil
}

func (s *HTTPPullSink) WriteBatch(batch types.Batch) error {
	if len(batch) == 0 {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	for _, td := range batch {
		pk := s.getPartitionKey(td.Tuple)
		entry, ok := s.partitions[pk]
		if !ok {
			entry = &partitionEntry{
				store: op.NewZSetStore(),
			}
			s.partitions[pk] = entry
		}

		if err := entry.store.ApplyDelta(types.Batch{td}); err != nil {
			return err
		}
	}

	return nil
}

func (s *HTTPPullSink) getPartitionKey(t types.Tuple) string {
	if len(s.partitionBy) == 0 {
		return "default"
	}
	if len(s.partitionBy) == 1 {
		return sanitizeHivePathSegment(fmt.Sprintf("%v", t[s.partitionBy[0]]))
	}

	vals := make(map[string]string, len(s.partitionBy))
	for _, col := range s.partitionBy {
		vals[col] = sanitizeHivePathSegment(fmt.Sprintf("%v", t[col]))
	}

	b, _ := json.Marshal(vals)
	return string(b)
}

func (s *HTTPPullSink) Close() error {
	if s.server != nil {
		_ = s.server.Shutdown(context.Background())
	}
	return nil
}

func (s *HTTPPullSink) handlePull(w http.ResponseWriter, r *http.Request) {
	// 1. Determine partition from query params
	pk := ""
	if len(s.partitionBy) == 0 {
		pk = "default"
	} else if len(s.partitionBy) == 1 {
		pk = sanitizeHivePathSegment(r.URL.Query().Get(s.partitionBy[0]))
	} else {
		// Complex key: consistent with getPartitionKey
		vals := make(map[string]string)
		for _, col := range s.partitionBy {
			vals[col] = sanitizeHivePathSegment(r.URL.Query().Get(col))
		}
		b, _ := json.Marshal(vals)
		pk = string(b)
	}

	s.mu.Lock()
	entry, ok := s.partitions[pk]
	s.mu.Unlock()

	if !ok {
		if len(s.partitions) > 0 {
			fmt.Printf("HTTP Pull Warning: Partition '%s' not found. Available partitions (sanitized): ", pk)
			i := 0
			s.mu.RLock()
			for k := range s.partitions {
				fmt.Printf("%s ", k)
				if i++; i > 10 {
					fmt.Print("... ")
					break
				}
			}
			s.mu.RUnlock()
			fmt.Println()
		}
		http.Error(w, "Partition not found", http.StatusNotFound)
		return
	}

	// 2. Materialize to Arrow Record
	// We create a temporary batch from the store.
	s.mu.RLock()
	batch := entry.store.ToBatch()
	s.mu.RUnlock()

	if len(batch) == 0 {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	// 3. Write Parquet Stream to Response
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Disposition", `attachment; filename="snapshot.parquet"`)

	props := parquet.NewWriterProperties()
	arrowProps := pqarrow.NewArrowWriterProperties(pqarrow.WithStoreSchema())

	fw, err := pqarrow.NewFileWriter(s.arrowSchema, w, props, arrowProps)
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to create parquet writer: %v", err), http.StatusInternalServerError)
		return
	}
	defer fw.Close()

	// Build Arrow arrays for the batch
	rec, err := s.batchToRecord(batch)
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to build arrow record: %v", err), http.StatusInternalServerError)
		return
	}
	defer rec.Release()

	if err := fw.Write(rec); err != nil {
		// Too late to change header, but we can log.
		fmt.Printf("Error writing parquet stream: %v\n", err)
	}
}

func (s *HTTPPullSink) batchToRecord(batch types.Batch) (arrow.Record, error) {
	builders := make([]array.Builder, len(s.arrowSchema.Fields()))
	for i, f := range s.arrowSchema.Fields() {
		builders[i] = array.NewBuilder(s.mem, f.Type)
	}
	defer func() {
		for _, b := range builders {
			if b != nil {
				b.Release()
			}
		}
	}()

	if err := AppendTupleDeltasToArrowBuilders(s.arrowSchema, builders, batch); err != nil {
		return nil, err
	}

	cols := make([]arrow.Array, len(builders))
	for i, b := range builders {
		cols[i] = b.NewArray()
	}
	// We don't release columns here as the record will take ownership?
	// Actually NewRecord takes ownership.

	rec := array.NewRecord(s.arrowSchema, cols, int64(len(batch)))
	// Release arrays because Record took them (it's documented)
	for _, a := range cols {
		a.Release()
	}

	// Reset builders so they are not released twice in defer
	// Actually builders are released in defer, which is fine.

	return rec, nil
}

func sanitizeHivePathSegment(v string) string {
	v = strings.TrimSpace(v)
	v = strings.ReplaceAll(v, string(filepath.Separator), "_")
	v = strings.ReplaceAll(v, "=", "_")
	v = strings.ReplaceAll(v, "..", "_")
	if v == "" {
		return "_"
	}
	return v
}
