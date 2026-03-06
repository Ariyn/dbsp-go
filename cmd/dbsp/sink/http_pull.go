package sink

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
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
	store         *op.ZSetStore
	snapshotBatch types.Batch
	snapshotBytes []byte
	dirty         bool
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

	as := BuildArrowSchema(schema)
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

	partitioned := make(map[string]types.Batch)
	for _, td := range batch {
		pk := s.getPartitionKey(td.Tuple)
		if strings.TrimSpace(os.Getenv("DBSP_DEBUG_PARTITION")) != "" {
			fmt.Printf("DEBUG partition key: %s\n", pk)
		}
		partitioned[pk] = append(partitioned[pk], td)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	for pk, deltas := range partitioned {
		entry, ok := s.partitions[pk]
		if !ok {
			entry = &partitionEntry{
				store: op.NewZSetStore(),
				dirty: true,
			}
			s.partitions[pk] = entry
		}

		if err := entry.store.ApplyDelta(deltas); err != nil {
			return err
		}
		entry.snapshotBatch = nil
		entry.snapshotBytes = nil
		entry.dirty = true
	}

	return nil
}

func (s *HTTPPullSink) getPartitionKey(t types.Tuple) string {
	if len(s.partitionBy) == 0 {
		return "default"
	}
	if len(s.partitionBy) == 1 {
		val := resolvePartitionValue(t, s.partitionBy[0])
		return sanitizeHivePathSegment(fmt.Sprintf("%v", val))
	}
	return buildOrderedPartitionKey(s.partitionBy, func(col string) string {
		val := resolvePartitionValue(t, col)
		return sanitizeHivePathSegment(fmt.Sprintf("%v", val))
	})
}

func resolvePartitionValue(t types.Tuple, col string) any {
	if t == nil {
		return nil
	}
	if v, ok := t[col]; ok {
		return v
	}
	colLower := strings.ToLower(strings.TrimSpace(col))
	if colLower == "" {
		return nil
	}
	for k, v := range t {
		keyLower := strings.ToLower(strings.TrimSpace(k))
		if keyLower == colLower {
			return v
		}
		if dot := strings.LastIndex(keyLower, "."); dot != -1 {
			if keyLower[dot+1:] == colLower {
				return v
			}
		}
	}
	return nil
}

func (s *HTTPPullSink) Close() error {
	if s.server != nil {
		_ = s.server.Shutdown(context.Background())
	}
	return nil
}

// Checkpoint writes the current state of all partitions to Parquet files in the given directory.
func (s *HTTPPullSink) Checkpoint(dir string) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	for pk, entry := range s.partitions {
		path := filepath.Join(dir, pk, "checkpoint.parquet")
		if err := entry.store.WriteToParquet(path, s.arrowSchema, s.mem); err != nil {
			return fmt.Errorf("failed to checkpoint partition %s: %w", pk, err)
		}
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
		pk = buildOrderedPartitionKey(s.partitionBy, func(col string) string {
			return sanitizeHivePathSegment(r.URL.Query().Get(col))
		})
	}

	s.mu.RLock()
	entry, ok := s.partitions[pk]
	s.mu.RUnlock()

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
	batch, payload, err := s.materializePartitionSnapshot(pk, entry)
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to materialize partition: %v", err), http.StatusInternalServerError)
		return
	}

	if len(batch) == 0 {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	// 3. Write Parquet Stream to Response
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Disposition", `attachment; filename="snapshot.parquet"`)

	if _, err := w.Write(payload); err != nil {
		fmt.Printf("Error writing parquet stream: %v\n", err)
	}
}

func (s *HTTPPullSink) materializePartitionSnapshot(pk string, entry *partitionEntry) (types.Batch, []byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if entry == nil {
		entry = s.partitions[pk]
	}
	if entry == nil {
		return nil, nil, nil
	}
	if !entry.dirty && entry.snapshotBatch != nil && entry.snapshotBytes != nil {
		return entry.snapshotBatch, entry.snapshotBytes, nil
	}
	batch := entry.store.SnapshotBatch()
	if len(batch) == 0 {
		entry.snapshotBatch = nil
		entry.snapshotBytes = nil
		entry.dirty = false
		return nil, nil, nil
	}
	payload, err := s.encodeBatchToParquet(batch)
	if err != nil {
		return nil, nil, err
	}
	entry.snapshotBatch = batch
	entry.snapshotBytes = payload
	entry.dirty = false
	return batch, payload, nil
}

func (s *HTTPPullSink) encodeBatchToParquet(batch types.Batch) ([]byte, error) {
	var buf bytes.Buffer
	props := parquet.NewWriterProperties()
	arrowProps := pqarrow.NewArrowWriterProperties(pqarrow.WithStoreSchema())

	fw, err := pqarrow.NewFileWriter(s.arrowSchema, &buf, props, arrowProps)
	if err != nil {
		return nil, fmt.Errorf("create parquet writer: %w", err)
	}
	rec, err := s.batchToRecord(batch)
	if err != nil {
		_ = fw.Close()
		return nil, err
	}
	defer rec.Release()
	if err := fw.Write(rec); err != nil {
		_ = fw.Close()
		return nil, err
	}
	if err := fw.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
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

func buildOrderedPartitionKey(columns []string, lookup func(string) string) string {
	var b strings.Builder
	for idx, col := range columns {
		if idx > 0 {
			b.WriteByte('/')
		}
		b.WriteString(col)
		b.WriteByte('=')
		b.WriteString(lookup(col))
	}
	return b.String()
}
