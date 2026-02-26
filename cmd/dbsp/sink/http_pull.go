package sink

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
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

type HTTPPullSink struct {
	cfg         *config.HTTPPullSinkConfig
	partitionBy []string
	schema      *config.ParquetSchema
	arrowSchema *arrow.Schema
	mem         memory.Allocator

	// Map of partition key -> ZSetStore
	// For simplicity, we use string representation of partition key.
	partitions map[string]*op.ZSetStore
	mu         sync.RWMutex

	server *http.Server
}

func NewHTTPPullSink(cfg map[string]interface{}, partitionBy []string, schema *config.ParquetSchema) (*HTTPPullSink, error) {
	hcfg, err := config.ParseHTTPPullSinkConfig(cfg)
	if err != nil {
		return nil, err
	}

	if schema == nil {
		return nil, fmt.Errorf("http_pull sink requires a parquet schema")
	}

	s := &HTTPPullSink{
		cfg:         hcfg,
		partitionBy: partitionBy,
		schema:      schema,
		mem:         memory.NewGoAllocator(),
		partitions:  make(map[string]*op.ZSetStore),
	}

	if hcfg.DiskSpillPath != "" {
		if err := os.MkdirAll(hcfg.DiskSpillPath, 0755); err != nil {
			return nil, fmt.Errorf("failed to create disk spill path: %v", err)
		}
	}

	as, err := s.buildArrowSchema(schema)
	if err != nil {
		return nil, err
	}
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

	affectedPartitions := make(map[string]*op.ZSetStore)
	for _, td := range batch {
		pk := s.getPartitionKey(td.Tuple)
		store, ok := s.partitions[pk]
		if !ok {
			store = op.NewZSetStore()
			s.partitions[pk] = store
		}
		if err := store.ApplyDelta(types.Batch{td}); err != nil {
			return err
		}
		affectedPartitions[pk] = store
	}

	if s.cfg.DiskSpillPath != "" {
		for pk, store := range affectedPartitions {
			if err := s.persistPartition(pk, store); err != nil {
				// We log or error. For now, log.
				fmt.Printf("failed to persist partition %s: %v\n", pk, err)
			}
		}
	}

	return nil
}

func (s *HTTPPullSink) persistPartition(pk string, store *op.ZSetStore) error {
	path := s.partitionSpillPath(pk)
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}

	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	batch := store.ToBatch()
	if len(batch) == 0 {
		_ = os.Remove(path)
		return nil
	}

	props := parquet.NewWriterProperties()
	arrowProps := pqarrow.NewArrowWriterProperties(pqarrow.WithStoreSchema())

	fw, err := pqarrow.NewFileWriter(s.arrowSchema, f, props, arrowProps)
	if err != nil {
		return err
	}
	defer fw.Close()

	rec, err := s.batchToRecord(batch)
	if err != nil {
		return err
	}
	defer rec.Release()

	return fw.Write(rec)
}

func (s *HTTPPullSink) partitionSpillPath(pk string) string {
	if len(s.partitionBy) == 0 {
		return filepath.Join(s.cfg.DiskSpillPath, "default.parquet")
	}

	values := make(map[string]string, len(s.partitionBy))
	if len(s.partitionBy) == 1 {
		values[s.partitionBy[0]] = pk
	} else {
		var parsed map[string]string
		if err := json.Unmarshal([]byte(pk), &parsed); err == nil {
			for _, key := range s.partitionBy {
				values[key] = parsed[key]
			}
		}
	}

	baseFilePath := filepath.Join(s.cfg.DiskSpillPath, "snapshot.parquet")
	return config.BuildHivePartitionPath(baseFilePath, s.partitionBy, values)
}

func (s *HTTPPullSink) getPartitionKey(t types.Tuple) string {
	if len(s.partitionBy) == 0 {
		return "default"
	}
	if len(s.partitionBy) == 1 {
		return fmt.Sprintf("%v", t[s.partitionBy[0]])
	}

	// Composite key: consistent with handlePull (string-based JSON)
	vals := make(map[string]string)
	for _, col := range s.partitionBy {
		vals[col] = fmt.Sprintf("%v", t[col])
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

func (s *HTTPPullSink) buildArrowSchema(ps *config.ParquetSchema) (*arrow.Schema, error) {
	fields := make([]arrow.Field, 0, len(ps.Columns)+1)
	for _, c := range ps.Columns {
		var dt arrow.DataType
		switch c.Type {
		case "int64":
			dt = arrow.PrimitiveTypes.Int64
		case "float64":
			dt = arrow.PrimitiveTypes.Float64
		case "string":
			dt = arrow.BinaryTypes.String
		default:
			dt = arrow.BinaryTypes.String
		}
		fields = append(fields, arrow.Field{Name: c.Name, Type: dt, Nullable: true})
	}
	// Always include __count for Z-set multiplicity
	fields = append(fields, arrow.Field{Name: "__count", Type: arrow.PrimitiveTypes.Int64, Nullable: false})

	return arrow.NewSchema(fields, nil), nil
}

func (s *HTTPPullSink) handlePull(w http.ResponseWriter, r *http.Request) {
	// 1. Determine partition from query params
	pk := ""
	if len(s.partitionBy) == 0 {
		pk = "default"
	} else if len(s.partitionBy) == 1 {
		pk = r.URL.Query().Get(s.partitionBy[0])
	} else {
		// Complex key: consistent with getPartitionKey
		vals := make(map[string]string)
		for _, col := range s.partitionBy {
			vals[col] = r.URL.Query().Get(col)
		}
		b, _ := json.Marshal(vals)
		pk = string(b)
	}

	s.mu.RLock()
	store, ok := s.partitions[pk]
	if !ok {
		s.mu.RUnlock()
		http.Error(w, "Partition not found", http.StatusNotFound)
		return
	}

	// 2. Materialize to Arrow Record
	// We create a temporary batch from the store.
	batch := store.ToBatch()
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

	for _, td := range batch {
		for i, f := range s.arrowSchema.Fields() {
			if f.Name == "__count" {
				builders[i].(*array.Int64Builder).Append(td.Count)
				continue
			}

			val, ok := td.Tuple[f.Name]
			if !ok || val == nil {
				builders[i].AppendNull()
				continue
			}

			switch f.Type.ID() {
			case arrow.INT64:
				iv, _ := coerceInt64(val)
				builders[i].(*array.Int64Builder).Append(iv)
			case arrow.FLOAT64:
				fv, _ := coerceFloat64(val)
				builders[i].(*array.Float64Builder).Append(fv)
			case arrow.STRING:
				builders[i].(*array.StringBuilder).Append(fmt.Sprintf("%v", val))
			default:
				builders[i].(*array.StringBuilder).Append(fmt.Sprintf("%v", val))
			}
		}
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
