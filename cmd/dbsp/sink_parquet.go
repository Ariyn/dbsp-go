package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/apache/arrow/go/v15/arrow"
	"github.com/apache/arrow/go/v15/arrow/array"
	"github.com/apache/arrow/go/v15/arrow/memory"
	"github.com/apache/arrow/go/v15/parquet"
	"github.com/apache/arrow/go/v15/parquet/compress"
	"github.com/apache/arrow/go/v15/parquet/pqarrow"

	"github.com/ariyn/dbsp/internal/dbsp/types"
)

type ParquetSink struct {
	cfg    ParquetSinkConfig
	schema *ParquetSchema

	arrowSchema *arrow.Schema
	mem         memory.Allocator

	file   *os.File
	writer *pqarrow.FileWriter

	builders []array.Builder // aligned with schema.Columns
	bufRows  int

	openedAt           time.Time
	rotateEvery        time.Duration
	rotateEveryBatches int
	batchesInFile      int
	fileSeq            int

	mu sync.Mutex
}

func NewParquetSink(config map[string]interface{}, schema *ParquetSchema) (*ParquetSink, error) {
	cfg, err := parseParquetSinkConfig(config)
	if err != nil {
		return nil, err
	}
	if schema == nil || len(schema.Columns) == 0 {
		return nil, fmt.Errorf("parquet sink requires a non-empty inferred schema")
	}

	rotateEvery, err := parseRotationDuration(cfg.RotateEvery)
	if err != nil {
		return nil, err
	}

	rowGroupSize := cfg.RowGroupSize
	if rowGroupSize <= 0 {
		rowGroupSize = 65536
		cfg.RowGroupSize = rowGroupSize
	}

	ps := &ParquetSink{
		cfg:                *cfg,
		schema:             schema,
		mem:                memory.NewGoAllocator(),
		rotateEvery:        rotateEvery,
		rotateEveryBatches: cfg.RotateEveryBatches,
	}

	as, err := ps.buildArrowSchema(schema)
	if err != nil {
		return nil, err
	}
	ps.arrowSchema = as

	if err := ps.openNewFileLocked(time.Now()); err != nil {
		return nil, err
	}
	return ps, nil
}

func (s *ParquetSink) WriteBatch(batch types.Batch) error {
	if len(batch) == 0 {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.writer == nil {
		if err := s.openNewFileLocked(time.Now()); err != nil {
			return err
		}
	}

	now := time.Now()
	// Rotate before writing the next batch if the current file has reached limits.
	if s.rotateEveryBatches > 0 && s.batchesInFile >= s.rotateEveryBatches {
		if err := s.rotateLocked(now); err != nil {
			return err
		}
	}
	if s.rotateEvery > 0 && now.Sub(s.openedAt) >= s.rotateEvery {
		if err := s.rotateLocked(now); err != nil {
			return err
		}
	}

	// Count this batch in the currently-open file.
	s.batchesInFile++

	for _, td := range batch {
		if err := s.appendRowLocked(td); err != nil {
			return err
		}
		if s.bufRows >= s.cfg.RowGroupSize {
			if err := s.flushLocked(); err != nil {
				return err
			}
		}
	}

	return nil
}

func (s *ParquetSink) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.flushLocked(); err != nil {
		_ = s.closeCurrentLocked()
		return err
	}
	return s.closeCurrentLocked()
}

func (s *ParquetSink) buildArrowSchema(schema *ParquetSchema) (*arrow.Schema, error) {
	fields := make([]arrow.Field, 0, len(schema.Columns))
	for _, c := range schema.Columns {
		switch c.Type {
		case "string":
			fields = append(fields, arrow.Field{Name: c.Name, Type: arrow.BinaryTypes.String, Nullable: true})
		case "int64":
			fields = append(fields, arrow.Field{Name: c.Name, Type: arrow.PrimitiveTypes.Int64, Nullable: true})
		case "float64":
			fields = append(fields, arrow.Field{Name: c.Name, Type: arrow.PrimitiveTypes.Float64, Nullable: true})
		default:
			// Unknown -> store as string.
			fields = append(fields, arrow.Field{Name: c.Name, Type: arrow.BinaryTypes.String, Nullable: true})
		}
	}
	return arrow.NewSchema(fields, nil), nil
}

func (s *ParquetSink) initBuildersLocked() {
	if s.builders != nil {
		return
	}
	s.builders = make([]array.Builder, 0, len(s.schema.Columns))
	for _, c := range s.schema.Columns {
		switch c.Type {
		case "string":
			s.builders = append(s.builders, array.NewStringBuilder(s.mem))
		case "int64":
			s.builders = append(s.builders, array.NewInt64Builder(s.mem))
		case "float64":
			s.builders = append(s.builders, array.NewFloat64Builder(s.mem))
		default:
			s.builders = append(s.builders, array.NewStringBuilder(s.mem))
		}
	}
}

func (s *ParquetSink) appendRowLocked(td types.TupleDelta) error {
	s.initBuildersLocked()

	for i, col := range s.schema.Columns {
		name := col.Name
		switch col.Type {
		case "int64":
			b := s.builders[i].(*array.Int64Builder)
			if name == "__count" {
				b.Append(td.Count)
				continue
			}
			v, ok := td.Tuple[name]
			if !ok || v == nil {
				b.AppendNull()
				continue
			}
			iv, ok := coerceInt64(v)
			if !ok {
				b.AppendNull()
				continue
			}
			b.Append(iv)

		case "float64":
			b := s.builders[i].(*array.Float64Builder)
			v, ok := td.Tuple[name]
			if !ok || v == nil {
				b.AppendNull()
				continue
			}
			fv, ok := coerceFloat64(v)
			if !ok {
				b.AppendNull()
				continue
			}
			b.Append(fv)

		case "string":
			b := s.builders[i].(*array.StringBuilder)
			v, ok := td.Tuple[name]
			if !ok || v == nil {
				b.AppendNull()
				continue
			}
			b.Append(fmt.Sprintf("%v", v))

		default:
			// Unknown types are stringified.
			b := s.builders[i].(*array.StringBuilder)
			v, ok := td.Tuple[name]
			if !ok || v == nil {
				b.AppendNull()
				continue
			}
			b.Append(fmt.Sprintf("%v", v))
		}
	}

	s.bufRows++
	return nil
}

func (s *ParquetSink) flushLocked() error {
	if s.bufRows == 0 {
		return nil
	}
	if s.writer == nil {
		return fmt.Errorf("parquet writer is nil")
	}

	cols := make([]arrow.Array, 0, len(s.builders))
	for _, b := range s.builders {
		arr := b.NewArray()
		cols = append(cols, arr)
	}
	rec := array.NewRecord(s.arrowSchema, cols, int64(s.bufRows))
	defer rec.Release()
	for _, a := range cols {
		a.Release()
	}

	if err := s.writer.Write(rec); err != nil {
		return err
	}

	// Reset builders.
	for _, b := range s.builders {
		b.Release()
	}
	s.builders = nil
	s.bufRows = 0
	return nil
}

func (s *ParquetSink) rotateLocked(now time.Time) error {
	if err := s.flushLocked(); err != nil {
		return err
	}
	if err := s.closeCurrentLocked(); err != nil {
		return err
	}
	s.batchesInFile = 0
	return s.openNewFileLocked(now)
}

func (s *ParquetSink) closeCurrentLocked() error {
	var firstErr error
	if s.writer != nil {
		if err := s.writer.Close(); err != nil {
			firstErr = err
		}
		s.writer = nil
	}
	if s.file != nil {
		if err := s.file.Close(); err != nil && !errors.Is(err, os.ErrClosed) {
			if firstErr == nil {
				firstErr = err
			}
		}
		s.file = nil
	}
	return firstErr
}

func (s *ParquetSink) openNewFileLocked(now time.Time) error {
	outPath, err := s.nextFilePath(now)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(outPath), 0755); err != nil {
		return fmt.Errorf("mkdir parquet dir: %w", err)
	}
	f, err := os.OpenFile(outPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("open parquet file %s: %w", outPath, err)
	}

	props := parquet.NewWriterProperties(parquet.WithCompression(parseCompression(s.cfg.Compression)))
	arrowProps := pqarrow.NewArrowWriterProperties(pqarrow.WithStoreSchema())
	w, err := pqarrow.NewFileWriter(s.arrowSchema, f, props, arrowProps)
	if err != nil {
		_ = f.Close()
		return fmt.Errorf("create parquet writer: %w", err)
	}

	s.file = f
	s.writer = w
	s.openedAt = now
	s.fileSeq++
	return nil
}

func (s *ParquetSink) nextFilePath(now time.Time) (string, error) {
	path := strings.TrimSpace(s.cfg.Path)
	if path == "" {
		return "", fmt.Errorf("parquet sink path is required")
	}

	// If path is a directory (ends with separator), write into it with default prefix.
	if strings.HasSuffix(path, string(os.PathSeparator)) {
		dir := filepath.Clean(path)
		return filepath.Join(dir, fmt.Sprintf("out-%s-%06d.parquet", now.UTC().Format("20060102T150405Z"), s.fileSeq)), nil
	}

	// If an existing directory is provided, treat it as directory.
	if st, err := os.Stat(path); err == nil && st.IsDir() {
		return filepath.Join(path, fmt.Sprintf("out-%s-%06d.parquet", now.UTC().Format("20060102T150405Z"), s.fileSeq)), nil
	}

	dir := filepath.Dir(path)
	base := filepath.Base(path)
	lower := strings.ToLower(base)
	if strings.HasSuffix(lower, ".parquet") {
		base = strings.TrimSuffix(base, filepath.Ext(base))
	}
	if base == "" || base == "." {
		base = "out"
	}

	name := fmt.Sprintf("%s-%s-%06d.parquet", base, now.UTC().Format("20060102T150405Z"), s.fileSeq)
	return filepath.Join(dir, name), nil
}

func parseCompression(s string) compress.Compression {
	s = strings.ToLower(strings.TrimSpace(s))
	if s == "" {
		return compress.Codecs.Zstd
	}
	switch s {
	case "zstd":
		return compress.Codecs.Zstd
	case "snappy":
		return compress.Codecs.Snappy
	case "gzip":
		return compress.Codecs.Gzip
	case "uncompressed", "none":
		return compress.Codecs.Uncompressed
	default:
		return compress.Codecs.Zstd
	}
}

func parseRotationDuration(s string) (time.Duration, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, nil
	}
	if d, err := time.ParseDuration(s); err == nil {
		return d, nil
	}
	// Match join_ttl / watermark style: "5 minutes".
	iv, err := types.ParseInterval(s)
	if err != nil {
		return 0, fmt.Errorf("invalid rotate_every %q: %w", s, err)
	}
	return time.Duration(iv.Millis) * time.Millisecond, nil
}

func coerceInt64(v any) (int64, bool) {
	switch x := v.(type) {
	case int:
		return int64(x), true
	case int64:
		return x, true
	case float64:
		return int64(x), true
	case string:
		i, err := strconv.ParseInt(strings.TrimSpace(x), 10, 64)
		if err != nil {
			return 0, false
		}
		return i, true
	default:
		return 0, false
	}
}

func coerceFloat64(v any) (float64, bool) {
	switch x := v.(type) {
	case float64:
		return x, true
	case float32:
		return float64(x), true
	case int:
		return float64(x), true
	case int64:
		return float64(x), true
	case string:
		f, err := strconv.ParseFloat(strings.TrimSpace(x), 64)
		if err != nil {
			return 0, false
		}
		return f, true
	default:
		return 0, false
	}
}
