package sink

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/apache/arrow/go/v15/arrow"
	"github.com/apache/arrow/go/v15/arrow/array"
	"github.com/apache/arrow/go/v15/arrow/memory"
	"github.com/apache/arrow/go/v15/parquet"
	"github.com/apache/arrow/go/v15/parquet/compress"
	"github.com/apache/arrow/go/v15/parquet/pqarrow"

	"github.com/ariyn/dbsp/cmd/dbsp/config"
	"github.com/ariyn/dbsp/internal/dbsp/types"
)

type ParquetSink struct {
	cfg    config.ParquetSinkConfig
	schema *config.ParquetSchema

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

func NewParquetSink(parquetCfg config.ParquetSinkConfig, schema *config.ParquetSchema) (*ParquetSink, error) {
	if schema == nil || len(schema.Columns) == 0 {
		return nil, fmt.Errorf("parquet sink requires a non-empty inferred schema")
	}

	rotateEvery, err := parseRotationDuration(parquetCfg.RotateEvery)
	if err != nil {
		return nil, err
	}

	rowGroupSize := parquetCfg.RowGroupSize
	if rowGroupSize <= 0 {
		rowGroupSize = 65536
		parquetCfg.RowGroupSize = rowGroupSize
	}

	ps := &ParquetSink{
		cfg:                parquetCfg,
		schema:             schema,
		mem:                memory.NewGoAllocator(),
		rotateEvery:        rotateEvery,
		rotateEveryBatches: parquetCfg.RotateEveryBatches,
	}

	ps.arrowSchema = BuildArrowSchema(schema, false)

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

	s.initBuildersLocked()
	if err := AppendTupleDeltasToArrowBuilders(s.arrowSchema, s.builders, batch); err != nil {
		return err
	}
	s.bufRows += len(batch)

	if s.bufRows >= s.cfg.RowGroupSize {
		if err := s.flushLocked(); err != nil {
			return err
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

func (s *ParquetSink) initBuildersLocked() {
	if s.builders != nil {
		return
	}
	s.builders = make([]array.Builder, len(s.arrowSchema.Fields()))
	for i, f := range s.arrowSchema.Fields() {
		s.builders[i] = array.NewBuilder(s.mem, f.Type)
	}
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
		b.Release()
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
	return types.ParseFlexibleDuration(s)
}

func coerceInt64(v any) (int64, bool) {
	return types.ToInt64Safe(v)
}

func coerceFloat64(v any) (float64, bool) {
	return types.ToFloat64Safe(v)
}
