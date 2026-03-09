package op

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/apache/arrow/go/v15/arrow"
	"github.com/apache/arrow/go/v15/arrow/array"
	"github.com/apache/arrow/go/v15/arrow/memory"
	"github.com/apache/arrow/go/v15/parquet"
	"github.com/apache/arrow/go/v15/parquet/compress"
	"github.com/apache/arrow/go/v15/parquet/pqarrow"
	"github.com/ariyn/dbsp/internal/dbsp/types"
)

// ZSetRef is a read-only view of a Z-set (multiset relation).
//
// This is the Value-stream representation in the RFC.
// For now, LookupByKey is implemented via scanning; it is correct but may be O(n).
//
// NOTE: This interface is intentionally minimal; it can be extended later with
// indexed lookups without changing the semantic contract.
type ZSetRef interface {
	// ForEach iterates over all tuples with non-zero counts.
	// If f returns false, iteration stops.
	ForEach(f func(t types.Tuple, count int64) bool)

	// LookupByKey returns all tuples whose keyFn(tuple)==key.
	LookupByKey(key any, keyFn func(types.Tuple) any) []types.TupleDelta

	// ToBatch materializes the Z-set as a delta batch (tuple,count pairs).
	ToBatch() types.Batch
}

type zsetEntry struct {
	tuple types.Tuple
	count int64
}

// ZSetStore is a mutable Z-set store (tuple -> count).
// It is used by IntegrateOp to accumulate delta batches into a Value snapshot.
type ZSetStore struct {
	entries map[string]*zsetEntry
}

func NewZSetStore() *ZSetStore {
	return &ZSetStore{entries: make(map[string]*zsetEntry)}
}

func (s *ZSetStore) ApplyDelta(delta types.Batch) error {
	if s.entries == nil {
		s.entries = make(map[string]*zsetEntry)
	}
	for _, td := range delta {
		tk := stableTupleKey(td.Tuple)
		e, ok := s.entries[tk]
		if !ok {
			if td.Count < 0 {
				return fmt.Errorf("zset underflow for tuple=%v count=%d", td.Tuple, td.Count)
			}
			e = &zsetEntry{tuple: td.Tuple}
			s.entries[tk] = e
		}

		e.count += td.Count
		if e.count == 0 {
			delete(s.entries, tk)
			continue
		}
		if e.count < 0 {
			return fmt.Errorf("zset underflow for tuple=%v resultingCount=%d", td.Tuple, e.count)
		}

		// Keep latest tuple materialization.
		e.tuple = td.Tuple
	}
	return nil
}

func (s *ZSetStore) SnapshotBatch() types.Batch {
	if s == nil || len(s.entries) == 0 {
		return nil
	}
	out := make(types.Batch, 0, len(s.entries))
	for _, e := range s.entries {
		if e == nil || e.count == 0 {
			continue
		}
		out = append(out, types.TupleDelta{Tuple: e.tuple, Count: e.count})
	}
	return out
}

func (s *ZSetStore) ForEach(f func(t types.Tuple, count int64) bool) {
	if s == nil {
		return
	}
	for _, e := range s.entries {
		if e == nil || e.count == 0 {
			continue
		}
		if !f(types.CloneTuple(e.tuple), e.count) {
			return
		}
	}
}

func (s *ZSetStore) AppendToArrowBuilders(arrowSchema *arrow.Schema, builders []array.Builder) (int, error) {
	if s == nil || len(s.entries) == 0 {
		return 0, nil
	}
	if arrowSchema == nil {
		return 0, fmt.Errorf("arrow schema is nil")
	}
	if len(builders) != len(arrowSchema.Fields()) {
		return 0, fmt.Errorf("builder count %d does not match schema fields %d", len(builders), len(arrowSchema.Fields()))
	}

	rows := 0
	for _, entry := range s.entries {
		if entry == nil || entry.count == 0 {
			continue
		}
		for i, field := range arrowSchema.Fields() {
			val, ok := entry.tuple[field.Name]
			if !ok || val == nil {
				builders[i].AppendNull()
				continue
			}

			switch field.Type.ID() {
			case arrow.INT64:
				iv, _ := types.ToInt64Safe(val)
				builders[i].(*array.Int64Builder).Append(iv)
			case arrow.FLOAT64:
				fv, _ := types.ToFloat64Safe(val)
				builders[i].(*array.Float64Builder).Append(fv)
			case arrow.STRING:
				builders[i].(*array.StringBuilder).Append(fmt.Sprintf("%v", val))
			default:
				builders[i].(*array.StringBuilder).Append(fmt.Sprintf("%v", val))
			}
		}
		rows++
	}

	return rows, nil
}

func (s *ZSetStore) EntryCount() int {
	if s == nil {
		return 0
	}
	count := 0
	for _, entry := range s.entries {
		if entry == nil || entry.count == 0 {
			continue
		}
		count++
	}
	return count
}

func (s *ZSetStore) LookupByKey(key any, keyFn func(types.Tuple) any) []types.TupleDelta {
	var out types.Batch
	s.ForEach(func(t types.Tuple, count int64) bool {
		if keyFn == nil {
			return true
		}
		if keyFn(t) == key {
			out = append(out, types.TupleDelta{Tuple: t, Count: count})
		}
		return true
	})
	return out
}

// WriteToParquet serializes the current state of the ZSetStore to a Parquet file.
func (s *ZSetStore) WriteToParquet(path string, arrowSchema *arrow.Schema, mem memory.Allocator) error {
	if s == nil || s.entries == nil || len(s.entries) == 0 {
		return nil // Nothing to save
	}

	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}

	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	props := parquet.NewWriterProperties(parquet.WithCompression(compress.Codecs.Snappy))
	arrProps := pqarrow.NewArrowWriterProperties(pqarrow.WithStoreSchema())

	writer, err := pqarrow.NewFileWriter(arrowSchema, f, props, arrProps)
	if err != nil {
		return err
	}
	defer writer.Close()

	// Materialize Z-set into arrow record
	builders := make([]array.Builder, len(arrowSchema.Fields()))
	for i, f := range arrowSchema.Fields() {
		builders[i] = array.NewBuilder(mem, f.Type)
	}
	defer func() {
		for _, b := range builders {
			if b != nil {
				b.Release()
			}
		}
	}()

	rowCount, err := s.AppendToArrowBuilders(arrowSchema, builders)
	if err != nil {
		return err
	}

	cols := make([]arrow.Array, len(builders))
	for i, b := range builders {
		cols[i] = b.NewArray()
	}
	rec := array.NewRecord(arrowSchema, cols, int64(rowCount))
	defer rec.Release()
	for _, a := range cols {
		a.Release()
	}

	return writer.Write(rec)
}

func (s *ZSetStore) ToBatch() types.Batch {
	var out types.Batch
	s.ForEach(func(t types.Tuple, count int64) bool {
		out = append(out, types.TupleDelta{Tuple: t, Count: count})
		return true
	})
	return out
}
