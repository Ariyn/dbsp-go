package main

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/apache/arrow/go/v15/arrow"
	"github.com/apache/arrow/go/v15/arrow/array"
	"github.com/apache/arrow/go/v15/arrow/memory"
	"github.com/apache/arrow/go/v15/parquet/file"
	"github.com/apache/arrow/go/v15/parquet/pqarrow"

	"github.com/ariyn/dbsp/internal/dbsp/op"
	sqlconv "github.com/ariyn/dbsp/internal/dbsp/sql"
	"github.com/ariyn/dbsp/internal/dbsp/types"
)

func TestE2E_Parquet_FinalSnapshotEqualsExpected_GroupByMultiAgg(t *testing.T) {
	// Goal: "집계 결과만 동일" 기준의 E2E.
	// - Source -> op.Execute -> ParquetSink
	// - Parquet 재로딩 후 delta 누적 => 최종 스냅샷 복원
	// - 입력 전체를 기준으로 계산한 기대 스냅샷과 비교

	query := "SELECT k, SUM(v), COUNT(id) FROM t GROUP BY k"
	root, err := sqlconv.ParseQueryToIncrementalDBSP(query)
	if err != nil {
		t.Fatalf("ParseQueryToIncrementalDBSP: %v", err)
	}

	// Build a Parquet sink with the expected output columns.
	dir := t.TempDir()
	prefix := filepath.Join(dir, "out")
	schema := &ParquetSchema{Columns: []ParquetColumn{
		{Name: "k", Type: "string"},
		{Name: "agg_delta", Type: "float64"},
		{Name: "count_delta", Type: "int64"},
		{Name: "__count", Type: "int64"},
	}}
	cfg := map[string]interface{}{
		"path":                 prefix,
		"compression":          "uncompressed",
		"row_group_size":       2,
		"rotate_every_batches": 1,
	}
	sink, err := NewParquetSink(cfg, schema)
	if err != nil {
		t.Fatalf("NewParquetSink: %v", err)
	}
	defer sink.Close()

	batches := []types.Batch{
		{{Tuple: types.Tuple{"k": "A", "v": 10.0, "id": int64(1)}, Count: 1}},
		{{Tuple: types.Tuple{"k": "B", "v": 3.0, "id": int64(2)}, Count: 1}},
		{{Tuple: types.Tuple{"k": "A", "v": 5.0, "id": int64(3)}, Count: 1}},
		// Retraction of one row
		{{Tuple: types.Tuple{"k": "B", "v": 3.0, "id": int64(2)}, Count: -1}},
		// Another update
		{{Tuple: types.Tuple{"k": "A", "v": 10.0, "id": int64(1)}, Count: -1}},
	}

	source := newSliceSource(batches)
	execute := func(b types.Batch) (types.Batch, error) { return op.Execute(root, b) }
	if err := runPipeline(context.Background(), source, sink, execute, nil, nil, 0); err != nil {
		t.Fatalf("runPipeline: %v", err)
	}
	if err := sink.Close(); err != nil {
		t.Fatalf("sink.Close: %v", err)
	}

	files := listParquetFiles(t, dir)
	if len(files) == 0 {
		t.Fatalf("expected parquet output files")
	}

	// Accumulate output deltas from parquet.
	gotAgg, gotCnt := accumulateAggFromParquetFiles(t, files)

	// Compute expected final snapshot by applying all input deltas.
	wantAgg, wantCnt := expectedFinalGroupAgg(batches)

	assertAggSnapshotEqual(t, gotAgg, gotCnt, wantAgg, wantCnt)
}

func TestE2E_JoinGroupBy_FinalSnapshotEqualsExpected_ExecuteTick(t *testing.T) {
	// Goal: Phase 2 핵심(Join + GroupAggregate)에 대해
	// - 2-source 그래프(Scan a, Scan b)를 ExecuteTick으로 평가
	// - 출력 델타를 누적해 최종 스냅샷을 복원
	// - 입력 전체 적용 후 최종 스냅샷(조인 결과 집계)과 동일함을 검증

	query := "SELECT a.k, SUM(b.v), COUNT(b.id) FROM a JOIN b ON a.id = b.id GROUP BY a.k"
	root, err := sqlconv.ParseQueryToIncrementalDBSP(query)
	if err != nil {
		t.Fatalf("ParseQueryToIncrementalDBSP: %v", err)
	}

	// Sanity: Join graphs have 2 sources; this test intentionally uses ExecuteTick.
	sources := op.SourceNames(root)
	if len(sources) != 2 {
		t.Fatalf("expected 2 sources for join graph, got %d (%v)", len(sources), sources)
	}

	// Prepare Parquet sink to store output deltas as well (optional, but covers sink+reload path).
	dir := t.TempDir()
	prefix := filepath.Join(dir, "out")
	schema := &ParquetSchema{Columns: []ParquetColumn{
		{Name: "a.k", Type: "string"},
		{Name: "agg_delta", Type: "float64"},
		{Name: "count_delta", Type: "int64"},
		{Name: "__count", Type: "int64"},
	}}
	cfg := map[string]interface{}{
		"path":                 prefix,
		"compression":          "uncompressed",
		"row_group_size":       2,
		"rotate_every_batches": 1,
	}
	sink, err := NewParquetSink(cfg, schema)
	if err != nil {
		t.Fatalf("NewParquetSink: %v", err)
	}
	defer sink.Close()

	// Tick inputs: keep it small but include deletes.
	ticks := []struct {
		a types.Batch
		b types.Batch
	}{
		{
			a: types.Batch{{Tuple: types.Tuple{"a.id": int64(1), "a.k": "A"}, Count: 1}},
				b: types.Batch{{Tuple: types.Tuple{"b.id": int64(1), "b.v": 10.0}, Count: 1}},
		},
		{
			a: types.Batch{{Tuple: types.Tuple{"a.id": int64(2), "a.k": "A"}, Count: 1}},
				b: types.Batch{{Tuple: types.Tuple{"b.id": int64(2), "b.v": 5.0}, Count: 1}},
		},
		{
			a: types.Batch{{Tuple: types.Tuple{"a.id": int64(3), "a.k": "B"}, Count: 1}},
				b: types.Batch{{Tuple: types.Tuple{"b.id": int64(3), "b.v": 7.0}, Count: 1}},
		},
		// Delete one right row (retract its contribution)
		{
			a: nil,
				b: types.Batch{{Tuple: types.Tuple{"b.id": int64(2), "b.v": 5.0}, Count: -1}},
		},
		// Delete one left row
		{
			a: types.Batch{{Tuple: types.Tuple{"a.id": int64(1), "a.k": "A"}, Count: -1}},
			b: nil,
		},
	}

	// Execute ticks, write output deltas to parquet.
	for _, tick := range ticks {
		out, err := op.ExecuteTick(root, map[string]types.Batch{"a": tick.a, "b": tick.b})
		if err != nil {
			t.Fatalf("ExecuteTick: %v", err)
		}
		if err := sink.WriteBatch(out); err != nil {
			t.Fatalf("sink.WriteBatch: %v", err)
		}
	}
	if err := sink.Close(); err != nil {
		t.Fatalf("sink.Close: %v", err)
	}

	files := listParquetFiles(t, dir)
	if len(files) == 0 {
		t.Fatalf("expected parquet output files")
	}

	gotAgg, gotCnt := accumulateAggFromParquetFilesWithKey(t, files, "a.k")
	wantAgg, wantCnt := expectedFinalJoinGroupAgg(ticks)
	assertAggSnapshotEqual(t, gotAgg, gotCnt, wantAgg, wantCnt)
}

func TestE2E_JoinGroupBy_CompositeKeys_FinalSnapshotEqualsExpected_ExecuteTick(t *testing.T) {
	query := "SELECT a.k1, a.k2, SUM(b.v), COUNT(b.id) FROM a JOIN b ON a.id = b.id AND a.k1 = b.k1 GROUP BY a.k1, a.k2"
	root, err := sqlconv.ParseQueryToIncrementalDBSP(query)
	if err != nil {
		t.Fatalf("ParseQueryToIncrementalDBSP: %v", err)
	}

	dir := t.TempDir()
	prefix := filepath.Join(dir, "out")
	schema := &ParquetSchema{Columns: []ParquetColumn{
		{Name: "a.k1", Type: "string"},
		{Name: "a.k2", Type: "string"},
		{Name: "agg_delta", Type: "float64"},
		{Name: "count_delta", Type: "int64"},
		{Name: "__count", Type: "int64"},
	}}
	cfg := map[string]interface{}{
		"path":                 prefix,
		"compression":          "uncompressed",
		"row_group_size":       2,
		"rotate_every_batches": 1,
	}
	sink, err := NewParquetSink(cfg, schema)
	if err != nil {
		t.Fatalf("NewParquetSink: %v", err)
	}
	defer sink.Close()

	ticks := []struct{
		a types.Batch
		b types.Batch
	}{
		{
			a: types.Batch{{Tuple: types.Tuple{"a.id": int64(1), "a.k1": "X", "a.k2": "P"}, Count: 1}},
			b: types.Batch{{Tuple: types.Tuple{"b.id": int64(1), "b.k1": "X", "b.v": 10.0}, Count: 1}},
		},
		{
			a: types.Batch{{Tuple: types.Tuple{"a.id": int64(2), "a.k1": "X", "a.k2": "Q"}, Count: 1}},
			b: types.Batch{{Tuple: types.Tuple{"b.id": int64(2), "b.k1": "X", "b.v": 5.0}, Count: 1}},
		},
		{
			a: types.Batch{{Tuple: types.Tuple{"a.id": int64(3), "a.k1": "Y", "a.k2": "P"}, Count: 1}},
			b: types.Batch{{Tuple: types.Tuple{"b.id": int64(3), "b.k1": "Y", "b.v": 7.0}, Count: 1}},
		},
		// Retract one right row
		{
			a: nil,
			b: types.Batch{{Tuple: types.Tuple{"b.id": int64(2), "b.k1": "X", "b.v": 5.0}, Count: -1}},
		},
	}

	for _, tick := range ticks {
		out, err := op.ExecuteTick(root, map[string]types.Batch{"a": tick.a, "b": tick.b})
		if err != nil {
			t.Fatalf("ExecuteTick: %v", err)
		}
		if err := sink.WriteBatch(out); err != nil {
			t.Fatalf("sink.WriteBatch: %v", err)
		}
	}
	if err := sink.Close(); err != nil {
		t.Fatalf("sink.Close: %v", err)
	}

	files := listParquetFiles(t, dir)
	if len(files) == 0 {
		t.Fatalf("expected parquet output files")
	}

	// Accumulate output deltas. Composite group key is (a.k1, a.k2).
	gotAgg := map[string]float64{}
	gotCnt := map[string]int64{}
	for _, path := range files {
		deltas := readParquetTupleDeltas(t, path)
		for _, td := range deltas {
			k1, _ := td.Tuple["a.k1"].(string)
			k2, _ := td.Tuple["a.k2"].(string)
			key := k1 + "|" + k2
			if v, ok := td.Tuple["agg_delta"]; ok {
				if fv, ok := v.(float64); ok {
					gotAgg[key] += fv
				}
			}
			if v, ok := td.Tuple["count_delta"]; ok {
				if iv, ok := v.(int64); ok {
					gotCnt[key] += iv
				}
			}
		}
	}

	wantAgg, wantCnt := expectedFinalJoinGroupAggComposite(ticks)
	assertAggSnapshotEqual(t, gotAgg, gotCnt, wantAgg, wantCnt)
}

func expectedFinalJoinGroupAggComposite(ticks []struct{ a, b types.Batch }) (map[string]float64, map[string]int64) {
	// Left: by join key (id,k1) -> (k2 -> count)
	left := map[string]map[string]int64{}
	// Right: by join key (id,k1) -> (v -> count)
	right := map[string]map[float64]int64{}

	joinKey := func(id int64, k1 string) string { return strconv.FormatInt(id, 10) + "|" + k1 }

	for _, tick := range ticks {
		for _, td := range tick.a {
			id, _ := td.Tuple["a.id"].(int64)
			k1, _ := td.Tuple["a.k1"].(string)
			k2, _ := td.Tuple["a.k2"].(string)
			jk := joinKey(id, k1)
			m := left[jk]
			if m == nil {
				m = map[string]int64{}
				left[jk] = m
			}
			m[k2] += td.Count
			if m[k2] == 0 {
				delete(m, k2)
			}
			if len(m) == 0 {
				delete(left, jk)
			}
		}
		for _, td := range tick.b {
			id, _ := td.Tuple["b.id"].(int64)
			k1, _ := td.Tuple["b.k1"].(string)
			v, _ := td.Tuple["b.v"].(float64)
			jk := joinKey(id, k1)
			m := right[jk]
			if m == nil {
				m = map[float64]int64{}
				right[jk] = m
			}
			m[v] += td.Count
			if m[v] == 0 {
				delete(m, v)
			}
			if len(m) == 0 {
				delete(right, jk)
			}
		}
	}

	agg := map[string]float64{}
	cnt := map[string]int64{}
	for jk, lks := range left {
		rvs := right[jk]
		if len(rvs) == 0 {
			continue
		}
		for k2, lc := range lks {
			for v, rc := range rvs {
				pairs := lc * rc
				if pairs == 0 {
					continue
				}
				// Need k1 from join key string: "id|k1"
				parts := strings.SplitN(jk, "|", 2)
				k1 := ""
				if len(parts) == 2 {
					k1 = parts[1]
				}
				gk := k1 + "|" + k2
				agg[gk] += float64(pairs) * v
				cnt[gk] += pairs
			}
		}
	}
	return agg, cnt
}

func accumulateAggFromParquetFilesWithKey(t *testing.T, files []string, keyCol string) (map[string]float64, map[string]int64) {
	t.Helper()
	agg := map[string]float64{}
	cnt := map[string]int64{}
	for _, path := range files {
		deltas := readParquetTupleDeltas(t, path)
		for _, td := range deltas {
			kAny, ok := td.Tuple[keyCol]
			if !ok {
				continue
			}
			k, _ := kAny.(string)
			if v, ok := td.Tuple["agg_delta"]; ok {
				if fv, ok := v.(float64); ok {
					agg[k] += fv
				}
			}
			if v, ok := td.Tuple["count_delta"]; ok {
				if iv, ok := v.(int64); ok {
					cnt[k] += iv
				}
			}
		}
	}
	return agg, cnt
}

func expectedFinalJoinGroupAgg(ticks []struct{ a, b types.Batch }) (map[string]float64, map[string]int64) {
	// Build final left/right table state as multisets.
	// Left: by id -> (k -> count)
	left := map[int64]map[string]int64{}
	// Right: by id -> (v -> count)
	right := map[int64]map[float64]int64{}

	applyLeft := func(td types.TupleDelta) {
		id, _ := td.Tuple["a.id"].(int64)
		k, _ := td.Tuple["a.k"].(string)
		m := left[id]
		if m == nil {
			m = map[string]int64{}
			left[id] = m
		}
		m[k] += td.Count
		if m[k] == 0 {
			delete(m, k)
		}
		if len(m) == 0 {
			delete(left, id)
		}
	}
	applyRight := func(td types.TupleDelta) {
		id, _ := td.Tuple["b.id"].(int64)
		v, _ := td.Tuple["b.v"].(float64)
		m := right[id]
		if m == nil {
			m = map[float64]int64{}
			right[id] = m
		}
		m[v] += td.Count
		if m[v] == 0 {
			delete(m, v)
		}
		if len(m) == 0 {
			delete(right, id)
		}
	}

	for _, tick := range ticks {
		for _, td := range tick.a {
			applyLeft(td)
		}
		for _, td := range tick.b {
			applyRight(td)
		}
	}

	agg := map[string]float64{}
	cnt := map[string]int64{}
	for id, lks := range left {
		rvs := right[id]
		if len(rvs) == 0 {
			continue
		}
		for k, lc := range lks {
			for v, rc := range rvs {
				pairs := lc * rc
				if pairs == 0 {
					continue
				}
				agg[k] += float64(pairs) * v
				cnt[k] += pairs
			}
		}
	}
	return agg, cnt
}

func listParquetFiles(t *testing.T, dir string) []string {
	t.Helper()
	ents, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	var files []string
	for _, e := range ents {
		if e.IsDir() {
			continue
		}
		if filepath.Ext(e.Name()) != ".parquet" {
			continue
		}
		files = append(files, filepath.Join(dir, e.Name()))
	}
	sort.Strings(files)
	return files
}

func accumulateAggFromParquetFiles(t *testing.T, files []string) (map[string]float64, map[string]int64) {
	t.Helper()
	agg := map[string]float64{}
	cnt := map[string]int64{}
	for _, path := range files {
		deltas := readParquetTupleDeltas(t, path)
		for _, td := range deltas {
			kAny, ok := td.Tuple["k"]
			if !ok {
				continue
			}
			k, _ := kAny.(string)
			if v, ok := td.Tuple["agg_delta"]; ok {
				if fv, ok := v.(float64); ok {
					agg[k] += fv
				}
			}
			if v, ok := td.Tuple["count_delta"]; ok {
				if iv, ok := v.(int64); ok {
					cnt[k] += iv
				}
			}
		}
	}
	return agg, cnt
}

func readParquetTupleDeltas(t *testing.T, path string) []types.TupleDelta {
	t.Helper()
	rdr, err := file.OpenParquetFile(path, false)
	if err != nil {
		t.Fatalf("OpenParquetFile(%s): %v", path, err)
	}
	defer rdr.Close()

	fr, err := pqarrow.NewFileReader(rdr, pqarrow.ArrowReadProperties{BatchSize: 1024}, memory.NewGoAllocator())
	if err != nil {
		t.Fatalf("NewFileReader(%s): %v", path, err)
	}

	rr, err := fr.GetRecordReader(context.Background(), nil, nil)
	if err != nil {
		t.Fatalf("GetRecordReader(%s): %v", path, err)
	}
	defer rr.Release()

	var out []types.TupleDelta
	for rr.Next() {
		rec := rr.Record()
		fields := rec.Schema().Fields()
		nameToIdx := make(map[string]int, len(fields))
		for i, f := range fields {
			nameToIdx[f.Name] = i
		}

		for row := 0; row < int(rec.NumRows()); row++ {
			tuple := make(types.Tuple)
			count := int64(1)
			for colName, idx := range nameToIdx {
				arr := rec.Column(idx)
				if arr.IsNull(row) {
					continue
				}
				val, ok := valueFromArrowArray(arr, row)
				if !ok {
					continue
				}
				if colName == "__count" {
					if iv, ok := val.(int64); ok {
						count = iv
					}
					continue
				}
				tuple[colName] = val
			}
			out = append(out, types.TupleDelta{Tuple: tuple, Count: count})
		}
	}
	if err := rr.Err(); err != nil && !errors.Is(err, io.EOF) {
		t.Fatalf("RecordReader.Err(%s): %v", path, err)
	}
	return out
}

func valueFromArrowArray(arr arrow.Array, row int) (any, bool) {
	switch arr.DataType().ID() {
	case arrow.STRING:
		return arr.(*array.String).Value(row), true
	case arrow.INT64:
		return arr.(*array.Int64).Value(row), true
	case arrow.FLOAT64:
		return arr.(*array.Float64).Value(row), true
	default:
		return nil, false
	}
}

func expectedFinalGroupAgg(batches []types.Batch) (map[string]float64, map[string]int64) {
	agg := map[string]float64{}
	cnt := map[string]int64{}
	for _, b := range batches {
		for _, td := range b {
			k, _ := td.Tuple["k"].(string)
			v, _ := td.Tuple["v"].(float64)
			agg[k] += v * float64(td.Count)
			// COUNT(id): id is always non-null in this test.
			cnt[k] += td.Count
		}
	}
	return agg, cnt
}

func assertAggSnapshotEqual(t *testing.T, gotAgg map[string]float64, gotCnt map[string]int64, wantAgg map[string]float64, wantCnt map[string]int64) {
	t.Helper()

	// Compare keys union.
	keys := map[string]bool{}
	for k := range gotAgg {
		keys[k] = true
	}
	for k := range gotCnt {
		keys[k] = true
	}
	for k := range wantAgg {
		keys[k] = true
	}
	for k := range wantCnt {
		keys[k] = true
	}

	for k := range keys {
		ga := gotAgg[k]
		wa := wantAgg[k]
		if ga != wa {
			t.Fatalf("agg mismatch for %q: got %v want %v (gotAgg=%v wantAgg=%v)", k, ga, wa, gotAgg, wantAgg)
		}
		gc := gotCnt[k]
		wc := wantCnt[k]
		if gc != wc {
			t.Fatalf("count mismatch for %q: got %v want %v (gotCnt=%v wantCnt=%v)", k, gc, wc, gotCnt, wantCnt)
		}
	}
}
