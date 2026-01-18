package main

import (
	"context"
	"testing"

	"github.com/ariyn/dbsp/internal/dbsp/op"
	sqlconv "github.com/ariyn/dbsp/internal/dbsp/sql"
	"github.com/ariyn/dbsp/internal/dbsp/types"
)

func TestRunPipeline_SQL_MultiAgg_DeleteRetraction(t *testing.T) {
	// End-to-end (cmd/dbsp) integration test:
	// Source -> runPipeline -> SQL compile -> op.Execute -> Sink
	// Verify delete(-1) produces correct aggregate deltas.
	query := "SELECT k, SUM(v), COUNT(id) FROM t GROUP BY k"
	root, err := sqlconv.ParseQueryToIncrementalDBSP(query)
	if err != nil {
		t.Fatalf("ParseQueryToIncrementalDBSP: %v", err)
	}

	source := newSliceSource([]types.Batch{
		{{Tuple: types.Tuple{"k": "A", "v": 10.0, "id": int64(1)}, Count: 1}},
		{{Tuple: types.Tuple{"k": "A", "v": 10.0, "id": int64(1)}, Count: -1}},
	})
	sink := newRecordingSink()

	execute := func(b types.Batch) (types.Batch, error) {
		return op.Execute(root, b)
	}

	if err := runPipeline(context.Background(), source, sink, execute, nil); err != nil {
		t.Fatalf("runPipeline: %v", err)
	}

	if got, want := len(sink.batches), 2; got != want {
		t.Fatalf("expected %d sink batches, got %d", want, got)
	}

	assertSingleTupleDelta(t, sink.batches[0], types.Tuple{"k": "A", "agg_delta": 10.0, "count_delta": int64(1)})
	assertSingleTupleDelta(t, sink.batches[1], types.Tuple{"k": "A", "agg_delta": -10.0, "count_delta": int64(-1)})
}

func assertSingleTupleDelta(t *testing.T, b types.Batch, want types.Tuple) {
	t.Helper()
	if len(b) != 1 {
		t.Fatalf("expected 1 tuple delta, got %d (%v)", len(b), b)
	}
	td := b[0]
	if td.Count != 1 {
		t.Fatalf("expected output Count=1, got %d", td.Count)
	}
	for k, w := range want {
		got, ok := td.Tuple[k]
		if !ok {
			t.Fatalf("missing key %q in output tuple: %v", k, td.Tuple)
		}
		switch wv := w.(type) {
		case float64:
			gv, ok := got.(float64)
			if !ok {
				t.Fatalf("expected %q to be float64, got %T (%v)", k, got, got)
			}
			if gv != wv {
				t.Fatalf("value mismatch for %q: got %v want %v", k, gv, wv)
			}
		case int64:
			gv, ok := got.(int64)
			if !ok {
				t.Fatalf("expected %q to be int64, got %T (%v)", k, got, got)
			}
			if gv != wv {
				t.Fatalf("value mismatch for %q: got %v want %v", k, gv, wv)
			}
		default:
			if got != w {
				t.Fatalf("value mismatch for %q: got %v want %v", k, got, w)
			}
		}
	}
}
