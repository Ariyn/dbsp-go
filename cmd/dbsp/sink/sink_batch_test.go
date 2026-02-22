package sink

import (
	"github.com/ariyn/dbsp/cmd/dbsp/internal/testutil"
	"github.com/ariyn/dbsp/cmd/dbsp/provider"
	"testing"
	"time"

	"github.com/ariyn/dbsp/internal/dbsp/types"
)


func TestBatchSink_FlushOnMaxSize(t *testing.T) {
	inner := testutil.NewRecordingSink()
	sink := NewBatchSink(provider.Sink(inner), 3, 0)

	for i := 0; i < 5; i++ {
		if err := sink.WriteBatch(types.Batch{{Tuple: types.Tuple{"id": i}, Count: 1}}); err != nil {
			t.Fatalf("WriteBatch error: %v", err)
		}
	}
	if err := sink.Close(); err != nil {
		t.Fatalf("Close error: %v", err)
	}

	lens := inner.BatchLens()
	if len(lens) != 2 {
		t.Fatalf("expected 2 flushes, got %d (%v)", len(lens), lens)
	}
	if lens[0] != 3 || lens[1] != 2 {
		t.Fatalf("expected [3 2], got %v", lens)
	}
}

func TestBatchSink_FlushOnDelay(t *testing.T) {
	inner := testutil.NewRecordingSink()
	sink := NewBatchSink(provider.Sink(inner), 100, 40*time.Millisecond)

	if err := sink.WriteBatch(types.Batch{{Tuple: types.Tuple{"id": 1}, Count: 1}}); err != nil {
		t.Fatalf("WriteBatch error: %v", err)
	}

	select {
	case <-inner.WriteCh:
		// ok
	case <-time.After(200 * time.Millisecond):
		t.Fatalf("expected delayed flush")
	}

	lens := inner.BatchLens()
	if len(lens) != 1 || lens[0] != 1 {
		t.Fatalf("expected one flush of 1, got %v", lens)
	}

	if err := sink.Close(); err != nil {
		t.Fatalf("Close error: %v", err)
	}
}

func TestBatchSink_CloseFlushesRemaining(t *testing.T) {
	inner := testutil.NewRecordingSink()
	sink := NewBatchSink(provider.Sink(inner), 10, time.Second)

	if err := sink.WriteBatch(types.Batch{{Tuple: types.Tuple{"id": 1}, Count: 1}}); err != nil {
		t.Fatalf("WriteBatch error: %v", err)
	}
	if err := sink.WriteBatch(types.Batch{{Tuple: types.Tuple{"id": 2}, Count: 1}}); err != nil {
		t.Fatalf("WriteBatch error: %v", err)
	}

	if err := sink.Close(); err != nil {
		t.Fatalf("Close error: %v", err)
	}

	lens := inner.BatchLens()
	if len(lens) != 1 || lens[0] != 2 {
		t.Fatalf("expected one flush of 2 on Close, got %v", lens)
	}
}
