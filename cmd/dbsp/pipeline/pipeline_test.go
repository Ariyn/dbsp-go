package pipeline

import (
	"os"
	"context"
	"testing"
	"time"

	"github.com/ariyn/dbsp/internal/dbsp/types"
)

type testAckSource struct {
	batches   []types.Batch
	ackCount  int
	nextIndex int
}

func (s *testAckSource) NextBatch() (types.Batch, error) {
	if s.nextIndex >= len(s.batches) {
		return nil, nil
	}
	batch := s.batches[s.nextIndex]
	s.nextIndex++
	return batch, nil
}

func (s *testAckSource) AckBatchProcessed(types.Batch) error {
	s.ackCount++
	return nil
}

func (s *testAckSource) Close() error {
	return nil
}

type testSink struct{}

func (testSink) WriteBatch(types.Batch) error {
	return nil
}

func (testSink) Close() error {
	return nil
}

func TestRunPipelineAcknowledgesProcessedBatch(t *testing.T) {
	src := &testAckSource{batches: []types.Batch{{{Tuple: types.Tuple{"a": 1}, Count: 1}}}}
	if err := RunPipeline(context.Background(), src, testSink{}, func(batch types.Batch) (types.Batch, error) {
		return batch, nil
	}); err != nil {
		t.Fatalf("run pipeline: %v", err)
	}
	if src.ackCount != 1 {
		t.Fatalf("expected 1 batch acknowledgement, got %d", src.ackCount)
	}
}

func TestNewPipelineProfilerFromEnvDefaults(t *testing.T) {
	t.Setenv("DBSP_PIPELINE_PROFILE", "1")
	t.Setenv("DBSP_PIPELINE_PROFILE_EVERY", "")

	profiler := newPipelineProfilerFromEnv()
	if !profiler.enabled {
		t.Fatal("expected profiler to be enabled")
	}
	if profiler.reportEvery != 50 {
		t.Fatalf("expected default reportEvery=50, got %d", profiler.reportEvery)
	}
}

func TestNewPipelineProfilerFromEnvRespectsPositiveOverride(t *testing.T) {
	t.Setenv("DBSP_PIPELINE_PROFILE", "1")
	t.Setenv("DBSP_PIPELINE_PROFILE_EVERY", "7")

	profiler := newPipelineProfilerFromEnv()
	if profiler.reportEvery != 7 {
		t.Fatalf("expected reportEvery=7, got %d", profiler.reportEvery)
	}
}

func TestPipelineProfilerObserveResetsAfterWindow(t *testing.T) {
	profiler := pipelineProfiler{enabled: true, reportEvery: 2}
	stdout := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	os.Stdout = w
	defer func() {
		os.Stdout = stdout
	}()

	profiler.observe(10, 12, time.Millisecond, 2*time.Millisecond, 3*time.Millisecond, time.Millisecond, 6*time.Millisecond)
	if profiler.windowBatches != 1 {
		t.Fatalf("expected 1 pending window batch, got %d", profiler.windowBatches)
	}
	profiler.observe(10, 12, time.Millisecond, 2*time.Millisecond, 3*time.Millisecond, time.Millisecond, 6*time.Millisecond)
	if profiler.windowBatches != 0 {
		t.Fatalf("expected window counters to reset after summary, got %d", profiler.windowBatches)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close writer: %v", err)
	}
	_ = r.Close()
	if profiler.totalBatches != 2 {
		t.Fatalf("expected totalBatches=2, got %d", profiler.totalBatches)
	}
}
