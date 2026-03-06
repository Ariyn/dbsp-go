package pipeline

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/ariyn/dbsp/cmd/dbsp/provider"
	"github.com/ariyn/dbsp/internal/dbsp/types"
	"github.com/ariyn/dbsp/internal/metrics"
)

type executeFn func(types.Batch) (types.Batch, error)

type pipelineProfiler struct {
	enabled       bool
	reportEvery   int
	windowBatches int
	windowInRows  int
	windowOutRows int
	windowNext    time.Duration
	windowExec    time.Duration
	windowSink    time.Duration
	windowAck     time.Duration
	windowTotal   time.Duration
	totalBatches  int
}

func newPipelineProfilerFromEnv() pipelineProfiler {
	trimmed := strings.TrimSpace(os.Getenv("DBSP_PIPELINE_PROFILE"))
	if trimmed == "" {
		return pipelineProfiler{}
	}
	reportEvery := 50
	if raw := strings.TrimSpace(os.Getenv("DBSP_PIPELINE_PROFILE_EVERY")); raw != "" {
		if parsed, err := strconv.Atoi(raw); err == nil && parsed > 0 {
			reportEvery = parsed
		}
	}
	return pipelineProfiler{enabled: true, reportEvery: reportEvery}
}

func (p *pipelineProfiler) observe(inRows, outRows int, nextDur, execDur, sinkDur, ackDur, totalDur time.Duration) {
	metrics.ObservePipelineBatch(inRows, outRows, nextDur, execDur, sinkDur, ackDur, totalDur)
	if p == nil || !p.enabled {
		return
	}
	p.windowBatches++
	p.totalBatches++
	p.windowInRows += inRows
	p.windowOutRows += outRows
	p.windowNext += nextDur
	p.windowExec += execDur
	p.windowSink += sinkDur
	p.windowAck += ackDur
	p.windowTotal += totalDur
	if p.windowBatches%p.reportEvery == 0 {
		p.printWindowSummary("window")
		p.resetWindow()
	}
}

func (p *pipelineProfiler) flush() {
	if p == nil || !p.enabled || p.windowBatches == 0 {
		return
	}
	p.printWindowSummary("final")
	p.resetWindow()
}

func (p *pipelineProfiler) printWindowSummary(scope string) {
	if p.windowBatches == 0 {
		return
	}
	totalMs := durationMillis(p.windowTotal)
	fmt.Printf(
		"PIPELINE profile scope=%s batches=%d total_batches=%d in_rows=%d out_rows=%d total_ms=%.3f avg_batch_ms=%.3f next_ms=%.3f next_pct=%.2f execute_ms=%.3f execute_pct=%.2f sink_ms=%.3f sink_pct=%.2f ack_ms=%.3f ack_pct=%.2f\n",
		scope,
		p.windowBatches,
		p.totalBatches,
		p.windowInRows,
		p.windowOutRows,
		totalMs,
		totalMs/float64(p.windowBatches),
		durationMillis(p.windowNext),
		percentOfDuration(p.windowNext, p.windowTotal),
		durationMillis(p.windowExec),
		percentOfDuration(p.windowExec, p.windowTotal),
		durationMillis(p.windowSink),
		percentOfDuration(p.windowSink, p.windowTotal),
		durationMillis(p.windowAck),
		percentOfDuration(p.windowAck, p.windowTotal),
	)
}

func (p *pipelineProfiler) resetWindow() {
	p.windowBatches = 0
	p.windowInRows = 0
	p.windowOutRows = 0
	p.windowNext = 0
	p.windowExec = 0
	p.windowSink = 0
	p.windowAck = 0
	p.windowTotal = 0
}

func durationMillis(d time.Duration) float64 {
	return float64(d) / float64(time.Millisecond)
}

func percentOfDuration(part, total time.Duration) float64 {
	if total <= 0 {
		return 0
	}
	return (float64(part) / float64(total)) * 100
}

func RunPipeline(ctx context.Context, source provider.Source, sink provider.Sink, execute executeFn) error {
	if ctx == nil {
		return fmt.Errorf("context is nil")
	}
	if source == nil {
		return fmt.Errorf("source is nil")
	}
	if sink == nil {
		return fmt.Errorf("sink is nil")
	}
	if execute == nil {
		return fmt.Errorf("execute function is nil")
	}

	stopCloser := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			_ = source.Close()
		case <-stopCloser:
		}
	}()
	defer close(stopCloser)
	profiler := newPipelineProfilerFromEnv()
	defer profiler.flush()

	batchCount := 0
	for {
		nextStarted := time.Now()
		batch, err := source.NextBatch()
		nextDur := time.Since(nextStarted)
		if err != nil {
			return err
		}
		if batch == nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return nil
		}

		batchCount++
		fmt.Printf("[%s] Processing batch %d with %d records...\n", time.Now().Format(time.RFC3339), batchCount, len(batch))
		cycleStarted := time.Now()
		executeStarted := time.Now()
		resultBatch, err := execute(batch)
		execDur := time.Since(executeStarted)
		if err != nil {
			return err
		}
		if strings.TrimSpace(os.Getenv("DBSP_DEBUG_PIPELINE")) != "" {
			fmt.Printf("DEBUG pipeline: output batch size=%d\n", len(resultBatch))
		}
		sinkStarted := time.Now()
		if err := sink.WriteBatch(resultBatch); err != nil {
			return err
		}
		sinkDur := time.Since(sinkStarted)
		ackDur := time.Duration(0)
		if ack, ok := source.(provider.BatchAcknowledger); ok {
			ackStarted := time.Now()
			if err := ack.AckBatchProcessed(batch); err != nil {
				return err
			}
			ackDur = time.Since(ackStarted)
		}
		profiler.observe(len(batch), len(resultBatch), nextDur, execDur, sinkDur, ackDur, time.Since(cycleStarted))

		if ctx.Err() != nil {
			return ctx.Err()
		}
	}
}
