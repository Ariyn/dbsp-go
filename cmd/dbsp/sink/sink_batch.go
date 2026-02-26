package sink

import (
	"fmt"
	"sync"
	"time"

	"github.com/ariyn/dbsp/cmd/dbsp/provider"
	"github.com/ariyn/dbsp/internal/dbsp/types"
	"gopkg.in/yaml.v3"
)

// PartitionedSink supports writing batches with fixed partition values.
type PartitionedSink interface {
	WriteBatchWithPartition(batch types.Batch, values map[string]string) error
}

type SinkBatchConfig struct {
	MaxBatchSize    int `yaml:"max_batch_size"`
	MaxBatchDelayMS int `yaml:"max_batch_delay_ms"`
}

type SinkBatchingWrapperConfig struct {
	Batch *SinkBatchConfig `yaml:"batch"`
}

func WrapSinkWithBatchingIfConfigured(cfg map[string]interface{}, sink provider.Sink) (provider.Sink, error) {
	if sink == nil {
		return nil, fmt.Errorf("sink is nil")
	}
	if cfg == nil {
		return sink, nil
	}

	yamlBytes, err := yaml.Marshal(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal sink config: %w", err)
	}
	var wrapperCfg SinkBatchingWrapperConfig
	if err := yaml.Unmarshal(yamlBytes, &wrapperCfg); err != nil {
		return nil, fmt.Errorf("failed to parse sink batching config: %w", err)
	}

	if wrapperCfg.Batch == nil {
		return sink, nil
	}
	maxSize := wrapperCfg.Batch.MaxBatchSize
	maxDelay := time.Duration(wrapperCfg.Batch.MaxBatchDelayMS) * time.Millisecond
	if maxSize <= 0 && maxDelay <= 0 {
		return sink, nil
	}
	if wrapperCfg.Batch.MaxBatchDelayMS < 0 {
		maxDelay = 0
	}

	return NewBatchSink(sink, maxSize, maxDelay), nil
}

type BatchSink struct {
	inner provider.Sink

	maxBatchSize  int
	maxBatchDelay time.Duration

	mu       sync.Mutex
	buffer   types.Batch
	timer    *time.Timer
	closed   bool
	asyncErr error

	partitionValues map[string]string
}

func NewBatchSink(inner provider.Sink, maxBatchSize int, maxBatchDelay time.Duration) *BatchSink {
	return &BatchSink{
		inner:         inner,
		maxBatchSize:  maxBatchSize,
		maxBatchDelay: maxBatchDelay,
	}
}

func (s *BatchSink) WriteBatch(batch types.Batch) error {
	if len(batch) == 0 {
		return nil
	}

	var shouldFlush bool
	var flushErr error

	s.mu.Lock()
	if s.asyncErr != nil {
		flushErr = s.asyncErr
		s.mu.Unlock()
		return flushErr
	}
	if s.closed {
		s.mu.Unlock()
		return fmt.Errorf("batch sink is closed")
	}
	if s.partitionValues != nil {
		s.mu.Unlock()
		return fmt.Errorf("batch sink cannot accept non-partitioned batches while partitioned buffer is active")
	}

	wasEmpty := len(s.buffer) == 0
	s.buffer = append(s.buffer, batch...)
	if wasEmpty && len(s.buffer) > 0 {
		s.startTimerLocked()
	}

	if s.maxBatchSize > 0 && len(s.buffer) >= s.maxBatchSize {
		shouldFlush = true
	}
	s.mu.Unlock()

	if shouldFlush {
		return s.Flush()
	}
	return nil
}

func (s *BatchSink) WriteBatchWithPartition(batch types.Batch, values map[string]string) error {
	if len(batch) == 0 {
		return nil
	}

	var shouldFlush bool

	s.mu.Lock()
	if s.asyncErr != nil {
		err := s.asyncErr
		s.mu.Unlock()
		return err
	}
	if s.closed {
		s.mu.Unlock()
		return fmt.Errorf("batch sink is closed")
	}
	if s.partitionValues == nil {
		s.partitionValues = copyPartitionValues(values)
	} else if !partitionValuesEqual(s.partitionValues, values) {
		s.mu.Unlock()
		return fmt.Errorf("batch sink received mixed partition values")
	}

	wasEmpty := len(s.buffer) == 0
	s.buffer = append(s.buffer, batch...)
	if wasEmpty && len(s.buffer) > 0 {
		s.startTimerLocked()
	}

	if s.maxBatchSize > 0 && len(s.buffer) >= s.maxBatchSize {
		shouldFlush = true
	}
	s.mu.Unlock()

	if shouldFlush {
		return s.Flush()
	}
	return nil
}

func (s *BatchSink) Flush() error {
	for {
		chunk, remaining, values, err := s.takeChunkForFlush()
		if err != nil {
			return err
		}
		if len(chunk) == 0 {
			return nil
		}
		if values != nil {
			ps, ok := s.inner.(PartitionedSink)
			if !ok {
				err := fmt.Errorf("batch sink inner does not support partitioned writes")
				s.setAsyncErr(err)
				return err
			}
			if err := ps.WriteBatchWithPartition(chunk, values); err != nil {
				s.setAsyncErr(err)
				return err
			}
		} else if err := s.inner.WriteBatch(chunk); err != nil {
			s.setAsyncErr(err)
			return err
		}

		if !remaining {
			return nil
		}
	}
}

func (s *BatchSink) Close() error {
	s.mu.Lock()
	if s.closed {
		err := s.asyncErr
		s.mu.Unlock()
		return err
	}
	s.closed = true
	s.stopTimerLocked()
	err := s.asyncErr
	s.mu.Unlock()

	if err != nil {
		_ = s.inner.Close()
		return err
	}
	if err := s.Flush(); err != nil {
		_ = s.inner.Close()
		return err
	}
	return s.inner.Close()
}

func (s *BatchSink) startTimerLocked() {
	if s.maxBatchDelay <= 0 {
		return
	}

	if s.timer == nil {
		s.timer = time.AfterFunc(s.maxBatchDelay, func() {
			if err := s.Flush(); err != nil {
				// error is captured by setAsyncErr; callers will see it on next WriteBatch/Close
			}
		})
		return
	}

	s.timer.Reset(s.maxBatchDelay)
}

func (s *BatchSink) stopTimerLocked() {
	if s.timer == nil {
		return
	}
	if !s.timer.Stop() {
		select {
		case <-s.timer.C:
		default:
		}
	}
}

func (s *BatchSink) takeChunkForFlush() (chunk types.Batch, remaining bool, values map[string]string, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.asyncErr != nil {
		return nil, false, nil, s.asyncErr
	}
	if len(s.buffer) == 0 {
		s.stopTimerLocked()
		return nil, false, nil, nil
	}

	maxSize := s.maxBatchSize
	if maxSize <= 0 {
		maxSize = len(s.buffer)
	}

	if len(s.buffer) > maxSize {
		chunk = append(types.Batch(nil), s.buffer[:maxSize]...)
		s.buffer = s.buffer[maxSize:]
		remaining = true
		values = copyPartitionValues(s.partitionValues)
		// Keep timer running if delay-based batching is enabled.
		if s.maxBatchDelay > 0 {
			s.timer.Reset(s.maxBatchDelay)
		}
		return chunk, remaining, values, nil
	}

	chunk = append(types.Batch(nil), s.buffer...)
	s.buffer = nil
	s.stopTimerLocked()
	values = copyPartitionValues(s.partitionValues)
	s.partitionValues = nil
	return chunk, false, values, nil
}

func (s *BatchSink) setAsyncErr(err error) {
	if err == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.asyncErr == nil {
		s.asyncErr = err
	}
	// stop timer so we don't keep trying
	s.stopTimerLocked()
}

func copyPartitionValues(values map[string]string) map[string]string {
	if values == nil {
		return nil
	}
	out := make(map[string]string, len(values))
	for k, v := range values {
		out[k] = v
	}
	return out
}

func partitionValuesEqual(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}
