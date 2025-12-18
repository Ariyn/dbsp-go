package main

import (
	"fmt"
	"sync"
	"time"

	"github.com/ariyn/dbsp/internal/dbsp/types"
	"gopkg.in/yaml.v3"
)

type SinkBatchConfig struct {
	MaxBatchSize    int `yaml:"max_batch_size"`
	MaxBatchDelayMS int `yaml:"max_batch_delay_ms"`
}

type SinkBatchingWrapperConfig struct {
	Batch *SinkBatchConfig `yaml:"batch"`
}

func wrapSinkWithBatchingIfConfigured(config map[string]interface{}, sink Sink) (Sink, error) {
	if sink == nil {
		return nil, fmt.Errorf("sink is nil")
	}
	if config == nil {
		return sink, nil
	}

	yamlBytes, err := yaml.Marshal(config)
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
	inner Sink

	maxBatchSize  int
	maxBatchDelay time.Duration

	mu       sync.Mutex
	buffer   types.Batch
	timer    *time.Timer
	closed   bool
	asyncErr error
}

func NewBatchSink(inner Sink, maxBatchSize int, maxBatchDelay time.Duration) *BatchSink {
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
		chunk, remaining, err := s.takeChunkForFlush()
		if err != nil {
			return err
		}
		if len(chunk) == 0 {
			return nil
		}
		if err := s.inner.WriteBatch(chunk); err != nil {
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

func (s *BatchSink) takeChunkForFlush() (chunk types.Batch, remaining bool, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.asyncErr != nil {
		return nil, false, s.asyncErr
	}
	if len(s.buffer) == 0 {
		s.stopTimerLocked()
		return nil, false, nil
	}

	maxSize := s.maxBatchSize
	if maxSize <= 0 {
		maxSize = len(s.buffer)
	}

	if len(s.buffer) > maxSize {
		chunk = append(types.Batch(nil), s.buffer[:maxSize]...)
		s.buffer = s.buffer[maxSize:]
		remaining = true
		// Keep timer running if delay-based batching is enabled.
		if s.maxBatchDelay > 0 {
			s.timer.Reset(s.maxBatchDelay)
		}
		return chunk, remaining, nil
	}

	chunk = append(types.Batch(nil), s.buffer...)
	s.buffer = nil
	s.stopTimerLocked()
	return chunk, false, nil
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
