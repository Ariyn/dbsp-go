package op

import (
	"fmt"

	"github.com/ariyn/dbsp/internal/dbsp/types"
)

// Watermark represents an event-time watermark
// Events with timestamps < watermark are considered "on-time"
// Events with timestamps >= watermark but < watermark + allowedLateness are "late"
// Events with timestamps >= watermark + allowedLateness are "too late" and may be dropped
type Watermark struct {
	Timestamp       int64 // Current watermark timestamp
	AllowedLateness int64 // How late events can arrive (in millis)
}

// WatermarkGenerator generates watermarks based on event timestamps
type WatermarkGenerator interface {
	// Update updates the watermark based on a new event timestamp
	Update(eventTime int64)
	// GetWatermark returns the current watermark
	GetWatermark() int64
}

// BoundedOutOfOrdernessWatermark generates watermarks allowing bounded out-of-orderness
type BoundedOutOfOrdernessWatermark struct {
	maxOutOfOrderness int64 // Maximum allowed out-of-orderness
	maxTimestamp      int64 // Maximum observed timestamp
}

// NewBoundedOutOfOrdernessWatermark creates a new watermark generator
func NewBoundedOutOfOrdernessWatermark(maxOutOfOrderness int64) *BoundedOutOfOrdernessWatermark {
	return &BoundedOutOfOrdernessWatermark{
		maxOutOfOrderness: maxOutOfOrderness,
		maxTimestamp:      0,
	}
}

// Update updates the watermark based on new event time
func (w *BoundedOutOfOrdernessWatermark) Update(eventTime int64) {
	if eventTime > w.maxTimestamp {
		w.maxTimestamp = eventTime
	}
}

// GetWatermark returns current watermark (maxTimestamp - maxOutOfOrderness)
func (w *BoundedOutOfOrdernessWatermark) GetWatermark() int64 {
	return w.maxTimestamp - w.maxOutOfOrderness
}

// LateEventBuffer buffers late events for reprocessing
type LateEventBuffer struct {
	buffer          []types.TupleDelta
	maxBufferSize   int
	allowedLateness int64
}

// NewLateEventBuffer creates a new late event buffer
func NewLateEventBuffer(maxSize int, allowedLateness int64) *LateEventBuffer {
	return &LateEventBuffer{
		buffer:          make([]types.TupleDelta, 0, maxSize),
		maxBufferSize:   maxSize,
		allowedLateness: allowedLateness,
	}
}

// Add adds a late event to the buffer
func (b *LateEventBuffer) Add(td types.TupleDelta) bool {
	if len(b.buffer) >= b.maxBufferSize {
		return false // Buffer full
	}
	b.buffer = append(b.buffer, td)
	return true
}

// GetAndClear returns all buffered events and clears the buffer
func (b *LateEventBuffer) GetAndClear() []types.TupleDelta {
	result := b.buffer
	b.buffer = make([]types.TupleDelta, 0, b.maxBufferSize)
	return result
}

// Len returns the number of buffered events
func (b *LateEventBuffer) Len() int {
	return len(b.buffer)
}

// LateEventPolicy defines how to handle late events
type LateEventPolicy int

const (
	// DropLateEvents drops events that arrive too late
	DropLateEvents LateEventPolicy = iota
	// BufferLateEvents buffers late events for reprocessing
	BufferLateEvents
	// EmitLateEvents processes late events and emits them separately
	EmitLateEvents
)

// WatermarkConfig configures watermark and late event handling
type WatermarkConfig struct {
	// Enabled indicates whether watermarking is enabled
	Enabled bool
	// MaxOutOfOrderness is the maximum allowed out-of-orderness
	MaxOutOfOrderness int64
	// AllowedLateness is how late events can arrive
	AllowedLateness int64
	// LateEventPolicy defines how to handle late events
	Policy LateEventPolicy
	// MaxBufferSize for late events (if BufferLateEvents policy)
	MaxBufferSize int
}

// DefaultWatermarkConfig returns a default watermark configuration
func DefaultWatermarkConfig() WatermarkConfig {
	return WatermarkConfig{
		Enabled:           false,
		MaxOutOfOrderness: 0,
		AllowedLateness:   0,
		Policy:            DropLateEvents,
		MaxBufferSize:     1000,
	}
}

// WatermarkAwareWindowOp extends WindowAggOp with watermark support
type WatermarkAwareWindowOp struct {
	*WindowAggOp
	watermarkGen WatermarkGenerator
	lateBuffer   *LateEventBuffer
	config       WatermarkConfig
	onTimeBuffer []types.TupleDelta // Buffer for on-time events
	lateBuffer2  []types.TupleDelta // Buffer for late events
}

type watermarkAwareSnapshotV1 struct {
	Base          windowAggSnapshotV1
	Config        WatermarkConfig
	MaxTimestamp  int64
	MaxOutOfOrder int64
	LateBuf       []types.TupleDelta
}

func (w *WatermarkAwareWindowOp) Snapshot() (any, error) {
	if w == nil {
		return watermarkAwareSnapshotV1{}, nil
	}
	baseAny, err := w.WindowAggOp.Snapshot()
	if err != nil {
		return nil, err
	}
	base, _ := baseAny.(windowAggSnapshotV1)

	snap := watermarkAwareSnapshotV1{Base: base, Config: w.config}
	if gen, ok := w.watermarkGen.(*BoundedOutOfOrdernessWatermark); ok && gen != nil {
		snap.MaxTimestamp = gen.maxTimestamp
		snap.MaxOutOfOrder = gen.maxOutOfOrderness
	}
	if w.lateBuffer != nil {
		// Copy buffer contents without mutating the buffer.
		snap.LateBuf = append([]types.TupleDelta(nil), w.lateBuffer.buffer...)
	}
	return snap, nil
}

func (w *WatermarkAwareWindowOp) Restore(state any) error {
	if w == nil {
		return fmt.Errorf("WatermarkAwareWindowOp is nil")
	}
	s, ok := state.(watermarkAwareSnapshotV1)
	if !ok {
		return fmt.Errorf("unexpected snapshot type %T", state)
	}

	// Restore base window state
	if err := w.WindowAggOp.Restore(s.Base); err != nil {
		return err
	}

	w.config = s.Config
	if w.config.Enabled {
		w.watermarkGen = NewBoundedOutOfOrdernessWatermark(s.MaxOutOfOrder)
		if gen, ok := w.watermarkGen.(*BoundedOutOfOrdernessWatermark); ok {
			gen.maxTimestamp = s.MaxTimestamp
		}
		if w.config.Policy == BufferLateEvents {
			w.lateBuffer = NewLateEventBuffer(w.config.MaxBufferSize, w.config.AllowedLateness)
			for _, td := range s.LateBuf {
				_ = w.lateBuffer.Add(td)
			}
		} else {
			w.lateBuffer = nil
		}
	} else {
		w.watermarkGen = nil
		w.lateBuffer = nil
	}
	return nil
}

// NewWatermarkAwareWindowOp creates a windowed aggregation operator with watermark support
func NewWatermarkAwareWindowOp(
	spec WindowSpecLite,
	keyFn func(types.Tuple) any,
	groupKeys []string,
	aggInit func() any,
	aggFn AggFunc,
	config WatermarkConfig,
) *WatermarkAwareWindowOp {
	baseOp := NewWindowAggOp(spec, keyFn, groupKeys, aggInit, aggFn)

	var watermarkGen WatermarkGenerator
	var lateBuffer *LateEventBuffer

	if config.Enabled {
		watermarkGen = NewBoundedOutOfOrdernessWatermark(config.MaxOutOfOrderness)
		if config.Policy == BufferLateEvents {
			lateBuffer = NewLateEventBuffer(config.MaxBufferSize, config.AllowedLateness)
		}
	}

	return &WatermarkAwareWindowOp{
		WindowAggOp:  baseOp,
		watermarkGen: watermarkGen,
		lateBuffer:   lateBuffer,
		config:       config,
		onTimeBuffer: make([]types.TupleDelta, 0),
		lateBuffer2:  make([]types.TupleDelta, 0),
	}
}

// Apply processes a batch with watermark-aware late event handling
func (w *WatermarkAwareWindowOp) Apply(batch types.Batch) (types.Batch, error) {
	if !w.config.Enabled {
		// No watermarking - use base implementation
		return w.WindowAggOp.Apply(batch)
	}

	// Classify events as on-time or late
	w.onTimeBuffer = w.onTimeBuffer[:0]
	w.lateBuffer2 = w.lateBuffer2[:0]

	for _, td := range batch {
		// Extract event time
		rawTs, ok := td.Tuple[w.Spec.TimeCol]
		if !ok || rawTs == nil {
			continue
		}
		eventTime, ok := rawTs.(int64)
		if !ok {
			continue
		}

		// Update watermark
		w.watermarkGen.Update(eventTime)

		// Check if event is late
		watermark := w.watermarkGen.GetWatermark()
		tooLateCutoff := watermark - w.config.AllowedLateness
		if eventTime < tooLateCutoff {
			// Too-late: always drop.
			continue
		}
		if eventTime < watermark {
			// Late but within allowed lateness
			switch w.config.Policy {
			case DropLateEvents:
				// Drop silently
				continue
			case BufferLateEvents:
				// Buffer for reprocessing
				if w.lateBuffer != nil {
					w.lateBuffer.Add(td)
				}
				continue
			case EmitLateEvents:
				// Process separately
				w.lateBuffer2 = append(w.lateBuffer2, td)
			}
			continue
		}
		{
			// On-time event
			w.onTimeBuffer = append(w.onTimeBuffer, td)
		}
	}

	// Process on-time events
	var result types.Batch
	if len(w.onTimeBuffer) > 0 {
		onTimeResult, err := w.WindowAggOp.Apply(w.onTimeBuffer)
		if err != nil {
			return nil, err
		}
		result = append(result, onTimeResult...)
	}

	// Process late events if policy is EmitLateEvents
	if w.config.Policy == EmitLateEvents && len(w.lateBuffer2) > 0 {
		lateResult, err := w.WindowAggOp.Apply(w.lateBuffer2)
		if err != nil {
			return nil, err
		}
		// Mark late events in output
		for i := range lateResult {
			lateResult[i].Tuple["__late"] = true
		}
		result = append(result, lateResult...)
	}

	// Clean up old windows based on watermark
	if w.watermarkGen != nil {
		w.cleanupOldWindows(w.watermarkGen.GetWatermark())
	}

	return result, nil
}

// cleanupOldWindows removes window state for windows that have expired
func (w *WatermarkAwareWindowOp) cleanupOldWindows(watermark int64) {
	if w.State.Data == nil {
		return
	}

	// Remove windows that have ended before watermark - allowedLateness
	cutoff := watermark - w.config.AllowedLateness

	for wid := range w.State.Data {
		if wid.End < cutoff {
			delete(w.State.Data, wid)
		}
	}
}

// ProcessBufferedLateEvents processes all buffered late events
func (w *WatermarkAwareWindowOp) ProcessBufferedLateEvents() (types.Batch, error) {
	if w.lateBuffer == nil || w.lateBuffer.Len() == 0 {
		return types.Batch{}, nil
	}

	buffered := w.lateBuffer.GetAndClear()
	return w.WindowAggOp.Apply(buffered)
}

// GetCurrentWatermark returns the current watermark timestamp
func (w *WatermarkAwareWindowOp) GetCurrentWatermark() int64 {
	if w.watermarkGen == nil {
		return 0
	}
	return w.watermarkGen.GetWatermark()
}
