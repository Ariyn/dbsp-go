package op

import (
	"fmt"
	"time"

	"github.com/ariyn/dbsp/internal/dbsp/types"
)

// StateResetter clears an operator's mutable runtime state while preserving
// its configuration and function fields.
type StateResetter interface {
	ResetState() error
}

// ResetGraphState walks the operator graph and clears mutable operator state.
func ResetGraphState(root *Node) error {
	if root == nil {
		return nil
	}
	for _, node := range postOrderNodes(root) {
		if err := resetOperatorState(node.Op); err != nil {
			return fmt.Errorf("reset %T: %w", node.Op, err)
		}
	}
	return nil
}

func resetOperatorState(operator Operator) error {
	if operator == nil {
		return nil
	}
	if chained, ok := operator.(*ChainedOp); ok {
		for _, inner := range chained.Ops {
			if err := resetOperatorState(inner); err != nil {
				return err
			}
		}
		return nil
	}
	resetter, ok := operator.(StateResetter)
	if !ok {
		return nil
	}
	return resetter.ResetState()
}

func (g *GroupAggOp) ResetState() error {
	if g == nil {
		return fmt.Errorf("GroupAggOp is nil")
	}
	g.state = make(map[any]any)
	g.multiState = make(map[any][]any)
	g.lastTouched = make(map[any]time.Time)
	g.nextTTLCheck = time.Time{}
	g.ttlExpiry = ttlExpiryQueue{}
	return nil
}

func (i *IntegrateOp) ResetState() error {
	if i == nil {
		return fmt.Errorf("IntegrateOp is nil")
	}
	i.store = NewZSetStore()
	return nil
}

func (b *BinaryOp) ResetState() error {
	if b == nil {
		return fmt.Errorf("BinaryOp is nil")
	}
	b.leftState = make(map[any]joinBucket)
	b.rightState = make(map[any]joinBucket)
	b.nextTTLCheck = time.Time{}
	b.leftTTLExpiry = ttlExpiryQueue{}
	b.rightTTLExpiry = ttlExpiryQueue{}
	return nil
}

func (d *DelayOp) ResetState() error {
	if d == nil {
		return fmt.Errorf("DelayOp is nil")
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.prev = nil
	d.next = nil
	d.initialized = false
	return nil
}

func (w *OrderedWindowOp) ResetState() error {
	if w == nil {
		return fmt.Errorf("OrderedWindowOp is nil")
	}
	w.Partitions = make(map[any]*orderedWindowPartition)
	w.lastTouched = make(map[any]time.Time)
	w.nextTTLCheck = time.Time{}
	w.ttlExpiry = ttlExpiryQueue{}
	w.backendLoaded = false
	return nil
}

func (w *WindowAggOp) ResetState() error {
	if w == nil {
		return fmt.Errorf("WindowAggOp is nil")
	}
	w.State = WindowAggState{Data: make(map[WindowID]map[any]any)}
	w.GroupCounts = make(map[WindowID]map[any]int64)
	w.PartitionBuffers = make(map[any]*PartitionBuffer)
	w.SessionBuffers = make(map[any]*PartitionBuffer)
	w.sessionOut = make(map[any]map[string]types.Tuple)
	w.frameOut = make(map[any]map[string]types.TupleDelta)
	w.cumulativeFrameCache = make(map[any]*cumulativeFramePartitionCache)
	w.bucketedCumulativeState = make(map[any]*bucketedCumulativePartitionState)
	w.lastTouchedWindow = make(map[WindowID]time.Time)
	w.lastTouchedPartition = make(map[any]time.Time)
	w.nextTTLCheck = time.Time{}
	w.windowTTLExpiry = ttlExpiryQueue{}
	w.partitionTTLExpiry = ttlExpiryQueue{}
	w.bucketTTLExpiry = ttlExpiryQueue{}
	w.observedMaxEventTime = 0
	w.backendLoaded = false
	return nil
}
