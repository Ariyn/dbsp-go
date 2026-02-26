package pipeline

import walpkg "github.com/ariyn/dbsp/internal/dbsp/wal"

type PipelineSnapshotterFunc struct {
	SnapFunc                        func() ([]byte, error)
	RestoreFunc                     func([]byte) error
	AfterCheckpointFunc             func(mode string, lastSeq int64)
	DrainCheckpointMutationsFunc    func() []walpkg.CheckpointMutation
	ApplyCheckpointMutationsFunc    func([]walpkg.CheckpointMutation) error
	RollbackCheckpointMutationsFunc func([]walpkg.CheckpointMutation)

	Mode                           string
	FullSnapshotEveryBatches       int
	MaxIncrementalMutationBytesVal int
}

func (p PipelineSnapshotterFunc) Snapshot() ([]byte, error) {
	if p.SnapFunc == nil {
		return nil, nil
	}
	return p.SnapFunc()
}

func (p PipelineSnapshotterFunc) Restore(b []byte) error {
	if p.RestoreFunc == nil {
		return nil
	}
	return p.RestoreFunc(b)
}

func (p PipelineSnapshotterFunc) CheckpointMode() string {
	return p.Mode
}

func (p PipelineSnapshotterFunc) FullSnapshotEvery() int {
	return p.FullSnapshotEveryBatches
}

func (p PipelineSnapshotterFunc) MaxIncrementalMutationBytes() int {
	return p.MaxIncrementalMutationBytesVal
}

func (p PipelineSnapshotterFunc) AfterCheckpoint(mode string, lastSeq int64) {
	if p.AfterCheckpointFunc == nil {
		return
	}
	p.AfterCheckpointFunc(mode, lastSeq)
}

func (p PipelineSnapshotterFunc) DrainCheckpointMutations() []walpkg.CheckpointMutation {
	if p.DrainCheckpointMutationsFunc == nil {
		return nil
	}
	return p.DrainCheckpointMutationsFunc()
}

func (p PipelineSnapshotterFunc) ApplyCheckpointMutations(mutations []walpkg.CheckpointMutation) error {
	if p.ApplyCheckpointMutationsFunc == nil || len(mutations) == 0 {
		return nil
	}
	return p.ApplyCheckpointMutationsFunc(mutations)
}

func (p PipelineSnapshotterFunc) RollbackCheckpointMutations(mutations []walpkg.CheckpointMutation) {
	if p.RollbackCheckpointMutationsFunc == nil || len(mutations) == 0 {
		return
	}
	p.RollbackCheckpointMutationsFunc(mutations)
}
