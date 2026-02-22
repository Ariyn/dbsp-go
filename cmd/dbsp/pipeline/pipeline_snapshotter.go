package pipeline

type PipelineSnapshotterFunc struct {
	SnapFunc    func() ([]byte, error)
	RestoreFunc func([]byte) error
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
