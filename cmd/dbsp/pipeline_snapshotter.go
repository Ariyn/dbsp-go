package main

type pipelineSnapshotterFunc struct {
	snap    func() ([]byte, error)
	restore func([]byte) error
}

func (p pipelineSnapshotterFunc) Snapshot() ([]byte, error) {
	if p.snap == nil {
		return nil, nil
	}
	return p.snap()
}

func (p pipelineSnapshotterFunc) Restore(b []byte) error {
	if p.restore == nil {
		return nil
	}
	return p.restore(b)
}
