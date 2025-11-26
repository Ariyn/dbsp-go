package op

import "github.com/ariyn/dbsp/internal/dbsp/types"

// MapOp applies a user function to each TupleDelta and emits zero or more TupleDelta outputs.
type MapOp struct {
	F func(types.TupleDelta) []types.TupleDelta
}

func (m *MapOp) Apply(batch types.Batch) (types.Batch, error) {
	var out types.Batch
	for _, td := range batch {
		res := m.F(td)
		for _, r := range res {
			out = append(out, r)
		}
	}
	return out, nil
}
