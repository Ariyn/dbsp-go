package op

import (
	"github.com/ariyn/dbsp/internal/dbsp/types"
)

type ProjectExprFn struct {
	OutCol string
	Eval   func(types.Tuple) (any, error)
}

// ProjectOp projects a tuple into a smaller tuple and can compute derived columns.
// Unlike MapOp, it can return an error if expression evaluation fails.
type ProjectOp struct {
	Columns []string
	Exprs   []ProjectExprFn
}

func (p *ProjectOp) Apply(batch types.Batch) (types.Batch, error) {
	if len(p.Columns) == 0 && len(p.Exprs) == 0 {
		return batch, nil
	}
	var out types.Batch
	for _, td := range batch {
		projected := make(types.Tuple)
		for _, col := range p.Columns {
			if v, ok := td.Tuple[col]; ok {
				projected[col] = v
			}
		}
		for _, e := range p.Exprs {
			v, err := e.Eval(td.Tuple)
			if err != nil {
				return nil, err
			}
			projected[e.OutCol] = v
		}
		out = append(out, types.TupleDelta{Tuple: projected, Count: td.Count})
	}
	return out, nil
}
