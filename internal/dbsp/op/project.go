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
	Columns   []string
	Exprs     []ProjectExprFn
	KeepInput bool
}

func (p *ProjectOp) SupportsPackedBatch() bool {
	return true
}

func (p *ProjectOp) Apply(batch types.Batch) (types.Batch, error) {
	if len(p.Columns) == 0 && len(p.Exprs) == 0 {
		return batch, nil
	}
	if p.KeepInput && len(p.Exprs) == 0 {
		return batch, nil
	}
	if !p.KeepInput && len(p.Exprs) == 0 {
		if projected, ok := p.applyPackedProjection(batch); ok {
			return projected, nil
		}
	}
	out := make(types.Batch, 0, len(batch))
	for _, td := range batch {
		tuple := td.EnsureTuple()
		if td.Packed != nil {
			projected, err := p.applyPackedComputedProjection(td.Packed, tuple)
			if err != nil {
				return nil, err
			}
			out = append(out, types.TupleDelta{Packed: projected, Count: td.Count})
			continue
		}
		capacity := len(p.Exprs)
		if p.KeepInput {
			capacity += len(tuple)
		} else {
			capacity += len(p.Columns)
		}
		projected := make(types.Tuple, capacity)
		if p.KeepInput {
			for k, v := range tuple {
				projected[k] = v
			}
		} else {
			for _, col := range p.Columns {
				if v, ok := tuple[col]; ok {
					projected[col] = v
				}
			}
		}
		for _, e := range p.Exprs {
			v, err := e.Eval(tuple)
			if err != nil {
				return nil, err
			}
			projected[e.OutCol] = v
		}
		out = append(out, types.TupleDelta{Tuple: projected, Count: td.Count})
	}
	return out, nil
}

func (p *ProjectOp) applyPackedComputedProjection(packed *types.PackedTuple, tuple types.Tuple) (*types.PackedTuple, error) {
	var base *types.PackedTuple
	if p.KeepInput {
		base = packed
	} else {
		base = packed.Project(p.Columns)
	}
	if len(p.Exprs) == 0 {
		return base, nil
	}
	extras := make(types.Tuple, len(p.Exprs))
	for _, expr := range p.Exprs {
		value, err := expr.Eval(tuple)
		if err != nil {
			return nil, err
		}
		extras[expr.OutCol] = value
	}
	return base.WithExtras(extras), nil
}

func (p *ProjectOp) applyPackedProjection(batch types.Batch) (types.Batch, bool) {
	if len(p.Columns) == 0 {
		return nil, false
	}
	out := make(types.Batch, 0, len(batch))
	for _, td := range batch {
		if td.Packed == nil {
			return nil, false
		}
		projected := td.Packed.Project(p.Columns)
		out = append(out, types.TupleDelta{Packed: projected, Count: td.Count})
	}
	return out, true
}
