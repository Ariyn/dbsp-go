package op

import "github.com/ariyn/dbsp/internal/dbsp/types"

// Integrate is a placeholder for an integrate operator. For Phase1 we keep a simple
// function that applies a delta batch to stored state externally (handled by GroupAggOp).
func IntegrateApply(state map[any]any, delta types.Batch) (types.Batch, error) {
	// No-op placeholder: real integrate would merge delta into snapshot state.
	return delta, nil
}
