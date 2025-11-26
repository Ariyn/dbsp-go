package sqlconv

import (
	"fmt"
	"sort"

	"github.com/ariyn/dbsp/internal/dbsp/types"
)

// SortBatchByOrderColumn sorts a batch by the given column name if it exists.
// If orderCol is empty, the batch is returned as-is.
// Sorting is stable to preserve original order for equal keys.
func SortBatchByOrderColumn(batch types.Batch, orderCol string) types.Batch {
	if orderCol == "" || len(batch) == 0 {
		return batch
	}

	out := make(types.Batch, len(batch))
	copy(out, batch)

	sort.SliceStable(out, func(i, j int) bool {
		vi := out[i].Tuple[orderCol]
		vj := out[j].Tuple[orderCol]

		// nil safety: nil은 항상 뒤로 보낸다.
		if vi == nil && vj == nil {
			return false
		}
		if vi == nil {
			return false
		}
		if vj == nil {
			return true
		}

		switch ti := vi.(type) {
		case int64:
			if tj, ok := vj.(int64); ok {
				return ti < tj
			}
		case int:
			if tj, ok := vj.(int); ok {
				return ti < tj
			}
		case float64:
			if tj, ok := vj.(float64); ok {
				return ti < tj
			}
		case string:
			if tj, ok := vj.(string); ok {
				return ti < tj
			}
		}

		// 타입이 다르면 문자열 비교로 fallback
		return fmt.Sprint(vi) < fmt.Sprint(vj)
	})

	return out
}
