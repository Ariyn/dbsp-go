package op

import (
	"testing"

	"github.com/ariyn/dbsp/internal/dbsp/types"
)

func TestWindowAggFrameValueModeReplacesRow(t *testing.T) {
	agg := &SumAgg{ColName: "v"}
	w := NewWindowAggOp(
		WindowSpecLite{},
		func(t types.Tuple) any { return t["id"] },
		[]string{"id"},
		func() any { return float64(0) },
		agg,
	)
	w.OrderByCol = "ts"
	w.FrameSpec = &FrameSpecLite{Type: "ROWS", StartType: "UNBOUNDED PRECEDING", EndType: "CURRENT ROW"}
	w.KeepInput = true
	w.EmitValue = true

	batch1 := types.Batch{{Tuple: types.Tuple{"id": "a", "ts": int64(1000), "v": 1.0}, Count: 1}}
	out, err := w.Apply(batch1)
	if err != nil {
		t.Fatalf("apply batch1: %v", err)
	}
	if len(out) != 1 {
		t.Fatalf("expected 1 output row, got %d", len(out))
	}
	if out[0].Count != 1 {
		t.Fatalf("expected count 1, got %d", out[0].Count)
	}
	if got := types.ToFloat64(out[0].Tuple["agg_result"]); got != 1.0 {
		t.Fatalf("expected agg_result 1.0, got %v", out[0].Tuple["agg_result"])
	}

	batch2 := types.Batch{
		{Tuple: types.Tuple{"id": "a", "ts": int64(1000), "v": 1.0}, Count: -1},
		{Tuple: types.Tuple{"id": "a", "ts": int64(1000), "v": 2.0}, Count: 1},
	}
	out, err = w.Apply(batch2)
	if err != nil {
		t.Fatalf("apply batch2: %v", err)
	}
	if len(out) != 2 {
		t.Fatalf("expected 2 output rows, got %d", len(out))
	}

	var minusOK, plusOK bool
	var minusVal, plusVal float64
	for _, td := range out {
		if td.Count == -1 {
			minusOK = true
			minusVal = types.ToFloat64(td.Tuple["agg_result"])
			if td.Tuple["id"] != "a" || td.Tuple["ts"] != int64(1000) {
				t.Fatalf("unexpected retraction key: %v", td.Tuple)
			}
		}
		if td.Count == 1 {
			plusOK = true
			plusVal = types.ToFloat64(td.Tuple["agg_result"])
			if td.Tuple["id"] != "a" || td.Tuple["ts"] != int64(1000) {
				t.Fatalf("unexpected insertion key: %v", td.Tuple)
			}
		}
	}

	if !minusOK || !plusOK {
		t.Fatalf("expected both retraction and insertion, got %+v", out)
	}
	if minusVal != 1.0 || plusVal != 2.0 {
		t.Fatalf("expected agg_result retraction 1.0 and insertion 2.0, got -%v +%v", minusVal, plusVal)
	}
}
