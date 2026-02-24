package ir

import (
	"testing"
	"time"

	"github.com/ariyn/dbsp/internal/dbsp/types"
)

func TestBuildExprFunc_ArithmeticAndCast(t *testing.T) {
	tup := types.Tuple{"a": int64(3)}

	f := BuildExprFunc("a + 2")
	v, err := f(tup)
	if err != nil {
		t.Fatalf("eval: %v", err)
	}
	if v.(float64) != 5 {
		t.Fatalf("expected 5, got %v", v)
	}

	f = BuildExprFunc("CAST(a AS BIGINT)")
	v, err = f(tup)
	if err != nil {
		t.Fatalf("eval cast: %v", err)
	}
	if v.(int64) != 3 {
		t.Fatalf("expected 3, got %v", v)
	}
}

func TestBuildExprFunc_CaseWhen(t *testing.T) {
	tup := types.Tuple{"a": int64(3)}
	f := BuildExprFunc("CASE WHEN a > 0 THEN 1 ELSE 0 END")
	v, err := f(tup)
	if err != nil {
		t.Fatalf("eval: %v", err)
	}
	if v.(int64) != 1 {
		t.Fatalf("expected 1, got %v", v)
	}
}

func TestBuildExprFunc_JSONCastAndMissingKey(t *testing.T) {
	tup := types.Tuple{"state": `{"active_power": 120.5}`}

	f := BuildExprFunc("state->'active_power'::DOUBLE")
	v, err := f(tup)
	if err != nil {
		t.Fatalf("eval json cast: %v", err)
	}
	if v.(float64) != 120.5 {
		t.Fatalf("expected 120.5, got %v", v)
	}

	fMissing := BuildExprFunc("state->'missing'::DOUBLE")
	vMissing, err := fMissing(tup)
	if err != nil {
		t.Fatalf("eval missing key: %v", err)
	}
	if vMissing.(float64) != 0 {
		t.Fatalf("expected 0 for missing key cast, got %v", vMissing)
	}
}

func TestBuildExprFunc_TimeBucketAndTimestampDiff(t *testing.T) {
	tup := types.Tuple{
		"event_time": "2024-01-01 10:02:30",
		"t1":         "2024-01-01 10:00:00",
		"t2":         "2024-01-01 10:05:00",
	}

	bucketExpr := BuildExprFunc("TIME_BUCKET(INTERVAL '5' MINUTE, event_time::TIMESTAMP)")
	bucket, err := bucketExpr(tup)
	if err != nil {
		t.Fatalf("eval time_bucket: %v", err)
	}
	bucketTime, ok := bucket.(time.Time)
	if !ok {
		t.Fatalf("expected time.Time bucket, got %T (%v)", bucket, bucket)
	}
	if bucketTime.UTC().Format("2006-01-02 15:04:05") != "2024-01-01 10:00:00" {
		t.Fatalf("expected 2024-01-01 10:00:00, got %v", bucketTime.UTC())
	}

	diffExpr := BuildExprFunc("t2::TIMESTAMP - t1::TIMESTAMP")
	diffSec, err := diffExpr(tup)
	if err != nil {
		t.Fatalf("eval timestamp diff: %v", err)
	}
	if diffSec.(float64) != 300 {
		t.Fatalf("expected 300 seconds, got %v", diffSec)
	}
}
