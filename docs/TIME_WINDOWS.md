# DBSP 시간 기반 윈도우 구현

이 문서는 DBSP-Go에 구현된 interval 기반 시간 윈도우 기능을 설명합니다.

## 개요

시간 기반 윈도우 집계는 스트리밍 데이터를 시간 간격으로 그룹화하여 처리하는 핵심 기능입니다. 세 가지 윈도우 타입을 지원합니다:

- **Tumbling Window**: 겹치지 않는 고정 크기 윈도우
- **Sliding Window**: 겹치는 고정 크기 윈도우
- **Session Window**: 비활동 시간 기반 동적 윈도우

## 구현된 기능

### 1. Interval 타입 (`types/types.go`)

시간 간격을 표현하고 파싱하는 기능:

```go
// Interval 정의
type Interval struct {
    Millis int64
}

// 파싱 예제
interval, _ := types.ParseInterval("5 minutes")  // 300000ms
interval, _ := types.ParseInterval("1 hour")     // 3600000ms
interval, _ := types.ParseInterval("30 seconds") // 30000ms
```

**지원하는 단위:**
- `millisecond`, `ms`
- `second`, `sec`
- `minute`, `min`
- `hour`, `hr`, `h`
- `day`, `d`

### 2. Window 타입 (`op/windowagg.go`)

#### WindowType 열거형
```go
const (
    WindowTypeTumbling WindowType = "TUMBLING"
    WindowTypeSliding  WindowType = "SLIDING"
    WindowTypeSession  WindowType = "SESSION"
)
```

#### WindowSpecLite 구조체
```go
type WindowSpecLite struct {
    TimeCol     string      // 타임스탬프 컬럼명
    SizeMillis  int64       // 윈도우 크기 (밀리초)
    WindowType  WindowType  // 윈도우 타입
    SlideMillis int64       // Sliding 윈도우의 슬라이드 크기
    GapMillis   int64       // Session 윈도우의 비활동 갭
}
```

## 사용 예제

### Tumbling Window

5분 간격으로 매출을 집계:

```go
spec := op.WindowSpecLite{
    TimeCol:    "ts",
    SizeMillis: 300000, // 5분
    WindowType: op.WindowTypeTumbling,
}

keyFn := func(t types.Tuple) any { return t["region"] }
aggInit := func() any { return float64(0) }
agg := &op.SumAgg{ColName: "amount"}

windowOp := op.NewWindowAggOp(spec, keyFn, []string{"region"}, aggInit, agg)

// 이벤트 적용
batch := types.Batch{
    {Tuple: types.Tuple{"ts": 100000, "region": "East", "amount": 1000.0}, Count: 1},
    {Tuple: types.Tuple{"ts": 350000, "region": "East", "amount": 1500.0}, Count: 1},
}

out, _ := windowOp.Apply(batch)
// 결과: [0, 300000) 윈도우에 1000.0, [300000, 600000) 윈도우에 1500.0
```

### Sliding Window

10초 윈도우, 5초마다 슬라이드:

```go
spec := op.WindowSpecLite{
    TimeCol:     "ts",
    SizeMillis:  10000, // 10초
    SlideMillis: 5000,  // 5초
    WindowType:  op.WindowTypeSliding,
}

keyFn := func(t types.Tuple) any { return t["user"] }
aggInit := func() any { return int64(0) }
agg := &op.CountAgg{}

windowOp := op.NewWindowAggOp(spec, keyFn, []string{"user"}, aggInit, agg)

// ts=7000 이벤트는 [0,10000)과 [5000,15000) 두 윈도우에 모두 포함됨
batch := types.Batch{
    {Tuple: types.Tuple{"ts": 2000, "user": "Alice"}, Count: 1},
    {Tuple: types.Tuple{"ts": 7000, "user": "Alice"}, Count: 1},
    {Tuple: types.Tuple{"ts": 12000, "user": "Alice"}, Count: 1},
}

out, _ := windowOp.Apply(batch)
// Alice의 이벤트가 여러 겹치는 윈도우에 집계됨
```

### Session Window

5초 비활동 시간으로 세션 구분:

```go
spec := op.WindowSpecLite{
    TimeCol:    "ts",
    GapMillis:  5000, // 5초 갭
    WindowType: op.WindowTypeSession,
}

keyFn := func(t types.Tuple) any { return t["user"] }
aggInit := func() any { return int64(0) }
agg := &op.CountAgg{}

windowOp := op.NewWindowAggOp(spec, keyFn, []string{"user"}, aggInit, agg)

// 5초 이상 갭이 있으면 새 세션으로 분리
batch := types.Batch{
    {Tuple: types.Tuple{"ts": 1000, "user": "Alice"}, Count: 1},  // Session 1
    {Tuple: types.Tuple{"ts": 3000, "user": "Alice"}, Count: 1},  // Session 1
    {Tuple: types.Tuple{"ts": 10000, "user": "Alice"}, Count: 1}, // Session 2 (gap > 5s)
}

out, _ := windowOp.Apply(batch)
// 두 개의 세션 윈도우가 생성됨
```

## 윈도우 알고리즘

### Sliding Window 계산

이벤트가 속하는 모든 윈도우를 계산:

```go
func windowIDsForSliding(spec WindowSpecLite, ts int64) []WindowID {
    // ts를 포함하는 모든 윈도우 찾기
    // 윈도우 시작점: 0, slide, 2*slide, 3*slide, ...
    // 조건: windowStart <= ts < windowStart + size
}
```

**예제:** size=10s, slide=5s
- ts=7s → 윈도우 [0,10), [5,15)
- ts=12s → 윈도우 [5,15), [10,20)

### Session Window 계산

파티션별로 정렬 후 갭 검사:

```go
func (w *WindowAggOp) applySession(batch types.Batch) {
    // 1. 파티션별로 이벤트 그룹화
    // 2. 각 파티션을 타임스탬프로 정렬
    // 3. 연속된 이벤트 간 갭 검사
    // 4. 갭 > GapMillis이면 새 세션 시작
}
```

## 출력 형식

모든 윈도우 연산의 출력에는 윈도우 메타데이터가 포함됩니다:

```go
delta := types.TupleDelta{
    Tuple: types.Tuple{
        "__window_start": 0,      // 윈도우 시작 (밀리초)
        "__window_end":   300000, // 윈도우 종료 (밀리초)
        "region":         "East",
        "sum":            2500.0,
    },
    Count: 1,
}
```

## 테스트

전체 테스트 실행:

```bash
go test -v ./internal/dbsp/op -run "TestWindowAggOp_Sliding|TestWindowAggOp_Session|TestInterval"
```

데모 실행:

```bash
go run ./cmd/demo
```

## 성능 고려사항

### Sliding Window
- 각 이벤트가 여러 윈도우에 속할 수 있음
- 윈도우 수: `size / slide` (최대)
- 메모리: O(active_windows × partitions)

### Session Window
- 파티션별 정렬 필요
- 메모리: O(events_per_partition)
- 늦게 도착한 이벤트 처리에 주의 필요

### Watermark (향후 구현)
```go
windowOp.WatermarkFn = func() int64 {
    return currentTime - latenessThreshold
}
```

## 향후 개선 사항

1. **Watermark 기반 윈도우 정리**
   - 오래된 윈도우 상태 자동 삭제
   - 메모리 관리 최적화

2. **늦게 도착한 이벤트 처리**
   - Late event 정책 설정
   - Allowed lateness 파라미터

3. **Session Window 병합**
   - 실시간 세션 병합 로직
   - 교차 파티션 최적화

4. **Window Trigger**
   - Event-time trigger
   - Processing-time trigger
   - Custom trigger 지원

## 참고 자료

- [DBSP 논문](https://github.com/vmware/database-stream-processor)
- Apache Flink Window 모델
- Google Dataflow Windowing
