# DBSP IVM — Phase1 Scaffold

간단한 Phase1 구현: Go로 작성된 최소 DBSP 연산자 스캐폴드입니다.

## 지원 기능

### 집계 (Aggregates)

- `SUM` (설정 가능한 컬럼)
- `COUNT`

### 핵심 기능

- **논리 계획 레이어**: SQL → LogicalPlan → DBSP 변환
- **자동 미분(Differentiation)**: DBSP 그래프를 증분(incremental) 버전으로 자동 변환
- **증분 실행**: Δ배치(delta batch)를 처리하여 증분 뷰 유지보수(IVM)

### SQL 지원

- 단일 테이블에서 `SELECT ... GROUP BY` (단일 그룹 키)
- `SUM(col)`, `COUNT(col)` 집계 함수
- **WHERE 절 필터링**
  - 비교 연산자: `=`, `!=`, `<`, `<=`, `>`, `>=`
  - 논리 연산자: `AND`, `OR`
  - **괄호를 통한 명시적 우선순위 제어**
    - 기본 우선순위: `AND` > `OR`
    - 괄호 사용으로 우선순위 변경 가능
    - 중첩 괄호 지원
    - 예: `(age > 50 OR status = 'vip') AND amount > 100`
- **Projection** (SELECT 특정 컬럼 선택)
- **시간 기반 윈도우 집계**
  - **Tumbling Windows**: 겹치지 않는 고정 크기 윈도우
  - **Sliding Windows**: 겹치는 윈도우 (size, slide 파라미터)
  - **Session Windows**: 비활동 시간 기반 동적 윈도우

### 시간 윈도우 기능

#### Interval 타입
- 시간 간격 표현: `"5 minutes"`, `"1 hour"`, `"30 seconds"`, `"2 days"`
- 자동 밀리초 변환 및 파싱

#### Tumbling Window
```go
spec := op.WindowSpecLite{
    TimeCol:     "ts",
    SizeMillis:  300000, // 5 minutes
    WindowType:  op.WindowTypeTumbling,
}
```

#### Sliding Window
```go
spec := op.WindowSpecLite{
    TimeCol:     "ts",
    SizeMillis:  600000,  // 10 minutes window
    SlideMillis: 300000,  // 5 minutes slide
    WindowType:  op.WindowTypeSliding,
}
```

#### Session Window
```go
spec := op.WindowSpecLite{
    TimeCol:    "ts",
    GapMillis:  300000,  // 5 minutes inactivity gap
    WindowType: op.WindowTypeSession,
}
```

## 빌드 및 실행

```bash
go build ./cmd/ivm
./ivm
```

## 테스트

```bash
go test ./...
```

## 사용 예제

```go
// SQL을 증분 DBSP 그래프로 변환
query := "SELECT region, sales FROM orders WHERE status = 'active'"
incNode, _ := sqlconv.ParseQueryToIncrementalDBSP(query)

// Δ배치 처리
batch := types.Batch{
    {Tuple: types.Tuple{"region": "East", "sales": 100, "status": "active", "id": 1}, Count: 1},
    {Tuple: types.Tuple{"region": "West", "sales": 50, "status": "inactive", "id": 2}, Count: 1},
}
deltas, _ := op.Execute(incNode, batch)
// 결과: region과 sales만 포함, inactive 필터링됨
```

### WHERE 절 복잡한 조건 예제

```go
// 괄호를 사용한 우선순위 제어
query := `
SELECT id, name, age 
FROM customers 
WHERE (age > 50 OR status = 'vip') AND amount > 100
`

// 중첩 괄호 사용
query := `
SELECT * FROM users 
WHERE ((age >= 18 AND age <= 30) OR (age >= 60 AND age <= 70)) 
  AND status = 'active'
`
```

## 구조

- `internal/dbsp/types`: 기본 타입 (`Tuple`, `TupleDelta`, `Batch`)
- `internal/dbsp/op`: 연산자 (`MapOp`, `GroupAggOp`, `ChainedOp`, `Operator` 인터페이스)
- `internal/dbsp/ir`: 논리 계획 (`LogicalScan`, `LogicalFilter`, `LogicalProject`, `LogicalGroupAgg`)
- `internal/dbsp/diff`: **자동 미분** (DBSP 핵심 기능)
- `internal/dbsp/sql`: SQL 파서 및 변환기
