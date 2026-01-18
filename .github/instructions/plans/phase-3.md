# Phase 3 — Relational IR → DBSP IR 변환기

## 목표

- Logical IR(Scan/Project/Filter/Join/GroupAgg)을 DBSP IR(Map/Binary/Integrate/Delay)로 변환한다.
- SQL 파서를 필수로 두지 않고도(수동 IR 구성 포함) 변환기가 안정적으로 동작한다.

## 범위(현재 구현 기준)

- 입력: `internal/dbsp/ir/plan.go`의 LogicalNode들
- 출력: `internal/dbsp/op` 실행 그래프(`*op.Node`) — DAG + Source leaf
- Join: 2-way equi-join만 지원(조건 1개 이상, composite key 지원)
- GroupAgg:
	- non-windowed: composite GROUP BY key 지원
	- multi-agg: `SUM(col)` + `COUNT(col)` 조합 지원(출력은 `agg_delta`, `count_delta`)
	- `COUNT(*)`는 multi-agg와 혼합 불가(정책)
- Filter: 단순 WHERE 서브셋(비교, AND/OR, 괄호, IS NULL/NOT NULL)
- Project: 컬럼 선택(필수) + (옵션) expr projection(`Exprs`)

## 산출물(완료 조건)

- 변환기 단위 테스트: LogicalPlan 입력 → DBSP 그래프 구조/연산자 타입이 기대와 일치
- 대표 SQL(또는 수동 LogicalPlan) 3개 이상에 대해 end-to-end 실행이 가능

## 변환 규칙(케이스별)

아래 규칙은 현재 구현의 기준 동작이며, 구현은 `internal/dbsp/ir/transform.go`에 있다.

### 1) Scan

- 입력: `LogicalScan{Table: "t"}`
- 출력: `&op.Node{Source: "t"}`

### 2) Join

- 입력: `LogicalJoin(Left=Scan(a), Right=Scan(b), Conditions=[...])`
- 출력 그래프 형태:
	- 루트: `*op.BinaryOp` (`Type == BinaryJoin`)
	- 입력: `Inputs[0].Source == "a"`, `Inputs[1].Source == "b"`
- 키 함수:
	- 조건 1개면 단일 key 값 사용
	- 조건 2개 이상이면 JSON 기반 composite key로 인코딩
	- 어느 한쪽 key 컴포넌트라도 NULL이면 match 하지 않음

### 3) Filter

- 입력: `LogicalFilter{PredicateSQL, Input=X}`
- 출력:
	- 기본: `*op.MapOp`(predicate true인 row만 통과)
	- 단, Join 위에 붙는 경우(또는 Join → Filter → …)는 Join을 유지하기 위해 다음 형태를 사용:
		- `JoinNode`를 만들고
		- 그 위에 `*op.ChainedOp{Ops:[filterOp, ...]}`를 단일 unary로 얹음

### 4) Project

- 입력: `LogicalProject{Columns, Exprs, Input=X}`
- 출력:
	- `Exprs`가 비어있으면 `*op.MapOp` 기반의 단순 projection
	- `Exprs`가 있으면 `*op.ProjectOp` 사용
	- Join 위에 붙는 경우 Join을 유지하기 위해 `JoinNode`를 Inputs로 두고 `*op.ChainedOp{Ops:[projectOp]}`로 감싼다.

### 5) GroupAgg

- 입력: `LogicalGroupAgg{Keys, Aggs/AggName, Input=X}`
- 출력:
	- non-windowed:
		- `Aggs`가 있으면 `op.NewGroupAggMultiOp`(multi-agg)
		- `Aggs`가 없으면 `op.NewGroupAggOp`
	- Filter(Join 위 포함) + GroupAgg:
		- `*op.ChainedOp{Ops:[filterOp, groupAggOp]}` 형태
		- Join인 경우 JoinNode를 Inputs로 둠
	- Join + GroupAgg:
		- JoinNode를 Inputs로 두고 `GroupAggOp`를 루트로 둔다.

## 검증 체크리스트(테스트에 반영)

- 구조 검증
	- Join: 루트 op 타입이 `*op.BinaryOp`인지, `BinaryJoin`인지, Source가 2개인지
	- Join 위 unary(필터/프로젝션/집계): Join이 Inputs로 유지되는지(2-input DAG 보존)
	- GroupAgg(multi-agg): `Aggs` slot 수/순서가 기대와 일치하는지
- 실행 검증
	- `op.ExecuteTick`으로 tick 입력을 번갈아 주입했을 때(좌/우), 최종 누적 스냅샷이 기대값과 동일한지
	- delete/retraction이 들어올 때 sum/count가 되돌아가는지

참고: Phase 3 시작용 테스트는 `internal/dbsp/ir/phase3_transform_test.go`에 추가한다.

## 다음에 할 일(요약)

- (완료) 변환 규칙 문서화(케이스별)
- 표현식/타입 힌트 처리 일관화(예: 숫자 타입 승격, NULL 비교 규칙)
- planner/optimizer(선택) 포인트 정의(푸시다운, projection pruning 등)
