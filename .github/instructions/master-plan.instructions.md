---
applyTo: '**'
---
# DBSP 기반 Incremental View Maintenance Engine (Go) — Implementation Spec

이 문서는 Golang으로 DBSP(DataBase Signal Processing) 모델을 기반으로 한
Incremental View Maintenance(IVM) 엔진을 구현하기 위한 기술 지침서이다.

상세 실행 계획(단계별 작업 순서/완료 조건/검증)은 `.github/instructions/plans/` 를 참고한다.

작업 규칙: todo.md에서 할 일을 읽고, 완료할때마다 todo.md 파일을 업데이트 할 것

## 0. Scope

- 초기 버전에서 지원할 대상:
  - 단일 테이블 또는 2-way JOIN
  - Projection, Filter, Join, GroupAggregate
  - SUM, COUNT 등 기본 Aggregation
  - Bag semantics (+1 / -1 multiplicity)
  - 입력은 ΔBatch(트랜잭션 변화 묶음)
  - 재귀, 비단조성 복잡 케이스는 제외

---

## 1. Delta Model

### Delta Tuple
```go
type Tuple map[string]any

type TupleDelta struct {
    Tuple Tuple
    Count int64 // +1 insert, -1 delete
}

type Batch []TupleDelta
```

- 모든 연산자는 Batch를 입력/출력으로 처리한다.

- Snapshot은 유지하지 않으며 integrate 연산자가 state를 구성한다.

## 2. IR 설계

### 2.1 Relational IR (논리 계획)

- LogicalScan

- LogicalProject

- LogicalFilter

- LogicalJoin

- LogicalGroupAgg

### 2.2 DBSP IR
- Map

- Binary

- Integrate

- Delay (필요 시)

- 그 외 모두 이 4개로 변환한다.

## 3. DBSP Operator Interface
```go
type Operator interface {
    Apply(batch Batch) (Batch, error)
}

type Node struct {
    Op     Operator
    Inputs []*Node
}
```

## 4. DBSP 기본 연산자 구현
### 4.1 Map Operator
```go
type MapOp struct {
    F func(TupleDelta) []TupleDelta
}
```
- Projection, Filter, Column 계산은 모두 Map에서 처리한다.

### 4.2 Binary Operator (Union/Diff/Join)

조인 구현 시 내부에 해시 인덱스(state map)를 유지한다.

예:

```go
type JoinOp struct {
    LeftKeyFn  func(Tuple) any
    RightKeyFn func(Tuple) any
    CombineFn  func(l, r Tuple) Tuple

    leftState  map[any][]Tuple
    rightState map[any][]Tuple
}
```

- ΔJoin = ΔR⋈S + R⋈ΔS + ΔR⋈ΔS 규칙에 따라 델타를 propagate한다.

### 4.3 GroupAgg Operator (Aggregation/Integrate)
```go
type AggFunc interface {
    Apply(prev any, td TupleDelta) (new any, outDelta *TupleDelta)
}

type GroupAggOp struct {
    KeyFn   func(Tuple) any
    AggInit func() any
    AggFn   AggFunc
    state   map[any]any // key → current agg value
}
```
- 입력은 ΔBatch

- state는 integrate가 담당

- 출력은 ΔAggregate(변화된 group 결과)

## 5. DBSP 자동 증분화 (Differentiation)

DBSP IR 그래프 Q → ΔQ 그래프를 생성한다.

*연산자별 미분 규칙*
|연산자|	미분 결과|
|---|---|
|map(f)|	map(f)|
|binary(S ⊙ T)|	dS ⊙ T + S ⊙ dT + dS ⊙ dT|
|delay|	delay(dS)|
|integrate|	identity|

*Differentiation 알고리즘*

```go
func Differentiate(n *Node) *Node {
    switch op := n.Op.(type) {
    case *MapOp:
        return NewMapNode(op.F, Differentiate(n.Inputs[0]))

    case *BinaryOp: // join/union/diff
        return DifferentiateBinary(n)

    case *GroupAggLogicalOp:
        dInput := Differentiate(n.Inputs[0])
        return NewGroupAggNode(op.KeyFn, op.AggInit, op.AggFn, dInput)

    default:
        panic("unsupported operator")
    }
}
```

## 6. Execution Model
- Push 모델(ΔBatch가 소스에서 들어오면 위→아래로 propagate)

- DAG topological order로 실행

- 각 Operator는 내부 state를 유지해 집계/조인을 처리

- Return: ΔView (Output delta)
  
```go
func Execute(root *Node, delta Batch) (Batch, error)
```

## 7. 단계별 개발 로드맵
*Phase 1 — 단일 테이블 GroupAggregate IVM*
- Map → GroupAgg → Integrate

- 기본적인 Δ 처리 및 상태 유지 검증

*Phase 2 — Join + GroupAggregate*
- Hash Join Op 구현

- ΔJoin 규칙 구현

- GroupAgg로 전달 및 ΔView 생성

*Phase 3 — Relational IR → DBSP IR 변환기*
- LogicalJoin, LogicalGroupAgg 등을 DBSP 노드로 변환

- SQL 파서(옵션) 또는 수동 IR 생성

*Phase 4 — 최적화 및 실행 엔진 정교화*
- Memory 관리

- Concurrency(optional)

- Snapshot/Restore(Optional)

8. 최종 목표
- ΔBatch 입력 → DBSP ΔGraph 실행 → ΔView 출력

- Full snapshot은 integrate로 복원

- 기존 DB 테이블의 전체 재계산 없이 실시간 반영 가능한 IVM 엔진 구축