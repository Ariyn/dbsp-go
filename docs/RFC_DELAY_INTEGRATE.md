# RFC: First-class Delay/Integrate + Explicit Differentiation (DBSP-Go)

## 1. Summary
이 RFC는 DBSP-Go에 DBSP 이론에 맞는 `Delay`/`Integrate`를 **1급 operator**로 도입하고,
실행기가 **Delay를 포함한 사이이클만 허용**하도록 실행 모델을 재정의한다.
또한 미분(dQ) 생성 시 Binary(특히 Join)는 “operator 내부 구현”에 의존하지 않고,
**3항 결합 그래프를 명시적으로 조합**하도록 설계한다.

## 2. Background (Current)
- 실행기: [internal/dbsp/op/operator.go](../internal/dbsp/op/operator.go)
  - per-tick DAG 평가
  - 사이클 발견 시 즉시 에러
- 미분기: [internal/dbsp/diff/differentiate.go](../internal/dbsp/diff/differentiate.go)
  - “대부분 operator가 delta-native” 가정으로 토폴로지만 복제
- Integrate: [internal/dbsp/op/integrate.go](../internal/dbsp/op/integrate.go)
  - placeholder (실제 누적 의미 없음)
- Join: [internal/dbsp/op/binary.go](../internal/dbsp/op/binary.go)
  - 내부 state 기반으로 product rule(3항)을 “내장 구현”

본 RFC는 엔진을 DBSP식 "값 스트림 Q"와 "델타 스트림 dQ"로 분리 가능한 방향으로 되돌린다.

## 3. Goals / Non-goals
### Goals
- G1: 사이클 허용 조건을 고정한다: **모든 사이클은 Delay를 반드시 포함**
- G2: Value를 DBSP에 맞게 정의한다: tick마다 관계는 **Z-set(튜플→정수 가중치)**
- G3: Binary 미분은 반드시 명시적으로 조합한다:
  - $d(S \odot T) = (dS \odot T) + (S \odot dT) + (dS \odot dT)$
- G4: Delay/Integrate의 tick semantics(2-phase) 명시

### Non-goals
- N1: Delay 없는 즉시 피드백(combinational cycle) 지원
- N2: 재귀/고정점(반복 평가) 지원
- N3: 기존 delta-native operator를 전부 즉시 교체

## 4. Terms
- tick: 논리 시간 스텝(실행기 1회 호출)
- Z-set: 관계의 DBSP 표현. 튜플 t의 가중치 w(t)∈Z
- Value stream: tick별 Z-set 스냅샷 S[t]
- Delta stream: tick별 변화량 ΔS[t]

현재 `types.Batch`(TupleDelta 리스트)는 유지하되,
**Batch가 Value인지 Delta인지는 그래프 메타데이터로 구분**한다.

## 5. Cycle Rule (Fixed)
- 그래프에 사이클이 존재하더라도, **모든 단순 사이클은 최소 1개의 Delay를 포함**해야 한다.
- Delay를 제거(또는 register로 수축)한 그래프는 DAG여야 한다.

### Validation
- 컴파일/실행 전 `ValidateNoCombinationalCycles(root)`로 검증한다.
  - 위반 시: "Delay 없는 사이클" 에러

## 6. Operator Semantics
### 6.1 Delay (1-tick register)
- 의미:
  - `delay(S)[0] = seed`
  - `delay(S)[t] = S[t-1]`
- 실행 의미: **read-old / write-new (2-phase commit)**
  - tick t에서 출력은 prev
  - tick t에서 입력을 next로 저장
  - tick 종료 시 prev=next로 커밋

### 6.2 Integrate (delta → value)
- 의미:
  - `I[t] = I[t-1] + ΔS[t]` (Z-set 덧셈)
- Integrate는 레지스터가 아니므로, **사이클 허용의 근거는 Delay**다.

## 7. Value Representation (DBSP-aligned)
- 의미적으로 Value는 tick마다 **완전한 Z-set 스냅샷**이다.
- 런타임 최적화를 위해, 스냅샷을 매 tick `Batch`로 물질화하지 않고
  `ZSetRef`(읽기 전용 참조 핸들)로 전달할 수 있다.

### Required minimal capabilities
- `Iter()` (전체 순회)
- `LookupByKey(k)` (Join 가속)

Integrate는 내부에 `ZSetStore`를 유지하고, 이를 `ZSetRef`로 노출한다.

## 8. Executor Model (Delay cycles)
### Key idea
Delay 노드를 논리적으로 분해해 다룬다:
- DelayRead: tick 시작 시점 prev를 내보내는 소스
- DelayWrite: tick 계산 결과를 next로 저장하는 싱크

### Tick phases
1) Read: DelayRead가 prev 제공
2) Compute: combinational DAG를 topo 순서로 평가
3) Write: DelayWrite가 next 저장
4) Commit: 모든 Delay가 prev=next 커밋

## 9. Differentiation (Explicit Join composition)
### Rules
- Map: `d(map(f,S)) = map(f, dS)`
- Delay: `d(delay(S)) = delay(dS)`
- Integrate: `d(integrate(ΔS)) = ΔS`
- Binary: product rule 3항

### Join: MUST be explicit 3-term graph
`J = R ⋈ S`에서:
- `ΔJ = (ΔR ⋈ S) + (R ⋈ ΔS) + (ΔR ⋈ ΔS)`
- 여기서 `R`와 `S`는 각각 `integrate(ΔR)`, `integrate(ΔS)`로 얻는 Value다.

따라서 dQ는 다음 서브그래프를 반드시 가진다:
- Join(ΔR, S_value)
- Join(R_value, ΔS)
- Join(ΔR, ΔS)
- 위 3개를 Union(+)으로 합침

## 10. Staged Migration
- M1: combinational-cycle validator + Delay-cycle executor
- M2: DelayOp/IntegrateOp + ZSetStore/ZSetRef
- M3: Differentiate v2에서 Join 3항 명시 조합 생성
- M4: 기존 join 내장 product rule 구현은 최적화/레퍼런스로만 유지(스펙 기준은 M3)

## 11. Minimal Test Matrix
- Delay: seed/1tick shift, delete(음수 가중치)도 1tick 지연
- Cycles:
  - Delay 없는 사이클은 실패
  - Delay 포함 사이클은 성공 + tick 결과 결정적
- Join differentiate:
  - 3항 명시 조합 dQ 결과가 기준값과 일치
  - insert/delete 혼합, 중복도(count>1) 포함
- Integrate:
  - Δ 누적 결과가 Z-set 스냅샷 정의와 일치
