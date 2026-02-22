# Phase 5 — Non-Recursive CTE (Single Query Scope, Temporary Node)

## 목표

- `WITH`(비재귀 CTE)를 단일 쿼리 내부에서 지원한다.
- CTE를 논리 계획에서 임시 노드로 유지하고, DBSP 변환 시 공유 서브그래프로 연결한다.
- 동일 스코프 CTE 이름 중복은 **후속 정의가 덮어쓴다**.
- 미정의 CTE 참조는 **에러**로 처리한다.

## 범위/제약

- 포함: `WITH cte AS (subquery) SELECT ...` (다중 CTE 정의 포함)
- 제외: `WITH RECURSIVE`, 쿼리 바깥 스코프 공유, 뷰/카탈로그 영구 등록
- 스코프: 현재 최상위 단일 쿼리 실행 경로 내부만 유효

## Task 1 — SQL 파싱/바인딩 경로에 CTE 컨텍스트 도입

### 해야 할 일

- SQL 변환 진입점에서 CTE 목록을 추출한다.
- CTE 이름 → 서브쿼리(또는 변환된 LogicalNode) 매핑 컨텍스트를 만든다.
- 동일 이름이 다시 정의되면 기존 엔트리를 덮어쓴다.

### 진행 방법

1. `internal/sql/convert.go`에서 top-level query 변환 시작 시 CTE 컨텍스트 초기화
2. `internal/sql/select_helper.go`/`internal/sql/dml.go`에서 `WITH` 구문 순회
3. CTE 정의 순서대로 map 갱신(후속 정의 덮어쓰기)

### 작성할 테스트

- `internal/sql/dml_test.go`
  - 다중 CTE 정의 시 마지막 정의가 사용되는지
  - CTE 내부 subquery가 정상 파싱/바인딩되는지

### 주의점

- CTE 컨텍스트가 다음 쿼리로 누수되지 않도록 변환 단위로 분리
- 기본 테이블 이름과 CTE 이름 충돌 시 현재 쿼리에서는 CTE 우선 해석

## Task 2 — Logical Plan에 CTE 임시 노드/참조 모델 반영

### 해야 할 일

- CTE를 표현할 임시 노드(또는 plan-level CTE registry)를 추가한다.
- Scan/참조 해석 시 CTE 이름이면 임시 노드 참조로 연결한다.

### 진행 방법

1. `internal/ir/plan.go`에 CTE 표현 구조 추가
2. SQL→Logical 변환 단계에서 CTE 정의 노드를 먼저 생성
3. 본문 SELECT의 테이블 참조 해석 시 CTE 우선 조회

### 작성할 테스트

- `internal/sql/sql_test.go`
  - CTE 참조가 LogicalScan(실테이블)로 떨어지지 않고 CTE 참조로 연결되는지
  - 동일 CTE를 두 번 참조할 때 plan 구조가 의도대로 생성되는지

### 주의점

- 미정의 CTE 참조는 즉시 에러 반환(폴백 금지)
- 아직 재귀를 지원하지 않으므로 self-reference 검출 시 명시적 에러

## Task 3 — DBSP 변환에서 CTE 공유 서브그래프 보장

### 해야 할 일

- CTE 임시 노드를 DBSP 노드로 변환할 때 memoization을 적용한다.
- 동일 CTE 참조가 여러 번 등장해도 동일 DBSP 노드를 공유한다.

### 진행 방법

1. `internal/ir/transform.go`에 CTE 변환 캐시(이름/노드 기반) 추가
2. CTE 참조 변환 시 캐시 조회 후 기존 노드 재사용
3. JOIN/GroupAgg 등 상위 연산자는 기존 변환 규칙 유지

### 작성할 테스트

- `internal/ir/phase3_transform_test.go`
  - 동일 CTE 다중 참조 시 변환 결과가 중복 생성 없이 공유되는지
  - CTE + Join + GroupAgg 조합 구조 검증

### 주의점

- 공유 노드의 상태 오염을 막기 위해 입력 연결 순서/방향 일관성 유지
- 기존 비-CTE 경로 성능/동작 회귀가 없도록 기존 테스트 전수 통과 확인

## Task 4 — SQL E2E/에러 케이스 고정

### 해야 할 일

- SQL 파서 경유 end-to-end 테스트에 CTE 케이스를 추가한다.
- 오류 정책(미정의 CTE, 재귀 CTE 미지원)을 테스트로 고정한다.

### 진행 방법

1. `internal/sql/phase3_sql_e2e_test.go`에 정상 경로 케이스 추가
2. `internal/sql/sql_test.go` 또는 `internal/sql/dml_test.go`에 에러 케이스 추가
3. 필요 시 실행 엔진까지 연결해 결과 스냅샷 검증

### 작성할 테스트

- 정상
  - 단일 CTE + Filter + GroupAgg
  - 다중 CTE + Join + multi-agg
  - 동일 스코프 이름 재정의(후속 정의 적용)
- 오류
  - 미정의 CTE 참조 에러
  - `WITH RECURSIVE` 입력 시 미지원 에러

### 주의점

- 기대값 검증은 최종 누적 스냅샷 기준(기존 프로젝트 정책 유지)
- 에러 문자열은 핵심 키워드 기준으로 assert(과도한 문구 고정 금지)

## 완료 조건(DoD)

- CTE 관련 신규 테스트(단위 + E2E)가 모두 통과
- 기존 SQL/IR/Operator 테스트에 회귀 없음
- `todo.md`의 CTE 항목이 체크되고, 문서/테스트 링크가 최신 상태

## 권장 실행 순서

1. Task 1(파싱/바인딩)
2. Task 2(Logical CTE 모델)
3. Task 3(DBSP 변환 공유)
4. Task 4(E2E + 에러 정책)
