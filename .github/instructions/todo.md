
# DBSP-Go TODO (Phase 기반)

이 파일은 **현재 단계(Phase)** 기준으로 할 일을 관리합니다.

- 원칙: 작업을 시작하기 전에 항목을 추가하고, 완료 시 체크/상태를 업데이트합니다.
- 로드맵: [.github/instructions/master-plan.instructions.md](master-plan.instructions.md)
- 상세 계획: `.github/instructions/plans/` (특히 `plans/phase-3.md`)

---

## 현재 단계: Phase 4 — 최적화/운영성

### Phase 6 진행 — SQL Window/Expression 확장 (논문 범위 유지) [진행 중]

- [ ] **범위/제약 문서 고정**
  - 신규 기능은 Map/Binary/Delay/Integrate로만 하향 변환

- [ ] **SQL 파서 조합성 개선**
  - LAG/윈도우 집계가 있어도 WITH/WHERE/PROJECT/GROUP BY/ORDER BY와 조합

- [ ] **표현식 확장(Map 기반)**
  - time_bucket, epoch, interval, strftime, JSON state->, :: 캐스트

- [ ] **ORDER BY 경로 추가**
  - LogicalSort 파싱 및 SortOp 변환

- [ ] **윈도우 체이닝 정리**
  - LAG/윈도우 집계 결과가 Project/GroupAgg와 체이닝

- [ ] **E2E 정합성 검증**
  - 목표 쿼리 동일 출력 테스트 추가

### Phase 5 완료 — Non-Recursive CTE (단일 쿼리, 임시 노드) [완료: 2026-02-22]

- [x] **CTE 파싱/바인딩 컨텍스트 도입**
  - `WITH` 정의 순서대로 등록
  - 동일 이름 재정의 시 후속 정의로 덮어쓰기

- [x] **Logical Plan CTE 임시 노드/참조 반영**
  - CTE 참조를 실테이블 Scan과 구분
  - 미정의 CTE 참조 시 에러 처리

- [x] **DBSP 변환 시 CTE 공유 서브그래프 보장**
  - 동일 CTE 다중 참조 시 변환 노드 재사용
  - 기존 Join/GroupAgg 규칙 회귀 없음

- [x] **CTE 테스트(단위 + E2E) 추가**
  - 정상: 단일/다중 CTE, 재정의, Join+GroupAgg
  - 오류: 미정의 CTE, `WITH RECURSIVE` 미지원

### Phase 4 작업 목록(우선순위)

- [x] **타입/NULL 처리 정책을 테스트로 고정**
  - 비교(=, <, <=, >, >=)에서 숫자 문자열/혼합 숫자 타입
  - IS NULL / IS NOT NULL
  - 집계(SUM/COUNT)에서 숫자 문자열/NULL 처리

- [x] **상태/메모리 제어 규칙 정의(최소 1개)**
  - Join/GroupAgg state가 커지는 조건 명시
  - watermark/TTL 기반 compaction/GC 트리거 중 하나를 코드+테스트로 고정

- [x] **E2E 테스트 강화 (cmd/e2e 패키지)**
  - [x] **Task 01: WAL 복구 및 정합성 검증**
  - [x] **Task 02: Watermark 진행 및 윈도우 집계**
  - [x] **Task 03: Late Ingest(지연 데이터) 처리**

- [x] **벤치마크 기준선 추가(대표 1~2개)**
  - 단일 GroupAgg
  - Join + GroupAgg

### Phase 3 완료 조건(최소)

- [x] 변환기 단위 테스트: LogicalPlan → DBSP 그래프 구조/연산자 타입이 기대와 일치
- [x] 대표 SQL(또는 수동 LogicalPlan) 3개 이상에 대해 end-to-end 실행 가능

### Phase 3 마감(정책/문서/테스트 고정)

- [x] Phase 3 문서에 COUNT(*) multi-agg 허용 및 관대 타입(숫자 문자열) 규칙 반영
- [x] SQL E2E에서 COUNT(*) + JOIN + multi-agg 경로까지 테스트로 고정

### Phase 3 작업 목록

- [x] **단위 테스트: LogicalJoin → DBSP(BinaryJoin) 변환 구조 검증**
- [x] **단위 테스트: LogicalJoin + LogicalGroupAgg(multi-agg) 변환 구조 검증**
- [x] **단위 테스트: Filter(Join 위) + GroupAgg 변환 구조 검증**
- [x] **E2E(수동 LogicalPlan): Join → GroupAgg 실행 1개**
- [x] **E2E(수동 LogicalPlan): Filter(Join 위) → GroupAgg 실행 1개**
- [x] **E2E(수동 LogicalPlan): Join → Project(필요 시 expr 포함) 실행 1개**

- [x] **E2E(SQL): ParseQueryToDBSP 경로 3개 고정**
  - Filter + GroupAgg(multi-agg)
  - Join + GroupAgg(multi-agg)
  - Filter(Join 위) + GroupAgg(multi-agg)

- [x] **COUNT(*)를 multi-agg에서 허용(변환기/SQL 경로) + 테스트 추가**
- [x] **관대 타입 처리: 숫자 문자열을 비교/집계(SUM)에서 허용 + 테스트 고정**

### (완료) Phase 2 — Join + GroupAggregate

- [x] JOIN + GROUP BY 쿼리 2종(Q1 SUM, Q2 COUNT)에 대해 delete/retraction 포함 E2E가 통과
- [x] E2E는 "델타"가 아니라 "델타 누적 후 최종 스냅샷" 기준으로 동일성 검증

#### Phase 2 작업 목록

- [x] **E2E: Parquet sink 결과 동일성 검증**
  - 파이프라인 실행 → Parquet 파일 생성 → Parquet 재로딩
  - 키별로 `agg_delta`/`count_delta` 누적해 최종 집계 결과 복원
  - 동일 입력을 전체 스캔해 기대 결과 계산 후 비교

- [x] **E2E: JOIN + GROUP BY (delete 포함) 대표 케이스 1개 추가**
  - join key 1개, group key 1개부터 시작

- [x] **E2E: JOIN + GROUP BY (복합키) 대표 케이스 1개 추가**
  - join key 2개 또는 group key 2개

- [x] **운영성(선택): WAL 체크포인트/복구 E2E 1개 추가**
  - 체크포인트 저장 → 재시작(replay) → 동일 결과 확인

---

## 다음 단계(요약)

### Phase 3 — Relational IR → DBSP IR 변환기

- [x] 변환기 단위 테스트(논리 계획 → DBSP 그래프 구조)
- [x] SQL/수동 LogicalPlan 3개 이상 end-to-end 실행 가능

### Phase 4 — 최적화/운영성

- [x] 타입/NULL 처리 정책 정리(테스트 포함)
- [x] 메모리/상태(compaction/GC) 기준 정리 및 벤치마크 기준선 확보
