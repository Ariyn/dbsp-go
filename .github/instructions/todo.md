
# DBSP-Go TODO (Phase 기반)

이 파일은 **현재 단계(Phase)** 기준으로 할 일을 관리합니다.

- 원칙: 작업을 시작하기 전에 항목을 추가하고, 완료 시 체크/상태를 업데이트합니다.
- 로드맵: [.github/instructions/master-plan.instructions.md](master-plan.instructions.md)
- 상세 계획: `.github/instructions/plans/` (특히 `plans/phase-3.md`)

---

## 현재 단계: Phase 3 — Relational IR → DBSP IR 변환기

### Phase 3 완료 조건(최소)

- [x] 변환기 단위 테스트: LogicalPlan → DBSP 그래프 구조/연산자 타입이 기대와 일치
- [x] 대표 SQL(또는 수동 LogicalPlan) 3개 이상에 대해 end-to-end 실행 가능

### Phase 3 작업 목록

- [x] **단위 테스트: LogicalJoin → DBSP(BinaryJoin) 변환 구조 검증**
- [x] **단위 테스트: LogicalJoin + LogicalGroupAgg(multi-agg) 변환 구조 검증**
- [x] **단위 테스트: Filter(Join 위) + GroupAgg 변환 구조 검증**
- [x] **E2E(수동 LogicalPlan): Join → GroupAgg 실행 1개**
- [x] **E2E(수동 LogicalPlan): Filter(Join 위) → GroupAgg 실행 1개**
- [x] **E2E(수동 LogicalPlan): Join → Project(필요 시 expr 포함) 실행 1개**

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

- [ ] 변환기 단위 테스트(논리 계획 → DBSP 그래프 구조)
- [ ] SQL/수동 LogicalPlan 3개 이상 end-to-end 실행 가능

### Phase 4 — 최적화/운영성

- [ ] 타입/NULL 처리 정책 정리(테스트 포함)
- [ ] 메모리/상태(compaction/GC) 기준 정리 및 벤치마크 기준선 확보
