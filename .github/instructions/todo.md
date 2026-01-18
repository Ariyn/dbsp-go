# DBSP-Go TODO

이 파일은 현재 리포에서 **부족한 기능(갭)**을 작업 단위로 정리한 체크리스트입니다.

- 원칙: 이 TODO를 기준으로 작업을 진행하고, 작업을 완료할 때마다 체크/상태를 업데이트합니다.

---

## P0 (엔진 코어/정확성)

- [x] **그래프 실행 모델 구현**
  - 현재 `op.Execute`가 `root.Op.Apply(delta)`만 호출함 → `Node.Inputs` 기반 DAG 실행 필요
  - 목표: 단일/다중 입력 노드, topological order, 입력 라우팅(특히 BinaryOp), 재사용 가능한 실행기
  - 후보 파일: `internal/dbsp/op/operator.go`

- [x] **BinaryOp(Join/Union/Diff) 입력 모델 정리**
  - 현재 Join이 batch의 컬럼 존재 여부로 left/right를 추정 → 취약
  - 목표: left/right 스트림을 명시적으로 분리(예: 별도 노드/포트/태그)하고 실행기가 각 입력을 정확히 전달
  - 후보 파일: `internal/dbsp/op/binary.go`, `internal/dbsp/ir/transform.go`

- [x] **Join 상태(compaction) 및 메모리 누수 방지**
  - 기존 `leftState/rightState`의 `[]TupleDelta` append-only를 multiset(튜플→count)로 정규화
  - count==0 제거로 churn 시 상태 증가 방지
  - 안정성 강화를 위해 처리시간 기반 TTL(만료 시 join 결과 리트랙션) 옵션 추가
  - 후보 파일: `internal/dbsp/op/binary.go`

- [x] **Integrate/Delay를 실제 operator로 정착(또는 설계 재정의)**
  - 현재 integrate는 placeholder, diff는 “대부분 동일 operator 반환” 가정
  - 목표: DBSP 규칙에 맞는 `IntegrateOp`, `DelayOp`를 구현하고, **Delay 포함 사이클만 허용**하는 tick 실행 모델로 확장
  - 설계 RFC: `docs/RFC_DELAY_INTEGRATE.md`
  - 후보 파일: `internal/dbsp/op/integrate.go`, `internal/dbsp/diff/differentiate.go`
  - [x] combinational-cycle validator 추가(“모든 사이클은 Delay 포함” 규칙)
  - [x] Delay-cycle executor 추가(2-phase: read-old/write-new/commit)
  - [x] `DelayOp` 구현(seed/prev/next + commit)
  - [x] `IntegrateOp` 구현(Δ→Value: Z-set 누적) + `ZSetStore/ZSetRef` 정의
  - [x] Differentiate v2 구현: Join/Union/Diff를 (Join은 3항) **명시적 그래프 조합**으로 생성
  - [x] Join 연산자 패밀리 정의/구현(Δ⋈V, V⋈Δ, Δ⋈Δ) 및 테스트
  - [x] tick/사이클 기본 테스트(Delay shift, cycle legality)
  - [x] 미분 정확성 테스트(delete/retraction 포함)

- [x] **증분 정확성 테스트 강화(특히 delete 포함)**
  - Join + delete, Join+Agg + delete, Window + delete 같은 케이스가 핵심
  - DuckDB 대비 통합 테스트를 Join/Window로 확대
  - 후보 파일: `internal/dbsp/sql/duckdb_tpch_integration_test.go`, `internal/dbsp/op/*_test.go`
  - [x] SQL JOIN delete/retraction 테스트 추가
  - [x] SQL JOIN+GROUP BY delete/retraction 테스트 추가
  - [x] SQL JOIN+다중키 GROUP BY delete/retraction 테스트 추가
  - [x] cmd/dbsp 파이프라인 delete 통합 테스트 추가
  - [x] Window(TUMBLING/SLIDING) delete 델타 assert 테스트 추가
  - [x] Window(TUMBLING) 다중키 그룹키/eviction 단위 테스트 추가
  - [x] Window(SLIDING) 다중키 그룹키/eviction 단위 테스트 추가
  - [x] TimeWindowSpec 기반 WindowAgg 다중키 통합 테스트 추가(수동 IR 구성)
  - [x] TimeWindowSpec 기반 Sliding WindowAgg 다중키 통합 테스트 추가(수동 IR 구성)
  - [x] DuckDB TPCH JOIN+AGG incremental delete 통합 테스트 추가
  - [x] DuckDB 대비 Window incremental delete 통합 테스트 확대
  - [x] Join TTL 만료(retraction) 엣지 케이스 팩(5개) 추가
  - [x] Window MIN/MAX delete 엣지 케이스 팩(5개) 추가
  - [x] 순서독립/상쇄 성질 테스트 팩(5개) 추가
  - [x] DuckDB 대비 Sliding window delete 통합 테스트 팩(5개) 추가

---

## P1 (SQL/기능 확장)

- [x] **다중 GROUP BY 키 지원**
  - 현재 다수 경로에서 single-key 제한
  - 목표: composite key(튜플/struct) 또는 안정적인 key encoding 도입
  - 후보 파일: `internal/dbsp/ir/transform.go`, `internal/dbsp/ir/plan.go`

- [x] **다중 집계 지원 (예: `SELECT k, SUM(v), COUNT(*) ...`)**
  - 현재 “multiple aggregate functions not supported yet”
  - 목표: 다중 agg를 하나의 상태로 합치거나, 여러 GroupAgg를 병렬/체인으로 구성
  - 후보 파일: `internal/dbsp/sql/select_helper.go`, `internal/dbsp/ir/transform.go`

- [x] **`COUNT(*)` 의미 정합성 보장**
  - 현재 파싱/변환 흐름상 `COUNT(*)`이 `ColName="*"` 같은 형태로 들어갈 위험
  - 목표: COUNT(*)는 NULL 무시 없이 모든 row count
  - 후보 파일: `internal/dbsp/sql/select_helper.go`, `internal/dbsp/op/groupagg.go`

- [x] **INTERVAL 파싱 확장 (HOUR/DAY 등)**
  - 현재 SECOND/MINUTE만 지원
  - 후보 파일: `internal/dbsp/sql/convert.go`

- [x] **SELECT 표현식 지원 확대**
  - 최소 지원: `CAST`, 산술(+ - * /), `CASE WHEN`
  - 제약: 표현식은 `AS <alias>` 필수(출력 컬럼명 안정화)
  - 후보 파일: `internal/dbsp/sql/select_helper.go`, `internal/dbsp/ir/transform.go`

- [x] **JOIN 조건 확장**
  - 현재 단일 equi-join 조건만 지원
  - 목표: 다중 조건(AND), composite key join
  - 후보 파일: `internal/dbsp/ir/transform.go`

---

## P2 (스트리밍 운영성/완성도)

- [x] **Window 세션 윈도우 정식 구현(merge/extend)**
  - 구현: 파티션별 이벤트 버퍼 유지 후 세션 재계산 + (이전 출력 vs 신규 출력) diff로 retraction/insert emit
  - 포함: 배치 간 extend/merge, delete로 split
  - 후보 파일: `internal/dbsp/op/windowagg.go`

- [x] **워터마크/late-event 처리 end-to-end 연결**
  - `WatermarkAwareWindowOp`는 있으나 SQL 변환/파이프라인과 일관 결합 부족
  - 목표: 설정/플래그로 watermark 활성화, GC 규칙/late policy가 실제 결과에 반영
  - 후보 파일: `internal/dbsp/op/watermark.go`, `internal/dbsp/ir/transform.go`, `cmd/dbsp/*`
  - [x] cmd/dbsp: YAML 설정 + 그래프(WindowAggOp) wrapping 지원
  - [x] SQL: GROUP BY TUMBLE/HOP/SESSION → TimeWindowSpec → WindowAggOp end-to-end 연결 + cmd E2E 테스트

- [x] **상태 스냅샷/복구(옵션)**
  - 목표: 장기 실행/재시작 가능
  - 후보 파일: `internal/dbsp/state/*`
  - [x] SQLite 기반 입력 WAL(append-only) + 재시작 replay (체크포인트 전 단계)
  - [x] 체크포인트(그래프 snapshot) 저장/복구 + suffix replay 연결

- [x] **Parquet Sink (Arrow) + 회전 + 스키마 캐시**
  - 목표: 집계 결과(델타)를 Parquet으로 고속 저장, 시간/배치 단위 파일 회전
  - SQL 분석 시 출력 스키마를 추론하고 `schema_cache_path`에 저장하여 재사용
  - 후보 파일: `cmd/dbsp/sink_parquet.go`, `cmd/dbsp/schema_cache.go`

- [ ] **타입/NULL 처리 정책 정리**
  - 목표: 비교/캐스팅/NULL 전파를 일관된 규칙으로(테스트 포함)
  - 후보 파일: `internal/dbsp/types/*`, `internal/dbsp/op/*`, `internal/dbsp/ir/*`
