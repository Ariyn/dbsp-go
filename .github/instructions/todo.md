# DBSP-Go TODO

이 파일은 현재 리포에서 **부족한 기능(갭)**을 작업 단위로 정리한 체크리스트입니다.

- 원칙: 이 TODO를 기준으로 작업을 진행하고, 작업을 완료할 때마다 체크/상태를 업데이트합니다.

---

## P0 (엔진 코어/정확성)

- [ ] **그래프 실행 모델 구현**
  - 현재 `op.Execute`가 `root.Op.Apply(delta)`만 호출함 → `Node.Inputs` 기반 DAG 실행 필요
  - 목표: 단일/다중 입력 노드, topological order, 입력 라우팅(특히 BinaryOp), 재사용 가능한 실행기
  - 후보 파일: `internal/dbsp/op/operator.go`

- [ ] **BinaryOp(Join/Union/Diff) 입력 모델 정리**
  - 현재 Join이 batch의 컬럼 존재 여부로 left/right를 추정 → 취약
  - 목표: left/right 스트림을 명시적으로 분리(예: 별도 노드/포트/태그)하고 실행기가 각 입력을 정확히 전달
  - 후보 파일: `internal/dbsp/op/binary.go`, `internal/dbsp/ir/transform.go`

- [ ] **Join 상태(compaction) 및 메모리 누수 방지**
  - 현재 `leftState/rightState`가 `[]TupleDelta` append-only
  - 목표: multiset(튜플→count) 형태로 정규화, count==0 제거, 장기 실행에서 상태가 커지지 않도록 GC/정리 규칙 추가
  - 후보 파일: `internal/dbsp/op/binary.go`

- [ ] **Integrate/Delay를 실제 operator로 정착(또는 설계 재정의)**
  - 현재 integrate는 placeholder, diff는 “대부분 동일 operator 반환” 가정
  - 목표: DBSP 규칙에 맞는 `IntegrateOp`, `DelayOp`를 구현하거나, 현재 방식(상태 내장형 operator)과의 일관된 스펙을 문서화
  - 후보 파일: `internal/dbsp/op/integrate.go`, `internal/dbsp/diff/differentiate.go`

- [ ] **증분 정확성 테스트 강화(특히 delete 포함)**
  - Join + delete, Join+Agg + delete, Window + delete 같은 케이스가 핵심
  - DuckDB 대비 통합 테스트를 Join/Window로 확대
  - 후보 파일: `internal/dbsp/sql/duckdb_tpch_integration_test.go`, `internal/dbsp/op/*_test.go`

---

## P1 (SQL/기능 확장)

- [ ] **다중 GROUP BY 키 지원**
  - 현재 다수 경로에서 single-key 제한
  - 목표: composite key(튜플/struct) 또는 안정적인 key encoding 도입
  - 후보 파일: `internal/dbsp/ir/transform.go`, `internal/dbsp/ir/plan.go`

- [ ] **다중 집계 지원 (예: `SELECT k, SUM(v), COUNT(*) ...`)**
  - 현재 “multiple aggregate functions not supported yet”
  - 목표: 다중 agg를 하나의 상태로 합치거나, 여러 GroupAgg를 병렬/체인으로 구성
  - 후보 파일: `internal/dbsp/sql/select_helper.go`, `internal/dbsp/ir/transform.go`

- [ ] **`COUNT(*)` 의미 정합성 보장**
  - 현재 파싱/변환 흐름상 `COUNT(*)`이 `ColName="*"` 같은 형태로 들어갈 위험
  - 목표: COUNT(*)는 NULL 무시 없이 모든 row count
  - 후보 파일: `internal/dbsp/sql/select_helper.go`, `internal/dbsp/op/groupagg.go`

- [ ] **INTERVAL 파싱 확장 (HOUR/DAY 등)**
  - 현재 SECOND/MINUTE만 지원
  - 후보 파일: `internal/dbsp/sql/convert.go`

- [ ] **SELECT 표현식 지원 확대**
  - 현재 컬럼/집계/*만 허용
  - 목표(최소): `CAST`, 산술(+ - * /), `CASE WHEN` 등 중 일부
  - 후보 파일: `internal/dbsp/sql/select_helper.go`, `internal/dbsp/ir/transform.go`

- [ ] **JOIN 조건 확장**
  - 현재 단일 equi-join 조건만 지원
  - 목표: 다중 조건(AND), composite key join
  - 후보 파일: `internal/dbsp/ir/transform.go`

---

## P2 (스트리밍 운영성/완성도)

- [ ] **Window 세션 윈도우 정식 구현(merge/extend)**
  - 현재 주석대로 단순화 구현(이벤트별 새 세션)
  - 목표: 동일 파티션에서 gap 내 이벤트로 세션 확장/병합
  - 후보 파일: `internal/dbsp/op/windowagg.go`

- [ ] **워터마크/late-event 처리 end-to-end 연결**
  - `WatermarkAwareWindowOp`는 있으나 SQL 변환/파이프라인과 일관 결합 부족
  - 목표: 설정/플래그로 watermark 활성화, GC 규칙/late policy가 실제 결과에 반영
  - 후보 파일: `internal/dbsp/op/watermark.go`, `internal/dbsp/ir/transform.go`, `cmd/dbsp/*`

- [ ] **상태 스냅샷/복구(옵션)**
  - 목표: 장기 실행/재시작 가능
  - 후보 파일: `internal/dbsp/state/*`

- [ ] **타입/NULL 처리 정책 정리**
  - 목표: 비교/캐스팅/NULL 전파를 일관된 규칙으로(테스트 포함)
  - 후보 파일: `internal/dbsp/types/*`, `internal/dbsp/op/*`, `internal/dbsp/ir/*`
