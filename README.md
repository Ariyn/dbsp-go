# DBSP-Go (Incremental View Maintenance)

Go로 구현한 DBSP(DataBase Signal Processing) 기반 IVM(Incremental View Maintenance) 엔진입니다.

현재 리포는 설정 파일(YAML)로 **Source → (SQL Transform) → Sink** 파이프라인을 실행하는 CLI를 포함합니다.

## 빠른 시작 (CLI)

```bash
# 바로 실행
go run ./cmd/dbsp -config examples/config.yaml

# 또는 바이너리로 빌드 후 실행
go build -o dbsp ./cmd/dbsp
./dbsp -config examples/config.yaml
```

- CLI 플래그: `-config` (기본값 `config.yaml`)
- 설정 스키마/Source/Sink 상세: [cmd/dbsp/README.md](cmd/dbsp/README.md)

## 지원 기능(요약)

- **SQL → DBSP 변환 + 증분 실행**: `ParseQueryToIncrementalDBSP`로 SQL을 증분 그래프로 컴파일 후 Δ배치를 흘려서 Δ결과를 출력
- **Bag semantics**: 입력/출력은 `TupleDelta{Tuple, Count}` 형태로 +1/-1 multiplicity를 사용
- **Projection / Filter(WHERE)**
  - 비교: `=`, `!=`, `<`, `<=`, `>`, `>=`
  - 논리: `AND`, `OR` (괄호로 우선순위 제어 지원)
- **GROUP BY + Aggregates**: `SUM`, `COUNT` (다중 GROUP BY 키 지원)
- **JOIN**: 2-way equi-join 중심 (delete/retraction 포함 증분 전파 테스트 포함)
- **시간 기반 윈도우 집계**: Tumbling / Sliding / Session
- **Join state TTL (옵션)**: 설정의 `transform.join_ttl`로 조인 상태를 처리시간 기준으로 만료시키고 관련 retraction을 생성

## 제한 사항(현재)

- `SELECT k, SUM(v), COUNT(*)`처럼 **다중 집계 함수**는 아직 제한적일 수 있습니다.
- `COUNT(*)` 의미/호환성, JOIN 조건 확장(다중 조건/복합키), INTERVAL 파싱 확장 등은 TODO에 있습니다.

## 테스트

```bash
go test ./...
```

## 구조

- `cmd/dbsp`: YAML 기반 파이프라인 CLI
- `internal/dbsp/sql`: SQL 파서 및 변환기
- `internal/dbsp/ir`: LogicalPlan 및 변환
- `internal/dbsp/diff`: DBSP 그래프 자동 미분(증분화)
- `internal/dbsp/op`: 실행기/연산자(Join/GroupAgg/Window/Delay/Integrate 등)
