# DBSP-Go (Incremental View Maintenance)

Go로 구현한 DBSP(DataBase Signal Processing) 기반 IVM(Incremental View Maintenance) 엔진입니다.

현재 리포는 설정 파일(YAML)로 **Source → (SQL Transform) → Sink** 파이프라인을 실행하는 CLI를 포함합니다.

## 빠른 시작 (CLI)

```bash
# 실행 (사용자 YAML 필요)
go run ./cmd/dbsp -config ./your-config.yaml

# 또는 바이너리로 빌드 후 실행
go build -o dbsp ./cmd/dbsp
./dbsp -config ./your-config.yaml
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

## 런타임 최소 계약

- `source.type=http`
- `sink.type=http_pull`
- `transform.type=sql`
- 아래 기능은 제거됨: `partition`, `wal/checkpoint`, `state_backend`, `transform.ttl`, `transform.watermark`, `DML(INSERT/UPDATE/DELETE)`

## 제한 사항(현재)

- `SELECT k, SUM(v), COUNT(*)`처럼 **다중 집계 함수**는 아직 제한적일 수 있습니다.
- `COUNT(*)` 의미/호환성, JOIN 조건 확장(다중 조건/복합키), INTERVAL 파싱 확장 등은 TODO에 있습니다.

## 테스트

```bash
go test ./...
```

## 관측성

- Prometheus 메트릭 노출: `DBSP_METRICS_ADDR=:9090`
- 메트릭 경로 변경: `DBSP_METRICS_PATH=/metrics`
- pprof 노출: `DBSP_PPROF_ADDR=:6060`
- 같은 주소를 쓰면 하나의 HTTP 서버에서 `pprof`와 `metrics`를 함께 노출합니다.
- 샘플 Prometheus + Grafana 스택: [observability/docker-compose.yml](observability/docker-compose.yml)
- CPU/메모리 메트릭은 Prometheus 기본 Go/process collector로 함께 노출됩니다.
  - CPU: `process_cpu_seconds_total`
  - RSS 메모리: `process_resident_memory_bytes`
  - Go heap: `go_memstats_heap_alloc_bytes`

```bash
DBSP_METRICS_ADDR=:9090 DBSP_PPROF_ADDR=:6060 go run ./cmd/dbsp -config ./your-config.yaml

curl http://127.0.0.1:9090/metrics
curl http://127.0.0.1:6060/debug/pprof/
```

macOS에서 Prometheus/Grafana를 같이 띄우려면:

```bash
# 1) DBSP 프로세스에서 metrics 노출
DBSP_METRICS_ADDR=:9090 DBSP_PPROF_ADDR=:6060 /tmp/dbsp-go -config config.user.partition.request.yaml

# 2) 별도 터미널에서 observability 스택 실행
docker compose -f observability/docker-compose.yml up -d
```

- Prometheus UI: `http://127.0.0.1:9091`
- Grafana UI: `http://127.0.0.1:3000` (`admin` / `admin`)
- Grafana에는 `DBSP Overview` 대시보드가 자동으로 provision 됩니다.
- 대시보드에는 pipeline/operator 메트릭 외에 `Process CPU Usage`, `Process and Heap Memory` 패널도 포함됩니다.
- 기본 scrape target은 `host.docker.internal:9090` 입니다. Linux에서는 [observability/prometheus.yml](observability/prometheus.yml)의 target을 호스트 IP 또는 `host-gateway`에 맞게 바꿔야 합니다.

## 컨테이너 이미지 (GHCR)

GitHub Actions 워크플로우로 `ghcr.io`에 이미지를 빌드/푸시합니다.

- 워크플로우 파일: [.github/workflows/ghcr-build.yml](.github/workflows/ghcr-build.yml)
- 기본 이미지 경로: `ghcr.io/ariyn/dbsp-go`
- 기본 브랜치(`main`) 푸시 시 `latest` 태그가 생성됩니다.

```bash
# 이미지 가져오기
docker pull ghcr.io/ariyn/dbsp-go:latest

# 설정 파일을 마운트해서 실행
docker run --rm \
  -v "$PWD/examples/config.yaml:/app/config.yaml:ro" \
  ghcr.io/ariyn/dbsp-go:latest \
  -config /app/config.yaml
```

## 구조

- `cmd/dbsp`: YAML 기반 파이프라인 CLI
- `internal/dbsp/sql`: SQL 파서 및 변환기
- `internal/dbsp/ir`: LogicalPlan 및 변환
- `internal/dbsp/diff`: DBSP 그래프 자동 미분(증분화)
- `internal/dbsp/op`: 실행기/연산자(Join/GroupAgg/Window/Delay/Integrate 등)
