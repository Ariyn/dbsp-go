# dbsp CLI (cmd/dbsp)

`cmd/dbsp`는 설정 파일(YAML)로 **Source → (SQL Transform) → Sink** 파이프라인을 실행합니다.

## 실행

```bash
# 바로 실행
go run ./cmd/dbsp -config examples/config.yaml

# 또는 바이너리로 빌드 후 실행
go build -o dbsp ./cmd/dbsp
./dbsp -config examples/config.yaml
```

CLI 플래그:

- `-config` : 설정 파일 경로 (기본값 `config.yaml`)

## 설정 파일 스키마

```yaml
pipeline:
  wal:
    enabled: false
    path: /tmp/dbsp-wal.db
    sqlite_pragmas:
      temp_store: MEMORY | FILE | DEFAULT
      cache_size: 0
      mmap_size: 0
      busy_timeout_ms: 0
      extra_pragmas: {}
  source:
    type: csv | http | chain
    config: {}
  transform:
    type: sql
    query: "SELECT ..."
  sink:
    type: console | file
    config: {}
  state_backend:
    enabled: false
    type: memory | kv | lsm
    path: /tmp/dbsp-state.db
    memory_limit: "1GiB"
    checkpoint_mode: full | incremental
    checkpoint_every_batches: 100
    max_incremental_mutation_bytes: 1048576
  partition:
    enabled: false
    keys: [plant_id, local_date]
```

- `wal.enabled`(선택, 기본 false): 입력 배치를 SQLite에 append-only로 기록(WAL)
- `wal.path`(선택): SQLite DB 파일 경로
- `wal.sqlite_pragmas`(선택): SQLite pragma 튜닝(메모리/동시성/IO 제어)
  - `temp_store`: MEMORY | FILE | DEFAULT
  - `cache_size`: 페이지 캐시 크기(0이면 기본값)
  - `mmap_size`: mmap 크기(바이트, 0이면 기본값)
  - `busy_timeout_ms`: busy timeout(ms)
  - `extra_pragmas`: 추가 pragma map
- `state_backend.max_incremental_mutation_bytes`(선택, 기본 1048576): incremental checkpoint에서 drain된 mutation payload가 임계치를 넘으면 자동 full checkpoint로 승격
- `state_backend.memory_limit`(선택, 기본 `1GiB`): Go GC soft limit. 비어있거나 0이면 기본값 적용
- `partition.enabled`(선택, 기본 false): 파티션 fan-out 실행 모드
- `partition.keys`(필수 when enabled): Hive 경로 키 순서 (예: `[plant_id, local_date]`)

### state_backend 튜닝 가이드

`state_backend.checkpoint_mode: incremental`를 사용할 때는 아래 두 값을 함께 조정하는 것을 권장합니다.

- `state_backend.checkpoint_every_batches`
- `state_backend.max_incremental_mutation_bytes`

권장 시작값:

| 워크로드 성격 | checkpoint_every_batches | max_incremental_mutation_bytes | 의도 |
| --- | ---: | ---: | --- |
| 저변화율(작은 업데이트 다수) | 50~200 | 524288~1048576 | 증분 체크포인트 위주로 I/O 절감 |
| 중간 변화율(일반 OLAP ingest) | 20~100 | 1048576~4194304 | 복구 시간/체크포인트 비용 균형 |
| 고변화율(대량 upsert/delete burst) | 5~50 | 262144~1048576 | chain 장기화 방지, full 승격 빠르게 |

튜닝 순서:

1. 기본값(`checkpoint_every_batches=100`, `max_incremental_mutation_bytes=1048576`)으로 시작
2. 복구 시간이 길면 `checkpoint_every_batches`를 줄임
3. 체크포인트 I/O가 과하면 `checkpoint_every_batches`를 늘리되,
   mutation burst가 크면 `max_incremental_mutation_bytes`를 낮춰 자동 full 승격을 빠르게 유도
4. partition fan-out 환경에서는 파티션별 mutation 크기 편차가 크므로,
   먼저 hottest partition 기준으로 값을 맞춘 뒤 전체를 확장

주의: WAL replay는 **엔진 state 복구 목적**이며, 기본 구현은 replay 구간의 결과를 sink로 재출력하지 않습니다(중복 방지).
추가로, 이 정책에서는 프로세스가 “WAL에는 기록했지만 sink에 쓰기 전에” 크래시되면 해당 구간의 출력이 유실될 수 있습니다(재시작 시 replay가 출력 복구를 하지 않기 때문).

## Source 타입

### 1) CSV Source (`type: csv`)

```yaml
pipeline:
  source:
    type: csv
    config:
      path: examples/data.csv
      schema:
        time_bucket: string
        amount: float
        product: string
```

- `path`(필수): CSV 파일 경로
- `schema`(권장): 컬럼 타입 힌트 (`int` | `float` | `string`)
- 동작: 현재 구현은 CSV 전체를 한 번에 읽어 **단일 배치**로 방출합니다. 각 row는 `Count=+1`(insert)로 처리합니다.

### 2) HTTP Source (`type: http`)

```yaml
pipeline:
  source:
    type: http
    config:
      port: 8080
      path: /ingest
      schema:
        time_bucket: string
        amount: float
        product: string
      buffer_size: 1000
      max_batch_size: 100
      max_batch_delay_ms: 200
      max_request_bytes: 1048576
      max_buffer_bytes: 10485760
```

- `port`(선택, 기본 8080)
- `path`(선택, 기본 `/ingest`)
- `schema`(선택): JSON 필드 타입 변환 힌트
- `buffer_size`(선택, 기본 1000): 내부 대기 배치 수(배치 단위 버퍼링)
- `max_batch_size`(선택, 기본 100): 한 번에 반환할 최대 레코드 수
- `max_batch_delay_ms`(선택, 기본 0): 첫 레코드 수신 후 배치를 더 모을 최대 대기(ms)
- `max_request_bytes`(선택, 기본 0): HTTP 요청 바디 최대 크기(바이트). 0은 제한 없음
- `max_buffer_bytes`(선택, 기본 0): 내부 버퍼된 요청 바디 합산 크기 제한(바이트). 초과 시 429 반환

성능 팁: 가능하면 요청당 JSON array 크기를 키우고 `max_batch_size`를 넉넉히 잡으면
WAL/체크포인트 오버헤드가 줄어 ingest throughput이 개선됩니다.

HTTP ingest는 `POST` JSON을 받습니다(단일 object 또는 array 모두 가능).

```bash
curl -X POST 'http://localhost:8080/ingest' \
  -H 'Content-Type: application/json' \
  -d '[{"time_bucket":"2025-12-25T10:00:00Z","amount":10.5,"product":"A"}]'
```

### 3) Chain Source (`type: chain`)

여러 Source를 순서대로 실행합니다(앞 소스가 끝나면 다음 소스로 넘어감).

```yaml
pipeline:
  source:
    type: chain
    config:
      on_error: stop # stop(기본) | skip
      sources:
        - type: csv
          config:
            path: examples/data.csv
            schema:
              time_bucket: string
              amount: float
              product: string
        - type: http
          config:
            port: 8080
            path: /ingest
            schema:
              time_bucket: string
              amount: float
              product: string
```

- `on_error`: 소스 에러 처리 정책
  - `stop`(기본): 에러 즉시 파이프라인 종료
  - `skip`: 해당 소스를 닫고 다음 소스로 계속

## Transform 타입

현재 `transform.type`은 `sql`만 지원합니다.

```yaml
pipeline:
  transform:
    type: sql
    ttl: "24h"
    query: "SELECT time_bucket, SUM(amount) AS total_sales FROM sales GROUP BY time_bucket"
```

- `ttl`(선택): WAL 보존 기간(예: `"24h"`, `"10s"`, `"5 minutes"`).
  - 현재 구현에서 `ttl`은 `pipeline.wal.enabled=true`일 때 WAL 배치/체크포인트 정리에 적용됩니다.
  - DBSP 의미론 보존을 위해 논리 상태의 처리시간 만료에는 적용하지 않습니다.

## Partition fan-out (Hive-style)

`partition.enabled: true`면 **`transform.query`는 공통으로 1개만 사용**하고,
런타임에서 입력 배치를 `partition.keys` 기준으로 분할(demux)해 파티션별로 독립 state로 계산합니다.

```yaml
pipeline:
  transform:
    type: sql
    query: "SELECT panel_position, SUM(v_out*i_out) AS p FROM telemetry GROUP BY panel_position"
  partition:
    enabled: true
    keys: [plant_id, local_date]
```

동작:

- `transform.query`는 파티션 조건 없이 공통 집계/변환 로직만 작성합니다.
- 시작 시점에 `transform.query`를 1회 컴파일 preflight 하며, 실패하면 ingest 시작 전 즉시 종료됩니다.
- 레코드는 `partition.keys` 값으로 분할되어 해당 파티션 런타임에만 반영됩니다.
- `file`/`parquet` sink의 `path`, WAL `path`, `http_pull.disk_spill_path`에 Hive 경로를 추가합니다.
  - 예: `/tmp/out.parquet` + `plant_id=P-1, local_date=2026-02-24`
  - 결과: `/tmp/plant_id=P-1/local_date=2026-02-24/out.parquet`

## Sink 타입

### 1) Console Sink (`type: console`)

```yaml
pipeline:
  sink:
    type: console
    config:
      format: json # json(기본) | text
```

### 2) File Sink (`type: file`)

```yaml
pipeline:
  sink:
    type: file
    config:
      path: /tmp/out.jsonl
      format: json # json(기본, JSON Lines) | csv
```

- `format: json`은 TupleDelta를 **한 줄당 1개(JSON Lines)**로 append
- `format: csv`는 헤더를 자동 생성하고 마지막 컬럼에 `__count`를 씁니다

### 3) Parquet Sink (`type: parquet`)

Arrow 기반 Parquet 파일로 출력합니다. 기본적으로 배치/시간 기준 파일 회전을 지원합니다.

```yaml
pipeline:
  sink:
    type: parquet
    config:
      path: /tmp/dbsp-out # prefix (파일은 /tmp/dbsp-out-<ts>-<seq>.parquet)
      schema_cache_path: /tmp/dbsp-out.schema.json
      compression: zstd # zstd(기본) | snappy | gzip | uncompressed
      row_group_size: 65536
      rotate_every_batches: 1000
      rotate_every: "30s"
      batch:
        max_batch_size: 100
        max_batch_delay_ms: 200
```

- `schema_cache_path`에 SQL 분석 결과로 추론한 출력 스키마를 저장하고, 이후 실행에서 재사용합니다.
- 출력은 기본적으로 TupleDelta의 `Count`를 `__count` 컬럼에 저장합니다.

### (선택) Sink 배치 래핑

Sink의 `config.batch`를 설정하면 출력 배치를 모아서 flush 합니다.

```yaml
pipeline:
  sink:
    type: console
    config:
      format: json
      batch:
        max_batch_size: 100
        max_batch_delay_ms: 200
```

## 예제 설정

- `examples/config.yaml`: `chain`(CSV → HTTP) + `console` sink
- `examples/config_http.yaml`: HTTP source + console sink
- `examples/config_partition.yaml`: `partition.enabled=true` + Hive-style path fan-out 예제
