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
  source:
    type: csv | http | chain
    config: {}
  transform:
    type: sql
    query: "SELECT ..."
  sink:
    type: console | file
    config: {}
```

- `wal.enabled`(선택, 기본 false): 입력 배치를 SQLite에 append-only로 기록(WAL)
- `wal.path`(선택): SQLite DB 파일 경로

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
```

- `port`(선택, 기본 8080)
- `path`(선택, 기본 `/ingest`)
- `schema`(선택): JSON 필드 타입 변환 힌트
- `buffer_size`(선택, 기본 1000): 내부 버퍼 크기
- `max_batch_size`(선택, 기본 100): 한 번에 반환할 최대 레코드 수
- `max_batch_delay_ms`(선택, 기본 0): 첫 레코드 수신 후 배치를 더 모을 최대 대기(ms)

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
    join_ttl: "10 minutes"
    query: "SELECT time_bucket, SUM(amount) AS total_sales FROM sales GROUP BY time_bucket"
```

- `join_ttl`(선택): 조인 상태를 처리시간 기준으로 만료시키는 TTL(예: `"10s"`, `"5 minutes"`).
  - `0` 또는 미설정이면 TTL을 적용하지 않습니다.

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
