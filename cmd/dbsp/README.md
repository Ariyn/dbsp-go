# dbsp CLI (minimal runtime)

`cmd/dbsp`는 YAML 설정으로 **HTTP Source -> SQL Transform -> HTTP Pull Sink** 파이프라인만 실행합니다.

## 실행

```bash
go run ./cmd/dbsp -config config.yaml
```

## 지원 설정 (최소 계약)

```yaml
pipeline:
  source:
    type: http
    config:
      port: 8080
      path: /ingest
      schema:
        timestamp: int
        panel_position: string
        plant_id: string
        local_date: string
        v_out: float
        i_out: float

  transform:
    type: sql
    query: "SELECT * FROM telemetry"

  sink:
    type: http_pull
    config:
      port: 8081
      path: /pull
```

## 제거된 기능

- 다중 source/sink 선택(`chain`, `console`, `file`, `parquet`)
- 복구 경로(`wal`, `checkpoint`, `state_backend`)
- 시간 의미론(`transform.ttl`, `transform.watermark`)
- 파티션 fan-out 실행
- DML 입력(`INSERT`, `UPDATE`, `DELETE`) 배치 변환

## I/O API

- Ingest: `POST http://localhost:8080/ingest`
- Pull: `GET http://localhost:8081/pull`

`http_pull` 응답은 Parquet 바이너리(`PAR1`)입니다.
