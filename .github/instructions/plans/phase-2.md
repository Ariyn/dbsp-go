# Phase 2 — Join + GroupAggregate (현재 단계)

## 목표

- 2-way JOIN 결과를 GROUP BY로 집계하고, 입력 ΔBatch에 대해 **증분(ΔView)** 를 안정적으로 출력한다.
- "집계 결과만 동일" 기준으로 E2E 검증을 갖춘다(파일 sink 포함).

## 비목표(현재 단계에서 하지 않음)

- 재귀/비단조 쿼리
- N-way join, 복잡한 subquery 최적화
- 완전한 SQL 표준 타입 시스템

## 산출물(완료 조건)

- JOIN + GROUP BY 쿼리에 대해:
  - delete/retraction 포함 테스트가 통과한다.
  - pipeline E2E에서 출력 델타를 누적(integrate)했을 때 최종 집계 스냅샷이 기대값과 일치한다.
  - (선택) Parquet sink로 저장한 뒤 재로딩하여 동일성을 검증한다.

## 권장 진행 순서(작업 단위)

1) **스펙 고정(쿼리 범위/출력 스키마)**
- 우선 지원할 대표 쿼리 2개를 고정한다.
  - Q1: `SELECT k, SUM(v) FROM t GROUP BY k`
  - Q2: `SELECT k, COUNT(*) FROM t GROUP BY k`
- 출력 델타 컬럼 정책을 고정한다.
  - `SUM` → `agg_delta`
  - `COUNT` → `count_delta`
  - 공통: `__count`(TupleDelta.Count)

2) **E2E 검증(정확성) 먼저 추가**
- 파이프라인 실행 → sink 출력(배치/파일)
- sink 출력 델타를 키별로 누적해서 최종 결과 스냅샷을 복원한다.
- 동일 입력을 전체 스캔해 기대 스냅샷을 계산하고 비교한다.

3) **JOIN + GROUP BY 조합 케이스 확장**
- JOIN key 1개/2개
- GROUP BY key 1개/2개
- delete 포함(삽입/삭제 상쇄, 순서 변경)

4) **운영 관점 보강(선택)**
- Parquet sink:
  - rotation(시간/배치) + schema cache 재사용을 E2E에서 검증
- WAL/스냅샷:
  - 체크포인트 후 재시작 replay가 동일 결과를 만드는지 E2E 1개 추가

## 구현 팁(테스트 관점)

- E2E 검증에서는 "델타"를 비교하지 말고, 델타를 누적해 만든 최종 스냅샷을 비교한다.
- 비교 시에는 `count==0` 튜플 제거, key별 정규화(정렬/맵)로 안정적인 assert를 만든다.
