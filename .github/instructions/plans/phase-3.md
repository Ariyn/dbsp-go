# Phase 3 — Relational IR → DBSP IR 변환기

## 목표

- Logical IR(Scan/Project/Filter/Join/GroupAgg)을 DBSP IR(Map/Binary/Integrate/Delay)로 변환한다.
- SQL 파서를 필수로 두지 않고도(수동 IR 구성 포함) 변환기가 안정적으로 동작한다.

## 산출물(완료 조건)

- 변환기 단위 테스트: LogicalPlan 입력 → DBSP 그래프 구조/연산자 타입이 기대와 일치
- 대표 SQL(또는 수동 LogicalPlan) 3개 이상에 대해 end-to-end 실행이 가능

## 다음에 할 일(요약)

- 변환 규칙 문서화(케이스별)
- 표현식/타입 힌트 처리 일관화
- planner/optimizer(선택) 포인트 정의
