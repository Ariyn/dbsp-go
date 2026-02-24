# SQL Window/Expression 확장 원칙 (DBSP 논문 범위 유지)

## 1. 개요

DBSP-Go의 모든 SQL 확장은 DBSP 이론의 핵심 연산자(Map, Binary, Delay, Integrate)로의 **하향 변환(Lowering)**을 원칙으로 한다. 이는 복잡한 SQL 구문이 추가되더라도 시스템의 정합성과 미분 가능성을 보장하기 위함이다.

## 2. 핵심 원칙

- **원칙 1 (연산자 제약)**: 모든 신규 기능은 Map, Binary, Delay, Integrate의 조합으로 구현된다. 엔진 수준의 신규 연산자 추가는 지양한다.
- **원칙 2 (상태 관리)**: LAG, Window Aggregate 등 상태가 필요한 연산은 DBSP의 `Delay` 또는 `Integrate`를 포함한 하위 그래프로 변환되어야 한다. (현재 구현된 `LAG` 등은 내부적으로 이를 캡슐화한 `Map` 계열로 간주한다.)
- **원칙 3 (표현식 불변성)**: `time_bucket`, `epoch` 등 스칼라 함수는 순수 함수로서 `Map` 연산 내에서 처리된다.
- **원칙 4 (미분 정합성)**: 모든 확장은 $d(f(S)) = f(dS)$ (Map의 경우) 또는 $d(I(S)) = S$ (Integrate의 경우) 규칙을 준수해야 한다.

## 3. 기능별 매핑 전략

| SQL 기능 | DBSP 매핑 | 비고 |
| :--- | :--- | :--- |
| **LAG(...)** | `Map` (with internal buffer/delay) | 이전 값을 참조하는 지연 연산 |
| **Window Agg** | `Integrate` + `Binary` (Join) | 윈도우 경계 내 집계 누적 |
| **Scalar Fn** | `Map` | `time_bucket`, `epoch`, `strftime` 등 |
| **CTE** | 서브그래프 공유 | `LogicalWith`를 통한 구조적 재사용 |
| **ORDER BY** | `Sort` (Result materialization) | 최종 출력 계층의 연산 |

## 4. 데이터 타입 표준

- **Timestamp**: 모든 시간 계산은 `int64` (ms since epoch)를 기준으로 수행한다.
- **Interval**: 내부적으로 `ms` 단위의 정수로 정규화하여 산술 연산에 사용한다.
