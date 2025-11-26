# LAG 함수의 모노이드 분석

## LAG 동작 예시

```
LAG(value, 1) PARTITION BY key ORDER BY ts

초기 상태 (key='A'):
ts | value | LAG(value, 1)
---|-------|---------------
 1 |  10   | NULL
 2 |  20   | 10
 3 |  30   | 20
```

## 시나리오 1: 중간 행 삽입

```
새 행 삽입: (ts=1.5, value=15)

ts | value | LAG(value, 1)
---|-------|---------------
 1 |  10   | NULL
1.5|  15   | 10           ← 새 행
 2 |  20   | 15           ← 변경됨 (10→15)
 3 |  30   | 20           ← 변경 없음
```

**델타 출력:**

- (+) ts=1.5: LAG=10 (새 행)
- (-) ts=2: LAG=10 (이전 값 취소)
- (+) ts=2: LAG=15 (새 값)

**영향 범위:** 삽입 위치부터 끝까지

## 시나리오 2: 최소값 삭제

```
삭제: (ts=1, value=10)

ts | value | LAG(value, 1)
---|-------|---------------
1.5|  15   | NULL         ← 변경됨 (10→NULL)
 2 |  20   | 15           ← 변경 없음
 3 |  30   | 20           ← 변경 없음
```

**델타 출력:**

- (-) ts=1.5: LAG=10 (이전 값 취소)
- (+) ts=1.5: LAG=NULL (새 값)

**영향 범위:** 삭제 위치 직후 1개 행만

## 시나리오 3: 중간값 삭제

```
삭제: (ts=2, value=20)

ts | value | LAG(value, 1)
---|-------|---------------
 1 |  10   | NULL         ← 변경 없음
1.5|  15   | 10           ← 변경 없음
 3 |  30   | 15           ← 변경됨 (20→15)
```

**델타 출력:**

- (-) ts=3: LAG=20 (이전 값 취소)
- (+) ts=3: LAG=15 (새 값)

**영향 범위:** 삭제 위치 직후 1개 행만

## 모노이드 검증

### 1. 항등원(Identity) 테스트

```
빈 파티션 ⊕ 행 삽입 = 행 삽입
```

✅ **통과**: 빈 상태에서 시작 가능

### 2. 결합법칙(Associativity) 테스트

```
행1, 행2, 행3을 순차 삽입:
(행1 ⊕ 행2) ⊕ 행3 = 행1 ⊕ (행2 ⊕ 행3)
```

❌ **실패**: LAG는 **순서에 의존**함

- 행1→행2 삽입 시: 행2의 LAG = 행1.value
- 행2→행1 삽입 시: 행1의 LAG = NULL, 행2의 LAG = 행1.value
- **결과가 다름!**

### 3. 교환법칙(Commutativity) 테스트

```
행1 ⊕ 행2 = 행2 ⊕ 행1
```

❌ **실패**: LAG는 **순서에 민감**함

## 결론

### LAG는 전통적 모노이드가 아닙니다

**이유:**

1. **비결합적(Non-associative)**: 삽입 순서에 따라 결과가 달라짐
2. **비교환적(Non-commutative)**: 순서가 중요함
3. **상태 의존적**: 현재 행의 LAG 값은 전체 정렬 순서에 의존

### 그러나 DBSP 논문의 "상태 전이 모노이드"는 가능

**OrderedBuffer 모노이드:**

```go
type OrderedBuffer struct {
    entries []Entry // ORDER BY로 정렬된 행 목록
}

// 모노이드 연산: 두 버퍼 병합 후 재정렬
func (o OrderedBuffer) Combine(other OrderedBuffer) OrderedBuffer {
    merged := append(o.entries, other.entries...)
    sort.Stable(merged) // ORDER BY 기준 정렬
    return OrderedBuffer{entries: merged}
}
```

**핵심:**

- 모노이드 연산은 **버퍼 병합**
- LAG 값 계산은 **별도 단계** (버퍼→LAG 값)
- 버퍼 자체는 결합법칙 만족

## 구현 전략

### 방안 1: OrderedBuffer + 재계산

```go
type LagMonoid struct {
    buffer OrderedBuffer  // 정렬된 행 목록
}

func (l *LagMonoid) Apply(td TupleDelta) []TupleDelta {
    // 1. 버퍼 업데이트
    l.buffer.Add(td.Tuple, td.Count)
    
    // 2. 영향받는 행들의 LAG 재계산
    affectedRows := l.buffer.GetAffectedRows(td.Tuple)
    
    // 3. 변경된 LAG 값만 델타로 출력
    return computeLagDeltas(affectedRows)
}
```

**장점:**

- 정확한 결과
- 모든 삽입/삭제 처리 가능

**단점:**

- 최악의 경우 O(n) 재계산 (n = 파티션 크기)

### 방안 2: Append-only 최적화

```go
// ORDER BY가 단조 증가이고, 삽입만 허용
type AppendOnlyLagMonoid struct {
    lastRow Tuple  // 마지막 행만 유지
}

func (a *AppendOnlyLagMonoid) Apply(td TupleDelta) *TupleDelta {
    if td.Count < 0 {
        return nil, errors.New("delete not supported")
    }
    
    // 현재 행의 LAG = 이전 행
    lag := a.lastRow
    a.lastRow = td.Tuple
    
    return &TupleDelta{
        Tuple: mergeWithLag(td.Tuple, lag),
        Count: 1,
    }
}
```

**장점:**

- O(1) 처리
- 메모리 효율적

**단점:**

- 삭제 불가
- 순서 위반 시 실패

## 권장사항

**Phase 1**: Append-only LAG 구현

- 시계열 데이터에 유용
- 성능 최적화
- 제한사항 명시

**Phase 2**: Full LAG with OrderedBuffer

- 모든 케이스 지원
- 성능 트레이드오프 수용

**Phase 3**: 최적화

- Incremental 재계산 범위 최소화
- 캐싱 전략
