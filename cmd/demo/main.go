package main

import (
	"fmt"

	"github.com/ariyn/dbsp/internal/dbsp/op"
	sqlconv "github.com/ariyn/dbsp/internal/dbsp/sql"
	"github.com/ariyn/dbsp/internal/dbsp/state"
	"github.com/ariyn/dbsp/internal/dbsp/types"
)

func main() {
	fmt.Println("╔═══════════════════════════════════════════════════════════╗")
	fmt.Println("║  DBSP IVM Demo: 5분 단위 매출 집계                       ║")
	fmt.Println("╚═══════════════════════════════════════════════════════════╝")
	fmt.Println()

	// Run window demos first
	runWindowDemos()
	
	fmt.Println("\n\n╔═══════════════════════════════════════════════════════════╗")
	fmt.Println("║  Original Demo: 5분 단위 매출 집계                       ║")
	fmt.Println("╚═══════════════════════════════════════════════════════════╝")
	fmt.Println()

	// SQL 쿼리: 5분 단위로 그룹화하여 매출 합계 계산
	query := `
		SELECT time_bucket, SUM(amount) as total_sales
		FROM sales
		GROUP BY time_bucket
	`

	fmt.Println("📝 Query:")
	fmt.Println("   ", query)
	fmt.Println()

	// SQL을 증분 DBSP 그래프로 변환
	incNode, err := sqlconv.ParseQueryToIncrementalDBSP(query)
	if err != nil {
		fmt.Printf("❌ 파싱 에러: %v\n", err)
		return
	}

	fmt.Println("✅ SQL 쿼리를 증분 DBSP 그래프로 변환 완료")
	fmt.Println()

	// 시뮬레이션: 실시간으로 들어오는 매출 데이터
	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	fmt.Println("📊 시나리오 1: 초기 데이터 투입 (10:00-10:04)")
	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

	insertSQL1 := `
		INSERT INTO sales (time_bucket, amount, product) VALUES 
		('10:00', 1000, 'A'),
		('10:00', 1500, 'B'),
		('10:00', 2000, 'C')
	`

	fmt.Println("\n입력 SQL:")
	fmt.Println(insertSQL1)

	batch1, err := sqlconv.ParseDMLToBatch(insertSQL1)
	if err != nil {
		fmt.Printf("❌ SQL 파싱 에러: %v\n", err)
		return
	}

	fmt.Println("\n파싱된 데이터:")
	for _, td := range batch1 {
		fmt.Printf("   • 시간대: %s, 금액: %v원, 상품: %s\n",
			td.Tuple["time_bucket"], td.Tuple["amount"], td.Tuple["product"])
	}

	result1, err := op.Execute(incNode, batch1)
	if err != nil {
		fmt.Printf("❌ 실행 에러: %v\n", err)
		return
	}

	printResults("\n📈 10:00 시간대 집계 결과", result1, "10:00")
	fmt.Printf("   💰 총 매출: 4,500원 (1000 + 1500 + 2000)\n")

	// 추가 데이터 투입 (증분 업데이트)
	fmt.Println("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	fmt.Println("📊 시나리오 2: 추가 데이터 투입 (10:05-10:09)")
	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

	insertSQL2 := `
		INSERT INTO sales (time_bucket, amount, product) VALUES 
		('10:05', 3000, 'A'),
		('10:05', 2500, 'D'),
		('10:00', 500, 'E')
	`

	fmt.Println("\n입력 SQL:")
	fmt.Println(insertSQL2)

	batch2, err := sqlconv.ParseDMLToBatch(insertSQL2)
	if err != nil {
		fmt.Printf("❌ SQL 파싱 에러: %v\n", err)
		return
	}

	fmt.Println("\n파싱된 데이터:")
	for _, td := range batch2 {
		fmt.Printf("   • 시간대: %s, 금액: %v원, 상품: %s\n",
			td.Tuple["time_bucket"], td.Tuple["amount"], td.Tuple["product"])
	}

	result2, err := op.Execute(incNode, batch2)
	if err != nil {
		fmt.Printf("❌ 실행 에러: %v\n", err)
		return
	}

	printResults("\n📈 증분 업데이트 결과", result2, "")
	fmt.Printf("   💡 설명:\n")
	fmt.Printf("      - 10:05 시간대: 신규 5,500원 추가 (3000 + 2500)\n")
	fmt.Printf("      - 10:00 시간대: 기존 4,500원 → 5,000원 (500원 추가)\n")

	// 데이터 삭제 시뮬레이션 (환불 등)
	fmt.Println("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	fmt.Println("📊 시나리오 3: 데이터 삭제 (환불)")
	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

	fmt.Println("\n환불 처리 (수동 Batch - DELETE는 기존 튜플 정보 필요):")
	fmt.Printf("   • 시간대: 10:00, 금액: 1,000원 환불 (상품 A)\n")

	batch3 := types.Batch{
		{Tuple: types.Tuple{"time_bucket": "10:00", "amount": 1000, "product": "A"}, Count: -1}, // 환불
	}

	result3, err := op.Execute(incNode, batch3)
	if err != nil {
		fmt.Printf("❌ 실행 에러: %v\n", err)
		return
	}

	printResults("\n📉 환불 후 업데이트 결과", result3, "")
	fmt.Printf("   💡 설명: 10:00 시간대 5,000원 → 4,000원 (1,000원 차감)\n")

	// 복잡한 쿼리 예제: WHERE 절 포함
	fmt.Println("\n\n╔═══════════════════════════════════════════════════════════╗")
	fmt.Println("║  고급 예제: WHERE 절을 사용한 필터링                     ║")
	fmt.Println("╚═══════════════════════════════════════════════════════════╝")
	fmt.Println()

	query2 := `
		SELECT region, SUM(amount) as total
		FROM orders
		WHERE status = 'paid' AND amount > 100
		GROUP BY region
	`

	fmt.Println("📝 Query:")
	fmt.Println("   ", query2)
	fmt.Println()

	incNode2, err := sqlconv.ParseQueryToIncrementalDBSP(query2)
	if err != nil {
		fmt.Printf("❌ 파싱 에러: %v\n", err)
		return
	}

	insertOrders := `
		INSERT INTO orders (region, amount, status) VALUES 
		('Seoul', 150, 'paid'),
		('Seoul', 50, 'paid'),
		('Seoul', 200, 'pending'),
		('Busan', 300, 'paid'),
		('Seoul', 120, 'paid')
	`

	fmt.Println("입력 SQL:")
	fmt.Println(insertOrders)

	ordersBatch, err := sqlconv.ParseDMLToBatch(insertOrders)
	if err != nil {
		fmt.Printf("❌ SQL 파싱 에러: %v\n", err)
		return
	}

	fmt.Println("\n파싱된 데이터 (5건):")
	for i, td := range ordersBatch {
		status := "✅ 통과"
		amount := td.Tuple["amount"].(int64)
		if amount <= 100 {
			status = "❌ 필터됨 (amount <= 100)"
		} else if td.Tuple["status"] != "paid" {
			status = "❌ 필터됨 (status != paid)"
		}
		fmt.Printf("   %d. 지역: %s, 금액: %d원, 상태: %s → %s\n",
			i+1, td.Tuple["region"], amount, td.Tuple["status"], status)
	}

	ordersResult, err := op.Execute(incNode2, ordersBatch)
	if err != nil {
		fmt.Printf("❌ 실행 에러: %v\n", err)
		return
	}

	printResults("\n📈 지역별 매출 집계 (필터 적용 후)", ordersResult, "")
	fmt.Printf("   💡 설명: status='paid' AND amount>100 조건을 만족하는 3건만 집계\n")
	fmt.Printf("      - Seoul: 270원 (150 + 120)\n")
	fmt.Printf("      - Busan: 300원\n")

	// Projection 예제
	fmt.Println("\n\n╔═══════════════════════════════════════════════════════════╗")
	fmt.Println("║  Projection 예제: 특정 컬럼만 선택                       ║")
	fmt.Println("╚═══════════════════════════════════════════════════════════╝")
	fmt.Println()

	query3 := `
		SELECT product, amount
		FROM sales
		WHERE amount > 1000
	`

	fmt.Println("📝 Query:")
	fmt.Println("   ", query3)
	fmt.Println()

	incNode3, err := sqlconv.ParseQueryToIncrementalDBSP(query3)
	if err != nil {
		fmt.Printf("❌ 파싱 에러: %v\n", err)
		return
	}

	insertSales := `
		INSERT INTO sales (product, amount, category, store) VALUES 
		('Laptop', 1500, 'Electronics', 'A'),
		('Mouse', 50, 'Electronics', 'B'),
		('Monitor', 2000, 'Electronics', 'A')
	`

	fmt.Println("입력 SQL:")
	fmt.Println(insertSales)

	salesBatch, err := sqlconv.ParseDMLToBatch(insertSales)
	if err != nil {
		fmt.Printf("❌ SQL 파싱 에러: %v\n", err)
		return
	}

	fmt.Println("\n파싱된 데이터 (4개 컬럼):")
	for _, td := range salesBatch {
		status := "✅ 통과"
		amount := td.Tuple["amount"].(int64)
		if amount <= 1000 {
			status = "❌ 필터됨 (amount <= 1000)"
		}
		fmt.Printf("   • 상품: %s, 금액: %d원, 카테고리: %s, 매장: %s → %s\n",
			td.Tuple["product"], amount, td.Tuple["category"], td.Tuple["store"], status)
	}

	salesResult, err := op.Execute(incNode3, salesBatch)
	if err != nil {
		fmt.Printf("❌ 실행 에러: %v\n", err)
		return
	}

	printResults("\n📈 고액 상품 조회 결과 (product, amount만 선택)", salesResult, "")
	fmt.Printf("   💡 설명: category, store 컬럼은 제거되고 product, amount만 반환\n")

	// TUMBLE 윈도우 예제
	fmt.Println("\n╔═══════════════════════════════════════════════════════════╗")
	fmt.Println("║  Tumbling Window 예제: ts 기반 5분 윈도우                ║")
	fmt.Println("╚═══════════════════════════════════════════════════════════╝")
	fmt.Println()

	runTumbleWindowDemo()

	// DELETE / UPDATE 데모
	fmt.Println("\n╔═══════════════════════════════════════════════════════════╗")
	fmt.Println("║  DELETE / UPDATE 예제: State Store 활용                  ║")
	fmt.Println("╚═══════════════════════════════════════════════════════════╝")
	fmt.Println()

	runDeleteUpdateDemo()

	// LAG(PARTITION BY ...) 윈도우 함수 데모
	fmt.Println("\n╔═══════════════════════════════════════════════════════════╗")
	fmt.Println("║  LAG(Window) 예제: PARTITION BY + ORDER BY               ║")
	fmt.Println("╚═══════════════════════════════════════════════════════════╝")
	fmt.Println()

	runLagPartitionByDemo()

	fmt.Println("\n╔═══════════════════════════════════════════════════════════╗")
	fmt.Println("║  Demo 완료                                                ║")
	fmt.Println("╚═══════════════════════════════════════════════════════════╝")
}

// LAG(window) + PARTITION BY 데모
//
//	SELECT id, ts, value,
//	       LAG(value) OVER (PARTITION BY id ORDER BY ts) AS prev_value
//	FROM metrics
func runLagPartitionByDemo() {
	query := `
		SELECT LAG(value) OVER (PARTITION BY id ORDER BY ts) AS prev_value
		FROM metrics
	`

	fmt.Println("📝 Query:")
	fmt.Println("   ", query)
	fmt.Println()

	node, err := sqlconv.ParseQueryToIncrementalDBSP(query)
	if err != nil {
		fmt.Printf("❌ 파싱 에러: %v\n", err)
		return
	}

	fmt.Println("✅ LAG(Window) 쿼리를 증분 DBSP 그래프로 변환 완료")

	batch := types.Batch{
		// id=A 파티션
		{Tuple: types.Tuple{"id": "A", "ts": int64(1), "value": 10}, Count: 1},
		{Tuple: types.Tuple{"id": "A", "ts": int64(2), "value": 20}, Count: 1},
		{Tuple: types.Tuple{"id": "A", "ts": int64(3), "value": 30}, Count: 1},
		// id=B 파티션
		{Tuple: types.Tuple{"id": "B", "ts": int64(1), "value": 100}, Count: 1},
		{Tuple: types.Tuple{"id": "B", "ts": int64(2), "value": 200}, Count: 1},
	}

	fmt.Println("\n입력 Batch:")
	for _, td := range batch {
		fmt.Printf("   • %v\n", formatTuple(td.Tuple))
	}

	out, err := op.Execute(node, batch)
	if err != nil {
		fmt.Printf("❌ 실행 에러: %v\n", err)
		return
	}

	if len(out) == 0 {
		fmt.Println("   (결과 없음)")
		return
	}

	fmt.Println("\n📈 LAG 결과 (prev_value):")
	for _, td := range out {
		if td.Count <= 0 {
			continue
		}
		fmt.Printf("   • %v\n", formatTuple(td.Tuple))
	}
}

func runTumbleWindowDemo() {
	// ts 컬럼을 millisecond 정수로 가정한 간단한 예제
	query := `
		SELECT TUMBLE(ts, INTERVAL '5' MINUTE), SUM(amount) as total
		FROM events
		GROUP BY TUMBLE(ts, INTERVAL '5' MINUTE)
	`

	fmt.Println("📝 Query:")
	fmt.Println("   ", query)
	fmt.Println()

	node, err := sqlconv.ParseQueryToIncrementalDBSP(query)
	if err != nil {
		fmt.Printf("❌ 파싱 에러: %v\n", err)
		return
	}

	fmt.Println("✅ TUMBLE 윈도우 쿼리를 증분 DBSP 그래프로 변환 완료")

	// ts: 0ms, 2분, 7분 → 0~5분, 0~5분, 5~10분 윈도우
	batch1 := types.Batch{
		{Tuple: types.Tuple{"ts": int64(0), "amount": 100}, Count: 1},             // 0~5분
		{Tuple: types.Tuple{"ts": int64(2 * 60 * 1000), "amount": 50}, Count: 1},  // 0~5분
		{Tuple: types.Tuple{"ts": int64(7 * 60 * 1000), "amount": 200}, Count: 1}, // 5~10분
	}

	fmt.Println("\n입력 Batch (ts: millis):")
	for _, td := range batch1 {
		fmt.Printf("   • ts=%dms, amount=%v\n", td.Tuple["ts"], td.Tuple["amount"])
	}

	out, err := op.Execute(node, batch1)
	if err != nil {
		fmt.Printf("❌ 실행 에러: %v\n", err)
		return
	}

	printResults("\n📈 TUMBLE 윈도우 집계 결과", out, "")
	fmt.Printf("   💡 설명: 0~5분 윈도우 합계=150, 5~10분 윈도우 합계=200\n")
}

func runDeleteUpdateDemo() {
	store := state.NewStore()

	// 주문 집계 쿼리
	query := `
		SELECT status, SUM(amount) as total
		FROM orders
		GROUP BY status
	`

	fmt.Println("📝 Query:")
	fmt.Println("   ", query)
	fmt.Println()

	incNode, err := sqlconv.ParseQueryToIncrementalDBSP(query)
	if err != nil {
		fmt.Printf("❌ 파싱 에러: %v\n", err)
		return
	}

	// 초기 데이터 INSERT
	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	fmt.Println("📊 Step 1: 초기 주문 데이터")
	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

	insertSQL := `
		INSERT INTO orders (order_id, status, amount) VALUES 
		(1, 'pending', 100),
		(2, 'pending', 200),
		(3, 'completed', 300)
	`

	fmt.Println("\n입력 SQL:")
	fmt.Println(insertSQL)

	insertBatch, err := sqlconv.ParseDMLToBatch(insertSQL)
	if err != nil {
		fmt.Printf("❌ SQL 파싱 에러: %v\n", err)
		return
	}

	// State 업데이트
	store.ApplyBatch("orders", insertBatch)

	result1, err := op.Execute(incNode, insertBatch)
	if err != nil {
		fmt.Printf("❌ 실행 에러: %v\n", err)
		return
	}

	printResults("\n📈 초기 집계 결과", result1, "")
	fmt.Printf("   💡 pending: 300원, completed: 300원\n")

	// UPDATE 시나리오
	fmt.Println("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	fmt.Println("📊 Step 2: 주문 상태 변경 (UPDATE)")
	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

	updateSQL := `UPDATE orders SET status = 'completed' WHERE order_id = 1`

	fmt.Println("\n입력 SQL:")
	fmt.Println("   ", updateSQL)
	fmt.Println("\n   💡 order_id=1 (pending 100원) → completed로 변경")

	updateBatch, err := sqlconv.ParseDMLToBatchWithStore(updateSQL, store)
	if err != nil {
		fmt.Printf("❌ SQL 파싱 에러: %v\n", err)
		return
	}

	// State 업데이트
	store.ApplyBatch("orders", updateBatch)

	result2, err := op.Execute(incNode, updateBatch)
	if err != nil {
		fmt.Printf("❌ 실행 에러: %v\n", err)
		return
	}

	printResults("\n📈 UPDATE 후 델타", result2, "")
	fmt.Printf("   💡 pending: -100원, completed: +100원\n")

	// DELETE 시나리오
	fmt.Println("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	fmt.Println("📊 Step 3: 주문 취소 (DELETE)")
	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

	deleteSQL := `DELETE FROM orders WHERE status = 'pending'`

	fmt.Println("\n입력 SQL:")
	fmt.Println("   ", deleteSQL)
	fmt.Println("\n   💡 pending 주문 모두 취소 (order_id=2, 200원)")

	deleteBatch, err := sqlconv.ParseDMLToBatchWithStore(deleteSQL, store)
	if err != nil {
		fmt.Printf("❌ SQL 파싱 에러: %v\n", err)
		return
	}

	// State 업데이트
	store.ApplyBatch("orders", deleteBatch)

	result3, err := op.Execute(incNode, deleteBatch)
	if err != nil {
		fmt.Printf("❌ 실행 에러: %v\n", err)
		return
	}

	printResults("\n📈 DELETE 후 델타", result3, "")
	fmt.Printf("   💡 pending: -200원 (취소됨)\n")

	// 최종 상태 확인
	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	fmt.Println("📊 최종 State Store 상태")
	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

	table := store.GetTable("orders")
	allOrders := table.GetAll()

	fmt.Printf("\n남은 주문: %d개\n", len(allOrders))
	for _, order := range allOrders {
		fmt.Printf("   • order_id: %v, status: %v, amount: %v원\n",
			order["order_id"], order["status"], order["amount"])
	}

	fmt.Printf("\n   💡 최종 집계: completed 400원 (order_id 1, 3)\n")
}

func printResults(title string, batch types.Batch, timeBucket string) {
	fmt.Printf("%s\n", title)

	if len(batch) == 0 {
		fmt.Println("   (결과 없음)")
		return
	}

	for _, td := range batch {
		delta := td.Tuple["agg_delta"]
		if delta != nil {
			if td.Count > 0 {
				fmt.Printf("   ▲ 변화량: +%v\n", delta)
			} else if td.Count < 0 {
				fmt.Printf("   ▼ 변화량: %v\n", delta)
			}
		} else {
			// Projection 결과 (집계 없음)
			if td.Count > 0 {
				fmt.Printf("   • %v\n", formatTuple(td.Tuple))
			}
		}
	}
}

func formatTuple(t types.Tuple) string {
	result := "{"
	first := true
	for k, v := range t {
		if !first {
			result += ", "
		}
		result += fmt.Sprintf("%s: %v", k, v)
		first = false
	}
	result += "}"
	return result
}
