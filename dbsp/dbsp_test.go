package dbsp

import "testing"

// 간단한 end-to-end 테스트: GROUP BY SUM 쿼리에 INSERT DML 적용.
func TestEngine_PrepareAndApplyDML(t *testing.T) {
	engine := NewEngine()

	query := `
		SELECT region, SUM(amount) AS total
		FROM orders
		GROUP BY region
	`

	h, err := engine.Prepare(query)
	if err != nil {
		t.Fatalf("Prepare failed: %v", err)
	}

	dml := `
		INSERT INTO orders (region, amount) VALUES
		('Seoul', 100),
		('Busan', 200)
	`

	res, err := engine.ApplyDML("orders", dml, h)
	if err != nil {
		t.Fatalf("ApplyDML failed: %v", err)
	}
	if len(res) == 0 {
		t.Fatalf("expected non-empty result")
	}
}
