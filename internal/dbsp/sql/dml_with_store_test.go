package sqlconv

import (
	"testing"

	"github.com/ariyn/dbsp/internal/dbsp/state"
	"github.com/ariyn/dbsp/internal/dbsp/types"
)

func TestParseDMLToBatchWithStore_Delete(t *testing.T) {
	store := state.NewStore()

	// 초기 데이터 INSERT
	insertSQL := `INSERT INTO orders (order_id, status, amount) VALUES (1, 'pending', 100), (2, 'completed', 200), (3, 'pending', 150)`
	insertBatch, err := ParseDMLToBatch(insertSQL)
	if err != nil {
		t.Fatalf("Failed to parse INSERT: %v", err)
	}

	// State에 INSERT 반영
	table := store.GetTable("orders")
	table.ApplyBatch(insertBatch)

	// DELETE 실행
	deleteSQL := `DELETE FROM orders WHERE status = 'pending'`
	deleteBatch, err := ParseDMLToBatchWithStore(deleteSQL, store)
	if err != nil {
		t.Fatalf("Failed to parse DELETE: %v", err)
	}

	// 검증: 2개의 튜플이 삭제되어야 함 (order_id 1, 3)
	if len(deleteBatch) != 2 {
		t.Fatalf("Expected 2 deletes, got %d", len(deleteBatch))
	}

	// 모든 튜플이 Count: -1이어야 함
	for _, td := range deleteBatch {
		if td.Count != -1 {
			t.Errorf("Expected Count: -1, got %d for tuple %v", td.Count, td.Tuple)
		}
		if td.Tuple["status"] != "pending" {
			t.Errorf("Expected status='pending', got %v", td.Tuple["status"])
		}
	}

	// State에 DELETE 반영
	table.ApplyBatch(deleteBatch)

	// 남은 튜플 확인 (order_id=2만 남아야 함)
	remaining := table.GetAll()
	if len(remaining) != 1 {
		t.Fatalf("Expected 1 remaining tuple, got %d", len(remaining))
	}
	if remaining[0]["order_id"] != int64(2) {
		t.Errorf("Expected order_id=2, got %v", remaining[0]["order_id"])
	}
}

func TestParseDMLToBatchWithStore_Update(t *testing.T) {
	store := state.NewStore()

	// 초기 데이터 INSERT
	insertSQL := `INSERT INTO orders (order_id, status, amount) VALUES (1, 'pending', 100), (2, 'completed', 200)`
	insertBatch, err := ParseDMLToBatch(insertSQL)
	if err != nil {
		t.Fatalf("Failed to parse INSERT: %v", err)
	}

	table := store.GetTable("orders")
	table.ApplyBatch(insertBatch)

	// UPDATE 실행: pending → processing, amount +50
	updateSQL := `UPDATE orders SET status = 'processing', amount = 150 WHERE order_id = 1`
	updateBatch, err := ParseDMLToBatchWithStore(updateSQL, store)
	if err != nil {
		t.Fatalf("Failed to parse UPDATE: %v", err)
	}

	// 검증: DELETE(-1) + INSERT(+1) 쌍이 생성되어야 함
	if len(updateBatch) != 2 {
		t.Fatalf("Expected 2 deltas (DELETE + INSERT), got %d", len(updateBatch))
	}

	// 첫 번째는 DELETE (Count: -1)
	deleteTD := updateBatch[0]
	if deleteTD.Count != -1 {
		t.Errorf("Expected Count: -1 for DELETE, got %d", deleteTD.Count)
	}
	if deleteTD.Tuple["status"] != "pending" || deleteTD.Tuple["amount"] != int64(100) {
		t.Errorf("DELETE tuple incorrect: %v", deleteTD.Tuple)
	}

	// 두 번째는 INSERT (Count: +1)
	insertTD := updateBatch[1]
	if insertTD.Count != 1 {
		t.Errorf("Expected Count: 1 for INSERT, got %d", insertTD.Count)
	}
	if insertTD.Tuple["status"] != "processing" || insertTD.Tuple["amount"] != int64(150) {
		t.Errorf("INSERT tuple incorrect: %v", insertTD.Tuple)
	}

	// State에 UPDATE 반영
	table.ApplyBatch(updateBatch)

	// 검증: order_id=1의 상태가 업데이트되어야 함
	allTuples := table.GetAll()
	if len(allTuples) != 2 {
		t.Fatalf("Expected 2 tuples, got %d", len(allTuples))
	}

	var updatedTuple types.Tuple
	for _, tuple := range allTuples {
		if tuple["order_id"] == int64(1) {
			updatedTuple = tuple
			break
		}
	}

	if updatedTuple == nil {
		t.Fatal("Updated tuple not found")
	}

	if updatedTuple["status"] != "processing" {
		t.Errorf("Expected status='processing', got %v", updatedTuple["status"])
	}
	if updatedTuple["amount"] != int64(150) {
		t.Errorf("Expected amount=150, got %v", updatedTuple["amount"])
	}
}

func TestParseDMLToBatchWithStore_DeleteNoMatch(t *testing.T) {
	store := state.NewStore()

	// 초기 데이터 INSERT
	insertSQL := `INSERT INTO orders (order_id, status) VALUES (1, 'completed')`
	insertBatch, err := ParseDMLToBatch(insertSQL)
	if err != nil {
		t.Fatalf("Failed to parse INSERT: %v", err)
	}

	table := store.GetTable("orders")
	table.ApplyBatch(insertBatch)

	// 매치되지 않는 DELETE
	deleteSQL := `DELETE FROM orders WHERE status = 'pending'`
	deleteBatch, err := ParseDMLToBatchWithStore(deleteSQL, store)
	if err != nil {
		t.Fatalf("Failed to parse DELETE: %v", err)
	}

	// 매치되는 튜플이 없으므로 빈 배치
	if len(deleteBatch) != 0 {
		t.Errorf("Expected empty batch, got %d deltas", len(deleteBatch))
	}
}

func TestParseDMLToBatchWithStore_UpdateMultiple(t *testing.T) {
	store := state.NewStore()

	// 초기 데이터 INSERT
	insertSQL := `INSERT INTO orders (order_id, status, amount) VALUES (1, 'pending', 100), (2, 'pending', 200), (3, 'completed', 300)`
	insertBatch, err := ParseDMLToBatch(insertSQL)
	if err != nil {
		t.Fatalf("Failed to parse INSERT: %v", err)
	}

	table := store.GetTable("orders")
	table.ApplyBatch(insertBatch)

	// 여러 튜플 UPDATE
	updateSQL := `UPDATE orders SET status = 'processing' WHERE status = 'pending'`
	updateBatch, err := ParseDMLToBatchWithStore(updateSQL, store)
	if err != nil {
		t.Fatalf("Failed to parse UPDATE: %v", err)
	}

	// 2개의 튜플 × 2 (DELETE + INSERT) = 4개의 델타
	if len(updateBatch) != 4 {
		t.Fatalf("Expected 4 deltas, got %d", len(updateBatch))
	}

	// DELETE와 INSERT가 교대로 나타나야 함
	deleteCount := 0
	insertCount := 0
	for _, td := range updateBatch {
		if td.Count == -1 {
			deleteCount++
			if td.Tuple["status"] != "pending" {
				t.Errorf("DELETE tuple should have old status 'pending', got %v", td.Tuple["status"])
			}
		} else if td.Count == 1 {
			insertCount++
			if td.Tuple["status"] != "processing" {
				t.Errorf("INSERT tuple should have new status 'processing', got %v", td.Tuple["status"])
			}
		}
	}

	if deleteCount != 2 || insertCount != 2 {
		t.Errorf("Expected 2 DELETEs and 2 INSERTs, got %d DELETEs and %d INSERTs", deleteCount, insertCount)
	}

	// State에 UPDATE 반영
	table.ApplyBatch(updateBatch)

	// 검증: order_id 1, 2는 processing, 3은 completed
	allTuples := table.GetAll()
	if len(allTuples) != 3 {
		t.Fatalf("Expected 3 tuples, got %d", len(allTuples))
	}

	for _, tuple := range allTuples {
		orderID := tuple["order_id"].(int64)
		status := tuple["status"].(string)

		if orderID == 1 || orderID == 2 {
			if status != "processing" {
				t.Errorf("order_id=%d should have status='processing', got %v", orderID, status)
			}
		} else if orderID == 3 {
			if status != "completed" {
				t.Errorf("order_id=3 should have status='completed', got %v", status)
			}
		}
	}
}
