package state

import (
	"testing"

	"github.com/ariyn/dbsp/internal/dbsp/types"
)

func TestStore_InsertAndSelect(t *testing.T) {
	store := NewStore()
	table := store.GetTable("users")

	batch := types.Batch{
		{Tuple: types.Tuple{"id": int64(1), "name": "Alice"}, Count: 1},
		{Tuple: types.Tuple{"id": int64(2), "name": "Bob"}, Count: 1},
	}

	err := table.ApplyBatch(batch)
	if err != nil {
		t.Fatalf("ApplyBatch failed: %v", err)
	}

	if table.Count() != 2 {
		t.Errorf("expected 2 rows, got %d", table.Count())
	}

	all := table.GetAll()
	if len(all) != 2 {
		t.Errorf("expected 2 tuples, got %d", len(all))
	}
}

func TestTable_Delete(t *testing.T) {
	store := NewStore()
	table := store.GetTable("users")

	insertBatch := types.Batch{
		{Tuple: types.Tuple{"id": int64(1), "name": "Alice", "status": "active"}, Count: 1},
		{Tuple: types.Tuple{"id": int64(2), "name": "Bob", "status": "inactive"}, Count: 1},
		{Tuple: types.Tuple{"id": int64(3), "name": "Charlie", "status": "active"}, Count: 1},
	}

	table.ApplyBatch(insertBatch)

	deleteBatch := table.Delete(func(tuple types.Tuple) bool {
		return tuple["status"] == "inactive"
	})

	if len(deleteBatch) != 1 {
		t.Errorf("expected 1 deletion, got %d", len(deleteBatch))
	}

	if deleteBatch[0].Count != -1 {
		t.Errorf("expected Count=-1, got %d", deleteBatch[0].Count)
	}

	if table.Count() != 2 {
		t.Errorf("expected 2 remaining rows, got %d", table.Count())
	}
}

func TestTable_Update(t *testing.T) {
	store := NewStore()
	table := store.GetTable("products")

	insertBatch := types.Batch{
		{Tuple: types.Tuple{"id": int64(1), "name": "Laptop", "price": int64(1000)}, Count: 1},
		{Tuple: types.Tuple{"id": int64(2), "name": "Mouse", "price": int64(50)}, Count: 1},
	}

	table.ApplyBatch(insertBatch)

	updateBatch := table.Update(
		func(tuple types.Tuple) bool {
			return tuple["id"] == int64(1)
		},
		map[string]any{"price": int64(1200)},
	)

	if len(updateBatch) != 2 {
		t.Fatalf("expected 2 tuples in update batch, got %d", len(updateBatch))
	}

	if updateBatch[0].Count != -1 {
		t.Errorf("expected DELETE with Count=-1, got %d", updateBatch[0].Count)
	}

	if updateBatch[1].Count != 1 {
		t.Errorf("expected INSERT with Count=1, got %d", updateBatch[1].Count)
	}
}
