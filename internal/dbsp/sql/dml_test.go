package sqlconv

import (
	"testing"
)

func TestParseDMLToBatch_Insert(t *testing.T) {
	sql := "INSERT INTO sales (time_bucket, amount, product) VALUES ('10:00', 1000, 'A')"

	batch, err := ParseDMLToBatch(sql)
	if err != nil {
		t.Fatalf("ParseDMLToBatch failed: %v", err)
	}

	if len(batch) != 1 {
		t.Fatalf("expected 1 tuple, got %d", len(batch))
	}

	td := batch[0]
	if td.Count != 1 {
		t.Errorf("expected Count=1, got %d", td.Count)
	}

	if td.Tuple["time_bucket"] != "10:00" {
		t.Errorf("expected time_bucket='10:00', got %v", td.Tuple["time_bucket"])
	}

	if td.Tuple["amount"] != int64(1000) {
		t.Errorf("expected amount=1000, got %v", td.Tuple["amount"])
	}

	if td.Tuple["product"] != "A" {
		t.Errorf("expected product='A', got %v", td.Tuple["product"])
	}
}

func TestParseDMLToBatch_InsertMultipleRows(t *testing.T) {
	sql := `INSERT INTO sales (time_bucket, amount) VALUES 
		('10:00', 1000),
		('10:05', 2000),
		('10:10', 3000)`

	batch, err := ParseDMLToBatch(sql)
	if err != nil {
		t.Fatalf("ParseDMLToBatch failed: %v", err)
	}

	if len(batch) != 3 {
		t.Fatalf("expected 3 tuples, got %d", len(batch))
	}

	expected := []struct {
		timeBucket string
		amount     int64
	}{
		{"10:00", 1000},
		{"10:05", 2000},
		{"10:10", 3000},
	}

	for i, exp := range expected {
		if batch[i].Tuple["time_bucket"] != exp.timeBucket {
			t.Errorf("row %d: expected time_bucket=%s, got %v", i, exp.timeBucket, batch[i].Tuple["time_bucket"])
		}
		if batch[i].Tuple["amount"] != exp.amount {
			t.Errorf("row %d: expected amount=%d, got %v", i, exp.amount, batch[i].Tuple["amount"])
		}
	}
}

func TestParseDMLToBatch_InsertWithoutColumns(t *testing.T) {
	sql := "INSERT INTO sales VALUES ('10:00', 1000, 'A')"

	batch, err := ParseDMLToBatch(sql)
	if err != nil {
		t.Fatalf("ParseDMLToBatch failed: %v", err)
	}

	if len(batch) != 1 {
		t.Fatalf("expected 1 tuple, got %d", len(batch))
	}

	td := batch[0]

	// Without column names, should use col0, col1, col2
	if td.Tuple["col0"] != "10:00" {
		t.Errorf("expected col0='10:00', got %v", td.Tuple["col0"])
	}
	if td.Tuple["col1"] != int64(1000) {
		t.Errorf("expected col1=1000, got %v", td.Tuple["col1"])
	}
	if td.Tuple["col2"] != "A" {
		t.Errorf("expected col2='A', got %v", td.Tuple["col2"])
	}
}

func TestParseMultiDMLToBatch(t *testing.T) {
	sql := `
		INSERT INTO sales (time_bucket, amount) VALUES ('10:00', 1000);
		INSERT INTO sales (time_bucket, amount) VALUES ('10:05', 2000);
		INSERT INTO sales (time_bucket, amount) VALUES ('10:10', 3000);
	`

	batch, err := ParseMultiDMLToBatch(sql)
	if err != nil {
		t.Fatalf("ParseMultiDMLToBatch failed: %v", err)
	}

	if len(batch) != 3 {
		t.Fatalf("expected 3 tuples, got %d", len(batch))
	}

	amounts := []int64{1000, 2000, 3000}
	for i, exp := range amounts {
		if batch[i].Tuple["amount"] != exp {
			t.Errorf("row %d: expected amount=%d, got %v", i, exp, batch[i].Tuple["amount"])
		}
	}
}

func TestParseDMLToBatch_InsertNumbers(t *testing.T) {
	sql := "INSERT INTO orders (id, amount, price) VALUES (1, 100, 99.99)"

	batch, err := ParseDMLToBatch(sql)
	if err != nil {
		t.Fatalf("ParseDMLToBatch failed: %v", err)
	}

	if len(batch) != 1 {
		t.Fatalf("expected 1 tuple, got %d", len(batch))
	}

	td := batch[0]

	if td.Tuple["id"] != int64(1) {
		t.Errorf("expected id=1 (int64), got %v (%T)", td.Tuple["id"], td.Tuple["id"])
	}

	if td.Tuple["amount"] != int64(100) {
		t.Errorf("expected amount=100 (int64), got %v (%T)", td.Tuple["amount"], td.Tuple["amount"])
	}

	price, ok := td.Tuple["price"].(float64)
	if !ok {
		t.Errorf("expected price to be float64, got %T", td.Tuple["price"])
	}
	if price != 99.99 {
		t.Errorf("expected price=99.99, got %v", price)
	}
}
