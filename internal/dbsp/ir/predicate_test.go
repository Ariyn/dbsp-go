package ir

import (
	"testing"

	"github.com/ariyn/dbsp/internal/dbsp/op"
	"github.com/ariyn/dbsp/internal/dbsp/types"
)

func TestBuildPredicateFunc_Equality(t *testing.T) {
	pred := buildPredicateFunc("status = 'active'")

	tests := []struct {
		name   string
		tuple  types.Tuple
		expect bool
	}{
		{"match", types.Tuple{"status": "active"}, true},
		{"no match", types.Tuple{"status": "inactive"}, false},
		{"missing field", types.Tuple{"other": "value"}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := pred(tt.tuple)
			if result != tt.expect {
				t.Errorf("expected %v, got %v for tuple %v", tt.expect, result, tt.tuple)
			}
		})
	}
}

func TestBuildPredicateFunc_GreaterThan(t *testing.T) {
	pred := buildPredicateFunc("amount > 50")

	tests := []struct {
		name   string
		tuple  types.Tuple
		expect bool
	}{
		{"greater", types.Tuple{"amount": 100}, true},
		{"equal", types.Tuple{"amount": 50}, false},
		{"less", types.Tuple{"amount": 30}, false},
		{"missing", types.Tuple{"other": 100}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := pred(tt.tuple)
			if result != tt.expect {
				t.Errorf("expected %v, got %v for tuple %v", tt.expect, result, tt.tuple)
			}
		})
	}
}

func TestLogicalFilterToDBSP(t *testing.T) {
	filter := &LogicalFilter{
		PredicateSQL: "status = 'active'",
		Input:        &LogicalScan{Table: "t"},
	}

	node, err := LogicalToDBSP(filter)
	if err != nil {
		t.Fatalf("LogicalToDBSP failed: %v", err)
	}

	mapOp, ok := node.Op.(*op.MapOp)
	if !ok {
		t.Fatalf("expected MapOp, got %T", node.Op)
	}

	batch := types.Batch{
		{Tuple: types.Tuple{"status": "active", "id": 1}, Count: 1},
		{Tuple: types.Tuple{"status": "inactive", "id": 2}, Count: 1},
	}

	out, err := mapOp.Apply(batch)
	if err != nil {
		t.Fatalf("Apply failed: %v", err)
	}

	if len(out) != 1 {
		t.Fatalf("expected 1 output, got %d: %+v", len(out), out)
	}
	if out[0].Tuple["id"] != 1 {
		t.Errorf("expected id=1, got %v", out[0].Tuple["id"])
	}
}

func TestBuildPredicateFunc_LessThan(t *testing.T) {
	pred := buildPredicateFunc("amount < 50")

	tests := []struct {
		name   string
		tuple  types.Tuple
		expect bool
	}{
		{"less", types.Tuple{"amount": 30}, true},
		{"equal", types.Tuple{"amount": 50}, false},
		{"greater", types.Tuple{"amount": 60}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := pred(tt.tuple)
			if result != tt.expect {
				t.Errorf("expected %v, got %v", tt.expect, result)
			}
		})
	}
}

func TestBuildPredicateFunc_GreaterEqual(t *testing.T) {
	pred := buildPredicateFunc("score >= 80")

	tests := []struct {
		name   string
		tuple  types.Tuple
		expect bool
	}{
		{"greater", types.Tuple{"score": 90}, true},
		{"equal", types.Tuple{"score": 80}, true},
		{"less", types.Tuple{"score": 70}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := pred(tt.tuple)
			if result != tt.expect {
				t.Errorf("expected %v, got %v", tt.expect, result)
			}
		})
	}
}

func TestBuildPredicateFunc_LessEqual(t *testing.T) {
	pred := buildPredicateFunc("age <= 30")

	tests := []struct {
		name   string
		tuple  types.Tuple
		expect bool
	}{
		{"less", types.Tuple{"age": 25}, true},
		{"equal", types.Tuple{"age": 30}, true},
		{"greater", types.Tuple{"age": 35}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := pred(tt.tuple)
			if result != tt.expect {
				t.Errorf("expected %v, got %v", tt.expect, result)
			}
		})
	}
}

func TestBuildPredicateFunc_NotEqual(t *testing.T) {
	pred := buildPredicateFunc("status != 'inactive'")

	tests := []struct {
		name   string
		tuple  types.Tuple
		expect bool
	}{
		{"not equal", types.Tuple{"status": "active"}, true},
		{"equal", types.Tuple{"status": "inactive"}, false},
		{"different", types.Tuple{"status": "pending"}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := pred(tt.tuple)
			if result != tt.expect {
				t.Errorf("expected %v, got %v", tt.expect, result)
			}
		})
	}
}

func TestBuildPredicateFunc_AND(t *testing.T) {
	pred := buildPredicateFunc("age > 20 AND status = 'active'")

	tests := []struct {
		name   string
		tuple  types.Tuple
		expect bool
	}{
		{"both true", types.Tuple{"age": 25, "status": "active"}, true},
		{"first false", types.Tuple{"age": 18, "status": "active"}, false},
		{"second false", types.Tuple{"age": 25, "status": "inactive"}, false},
		{"both false", types.Tuple{"age": 18, "status": "inactive"}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := pred(tt.tuple)
			if result != tt.expect {
				t.Errorf("expected %v, got %v for %+v", tt.expect, result, tt.tuple)
			}
		})
	}
}

func TestBuildPredicateFunc_OR(t *testing.T) {
	pred := buildPredicateFunc("status = 'active' OR status = 'pending'")

	tests := []struct {
		name   string
		tuple  types.Tuple
		expect bool
	}{
		{"first true", types.Tuple{"status": "active"}, true},
		{"second true", types.Tuple{"status": "pending"}, true},
		{"both false", types.Tuple{"status": "inactive"}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := pred(tt.tuple)
			if result != tt.expect {
				t.Errorf("expected %v, got %v", tt.expect, result)
			}
		})
	}
}

func TestBuildPredicateFunc_ComplexCondition(t *testing.T) {
	// AND has higher precedence: (age >= 18 AND age < 65) OR status = 'vip'
	pred := buildPredicateFunc("age >= 18 AND age < 65 OR status = 'vip'")

	tests := []struct {
		name   string
		tuple  types.Tuple
		expect bool
	}{
		{"in range", types.Tuple{"age": 30, "status": "normal"}, true},
		{"too young", types.Tuple{"age": 16, "status": "normal"}, false},
		{"too old", types.Tuple{"age": 70, "status": "normal"}, false},
		{"vip young", types.Tuple{"age": 16, "status": "vip"}, true},
		{"vip old", types.Tuple{"age": 70, "status": "vip"}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := pred(tt.tuple)
			if result != tt.expect {
				t.Errorf("expected %v, got %v for %+v", tt.expect, result, tt.tuple)
			}
		})
	}
}

func TestBuildPredicateFunc_Parentheses(t *testing.T) {
	// Test basic parentheses
	pred := buildPredicateFunc("(age > 20)")

	if !pred(types.Tuple{"age": 25}) {
		t.Error("expected true for age=25")
	}
	if pred(types.Tuple{"age": 15}) {
		t.Error("expected false for age=15")
	}
}

func TestBuildPredicateFunc_ParenthesesWithOR(t *testing.T) {
	// Without parens: age > 50 OR status = 'vip' AND amount > 100
	// Natural precedence: age > 50 OR (status = 'vip' AND amount > 100)
	predNatural := buildPredicateFunc("age > 50 OR status = 'vip' AND amount > 100")

	// With parens: (age > 50 OR status = 'vip') AND amount > 100
	predWithParens := buildPredicateFunc("(age > 50 OR status = 'vip') AND amount > 100")

	testTuple := types.Tuple{"age": 60, "status": "normal", "amount": 50}

	// Natural: age > 50 is true, so whole expression is true
	if !predNatural(testTuple) {
		t.Error("natural precedence should be true")
	}

	// With parens: (age > 50 OR status = 'vip') is true, but amount > 100 is false
	if predWithParens(testTuple) {
		t.Error("with parens should be false")
	}
}

func TestBuildPredicateFunc_NestedParentheses(t *testing.T) {
	// ((age >= 18 AND age <= 30) OR (age >= 60 AND age <= 70)) AND status = 'active'
	pred := buildPredicateFunc("((age >= 18 AND age <= 30) OR (age >= 60 AND age <= 70)) AND status = 'active'")

	tests := []struct {
		name   string
		tuple  types.Tuple
		expect bool
	}{
		{"young active", types.Tuple{"age": 25, "status": "active"}, true},
		{"old active", types.Tuple{"age": 65, "status": "active"}, true},
		{"middle active", types.Tuple{"age": 45, "status": "active"}, false},
		{"young inactive", types.Tuple{"age": 25, "status": "inactive"}, false},
		{"old inactive", types.Tuple{"age": 65, "status": "inactive"}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := pred(tt.tuple)
			if result != tt.expect {
				t.Errorf("expected %v, got %v for %+v", tt.expect, result, tt.tuple)
			}
		})
	}
}

func TestBuildPredicateFunc_ComplexWithParentheses(t *testing.T) {
	// (status = 'gold' OR status = 'platinum') AND (amount > 1000 OR frequency > 10)
	pred := buildPredicateFunc("(status = 'gold' OR status = 'platinum') AND (amount > 1000 OR frequency > 10)")

	tests := []struct {
		name   string
		tuple  types.Tuple
		expect bool
	}{
		{"gold high amount", types.Tuple{"status": "gold", "amount": 1500, "frequency": 5}, true},
		{"gold high freq", types.Tuple{"status": "gold", "amount": 500, "frequency": 15}, true},
		{"platinum high amount", types.Tuple{"status": "platinum", "amount": 2000, "frequency": 3}, true},
		{"silver high amount", types.Tuple{"status": "silver", "amount": 1500, "frequency": 5}, false},
		{"gold low both", types.Tuple{"status": "gold", "amount": 500, "frequency": 5}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := pred(tt.tuple)
			if result != tt.expect {
				t.Errorf("expected %v, got %v for %+v", tt.expect, result, tt.tuple)
			}
		})
	}
}
