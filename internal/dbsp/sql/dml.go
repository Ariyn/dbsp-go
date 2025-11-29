package sqlconv

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/Ariyn/tree-sitter-duckdb/bindings/go/ast"
	"github.com/Ariyn/tree-sitter-duckdb/bindings/go/parser"
	"github.com/ariyn/dbsp/internal/dbsp/ir"
	"github.com/ariyn/dbsp/internal/dbsp/state"
	"github.com/ariyn/dbsp/internal/dbsp/types"
)

// ParseDMLToBatch converts INSERT/UPDATE/DELETE SQL statements to types.Batch
// For DELETE and UPDATE, a state store is required
func ParseDMLToBatch(sql string) (types.Batch, error) {
	return ParseDMLToBatchWithStore(sql, nil)
}

// ParseDMLToBatchWithStore converts DML with optional state store for DELETE/UPDATE
func ParseDMLToBatchWithStore(sql string, store *state.Store) (types.Batch, error) {
	p := parser.NewParser()
	stmt, err := p.Parse(sql)
	if err != nil {
		return nil, fmt.Errorf("failed to parse SQL: %w", err)
	}

	switch stmt := stmt.(type) {
	case *ast.Insert:
		return parseInsert(stmt)
	case *ast.Delete:
		return parseDelete(stmt, store)
	case *ast.Update:
		return parseUpdate(stmt, store)
	default:
		return nil, fmt.Errorf("unsupported SQL statement type: %T", stmt)
	}
}

// parseInsert converts INSERT INTO statement to Batch
func parseInsert(stmt *ast.Insert) (types.Batch, error) {
	var batch types.Batch

	// Get column names
	columns := stmt.Columns

	// Handle VALUES clause
	if len(stmt.Values) == 0 {
		return nil, fmt.Errorf("unsupported INSERT type: only VALUES is supported")
	}

	for _, row := range stmt.Values {
		tuple := make(types.Tuple)
		for i, expr := range row {
			var colName string
			if i < len(columns) {
				colName = columns[i]
			} else {
				colName = fmt.Sprintf("col%d", i)
			}

			value, err := extractValue(expr)
			if err != nil {
				return nil, fmt.Errorf("failed to extract value: %w", err)
			}
			tuple[colName] = value
		}

		batch = append(batch, types.TupleDelta{
			Tuple: tuple,
			Count: 1,
		})
	}

	return batch, nil
}

// parseDelete converts DELETE FROM statement to Batch with Count: -1
func parseDelete(stmt *ast.Delete, store *state.Store) (types.Batch, error) {
	if store == nil {
		return nil, fmt.Errorf("DELETE requires a state store")
	}

	// Get table name
	tableName := stmt.Table

	// Build predicate from WHERE clause
	var predicate func(types.Tuple) bool
	if stmt.Where != nil {
		whereSQL := stmt.Where.String()
		predicate = ir.BuildPredicateFunc(whereSQL)
	} else {
		// No WHERE clause: delete all
		predicate = func(types.Tuple) bool { return true }
	}

	// Execute delete on store
	table := store.GetTable(tableName)
	return table.Delete(predicate), nil
}

// parseUpdate converts UPDATE statement to Batch (DELETE old + INSERT new)
func parseUpdate(stmt *ast.Update, store *state.Store) (types.Batch, error) {
	if store == nil {
		return nil, fmt.Errorf("UPDATE requires a state store")
	}

	// Get table name
	tableName := stmt.Table

	// Parse SET clause
	updates := make(map[string]any)
	for _, a := range stmt.Set {
		colName := a.Column
		value, err := extractValue(a.Value)
		if err != nil {
			return nil, fmt.Errorf("failed to extract update value: %w", err)
		}
		updates[colName] = value
	}

	// Build predicate from WHERE clause
	var predicate func(types.Tuple) bool
	if stmt.Where != nil {
		whereSQL := stmt.Where.String()
		predicate = ir.BuildPredicateFunc(whereSQL)
	} else {
		// No WHERE clause: update all
		predicate = func(types.Tuple) bool { return true }
	}

	// Execute update on store
	table := store.GetTable(tableName)
	return table.Update(predicate, updates), nil
}

// extractValue extracts Go value from sqlparser.Expr
func extractValue(expr ast.Expr) (any, error) {
	switch v := expr.(type) {
	case *ast.Literal:
		switch v.Type {
		case "INTEGER":
			// Try int first, but fall back to float if it contains decimal point
			if strings.Contains(v.Value, ".") {
				return strconv.ParseFloat(v.Value, 64)
			}
			return strconv.ParseInt(v.Value, 10, 64)
		case "FLOAT":
			return strconv.ParseFloat(v.Value, 64)
		case "STRING":
			return v.Value, nil
		case "BOOL":
			return strconv.ParseBool(v.Value)
		case "NULL":
			return nil, nil
		default:
			return v.Value, nil
		}
	default:
		// Try to convert to string as fallback
		str := strings.Trim(expr.String(), "'\"")
		// Try to parse as number
		if !strings.Contains(str, ".") {
			if num, err := strconv.ParseInt(str, 10, 64); err == nil {
				return num, nil
			}
		}
		if num, err := strconv.ParseFloat(str, 64); err == nil {
			return num, nil
		}
		return str, nil
	}
}

// ParseMultiDMLToBatch parses multiple DML statements separated by semicolons
func ParseMultiDMLToBatch(sql string) (types.Batch, error) {
	var allBatch types.Batch

	// Split by semicolon
	statements := strings.Split(sql, ";")
	for _, stmt := range statements {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" {
			continue
		}

		batch, err := ParseDMLToBatch(stmt)
		if err != nil {
			return nil, err
		}
		allBatch = append(allBatch, batch...)
	}

	return allBatch, nil
}
