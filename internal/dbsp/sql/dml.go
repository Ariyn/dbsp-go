package sqlconv

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/ariyn/dbsp/internal/dbsp/ir"
	"github.com/ariyn/dbsp/internal/dbsp/state"
	"github.com/ariyn/dbsp/internal/dbsp/types"
	"github.com/xwb1989/sqlparser"
)

// ParseDMLToBatch converts INSERT/UPDATE/DELETE SQL statements to types.Batch
// For DELETE and UPDATE, a state store is required
func ParseDMLToBatch(sql string) (types.Batch, error) {
	return ParseDMLToBatchWithStore(sql, nil)
}

// ParseDMLToBatchWithStore converts DML with optional state store for DELETE/UPDATE
func ParseDMLToBatchWithStore(sql string, store *state.Store) (types.Batch, error) {
	stmt, err := sqlparser.Parse(sql)
	if err != nil {
		return nil, fmt.Errorf("failed to parse SQL: %w", err)
	}

	switch stmt := stmt.(type) {
	case *sqlparser.Insert:
		return parseInsert(stmt)
	case *sqlparser.Delete:
		return parseDelete(stmt, store)
	case *sqlparser.Update:
		return parseUpdate(stmt, store)
	default:
		return nil, fmt.Errorf("unsupported SQL statement type: %T", stmt)
	}
}

// parseInsert converts INSERT INTO statement to Batch
func parseInsert(stmt *sqlparser.Insert) (types.Batch, error) {
	var batch types.Batch

	// Get column names
	var columns []string
	if len(stmt.Columns) > 0 {
		for _, col := range stmt.Columns {
			columns = append(columns, col.String())
		}
	}

	// Handle VALUES clause
	switch rows := stmt.Rows.(type) {
	case sqlparser.Values:
		for _, valTuple := range rows {
			tuple := make(types.Tuple)

			for i, expr := range valTuple {
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
	default:
		return nil, fmt.Errorf("unsupported INSERT type: %T", rows)
	}

	return batch, nil
}

// parseDelete converts DELETE FROM statement to Batch with Count: -1
func parseDelete(stmt *sqlparser.Delete, store *state.Store) (types.Batch, error) {
	if store == nil {
		return nil, fmt.Errorf("DELETE requires a state store")
	}

	// Get table name
	tableName := sqlparser.String(stmt.TableExprs)

	// Build predicate from WHERE clause
	var predicate func(types.Tuple) bool
	if stmt.Where != nil {
		whereSQL := sqlparser.String(stmt.Where.Expr)
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
func parseUpdate(stmt *sqlparser.Update, store *state.Store) (types.Batch, error) {
	if store == nil {
		return nil, fmt.Errorf("UPDATE requires a state store")
	}

	// Get table name
	tableName := sqlparser.String(stmt.TableExprs)

	// Parse SET clause
	updates := make(map[string]any)
	for _, expr := range stmt.Exprs {
		colName := expr.Name.Name.String()
		value, err := extractValue(expr.Expr)
		if err != nil {
			return nil, fmt.Errorf("failed to extract update value: %w", err)
		}
		updates[colName] = value
	}

	// Build predicate from WHERE clause
	var predicate func(types.Tuple) bool
	if stmt.Where != nil {
		whereSQL := sqlparser.String(stmt.Where.Expr)
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
func extractValue(expr sqlparser.Expr) (any, error) {
	switch v := expr.(type) {
	case *sqlparser.SQLVal:
		switch v.Type {
		case sqlparser.IntVal:
			return strconv.ParseInt(string(v.Val), 10, 64)
		case sqlparser.FloatVal:
			return strconv.ParseFloat(string(v.Val), 64)
		case sqlparser.StrVal:
			return string(v.Val), nil
		default:
			return string(v.Val), nil
		}
	case *sqlparser.NullVal:
		return nil, nil
	default:
		// Try to convert to string as fallback
		str := strings.Trim(sqlparser.String(v), "'\"")
		// Try to parse as number
		if num, err := strconv.ParseInt(str, 10, 64); err == nil {
			return num, nil
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
