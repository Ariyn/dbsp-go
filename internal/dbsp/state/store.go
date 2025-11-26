package state

import (
	"fmt"
	"sync"

	"github.com/ariyn/dbsp/internal/dbsp/types"
)

// Store maintains the current state (snapshot) of all tables
type Store struct {
	mu     sync.RWMutex
	tables map[string]*Table
}

// Table represents a single table's state
type Table struct {
	mu    sync.RWMutex
	name  string
	rows  []types.Tuple
	index map[string][]int
}

// NewStore creates a new state store
func NewStore() *Store {
	return &Store{
		tables: make(map[string]*Table),
	}
}

// GetTable returns a table, creating it if it doesn't exist
func (s *Store) GetTable(name string) *Table {
	s.mu.Lock()
	defer s.mu.Unlock()

	if t, ok := s.tables[name]; ok {
		return t
	}

	t := &Table{
		name:  name,
		rows:  make([]types.Tuple, 0),
		index: make(map[string][]int),
	}
	s.tables[name] = t
	return t
}

// ApplyBatch applies a batch of changes to the store
func (s *Store) ApplyBatch(tableName string, batch types.Batch) error {
	table := s.GetTable(tableName)
	return table.ApplyBatch(batch)
}

// ApplyBatch applies changes to this table
func (t *Table) ApplyBatch(batch types.Batch) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	for _, td := range batch {
		if td.Count > 0 {
			for i := int64(0); i < td.Count; i++ {
				t.rows = append(t.rows, copyTuple(td.Tuple))
			}
		} else if td.Count < 0 {
			for i := int64(0); i < -td.Count; i++ {
				if !t.removeTuple(td.Tuple) {
					return fmt.Errorf("tuple not found for deletion: %v", td.Tuple)
				}
			}
		}
	}

	return nil
}

// removeTuple removes the first matching tuple
func (t *Table) removeTuple(tuple types.Tuple) bool {
	for i, row := range t.rows {
		if tuplesEqual(row, tuple) {
			t.rows[i] = t.rows[len(t.rows)-1]
			t.rows = t.rows[:len(t.rows)-1]
			return true
		}
	}
	return false
}

// Select returns all tuples matching the predicate
func (t *Table) Select(predicate func(types.Tuple) bool) []types.Tuple {
	t.mu.RLock()
	defer t.mu.RUnlock()

	var result []types.Tuple
	for _, row := range t.rows {
		if predicate == nil || predicate(row) {
			result = append(result, copyTuple(row))
		}
	}
	return result
}

// Delete removes tuples matching predicate and returns batch with Count: -1
func (t *Table) Delete(predicate func(types.Tuple) bool) types.Batch {
	t.mu.Lock()
	defer t.mu.Unlock()

	var batch types.Batch
	var remaining []types.Tuple

	for _, row := range t.rows {
		if predicate(row) {
			batch = append(batch, types.TupleDelta{
				Tuple: copyTuple(row),
				Count: -1,
			})
		} else {
			remaining = append(remaining, row)
		}
	}

	t.rows = remaining
	return batch
}

// Update modifies tuples and returns DELETE + INSERT batch
func (t *Table) Update(predicate func(types.Tuple) bool, updates map[string]any) types.Batch {
	t.mu.Lock()
	defer t.mu.Unlock()

	var batch types.Batch

	for i, row := range t.rows {
		if predicate(row) {
			batch = append(batch, types.TupleDelta{
				Tuple: copyTuple(row),
				Count: -1,
			})

			newTuple := copyTuple(row)
			for k, v := range updates {
				newTuple[k] = v
			}

			batch = append(batch, types.TupleDelta{
				Tuple: newTuple,
				Count: 1,
			})

			t.rows[i] = newTuple
		}
	}

	return batch
}

// Count returns the number of rows
func (t *Table) Count() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return len(t.rows)
}

// GetAll returns all tuples
func (t *Table) GetAll() []types.Tuple {
	return t.Select(nil)
}

func copyTuple(t types.Tuple) types.Tuple {
	copy := make(types.Tuple, len(t))
	for k, v := range t {
		copy[k] = v
	}
	return copy
}

func tuplesEqual(a, b types.Tuple) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}
