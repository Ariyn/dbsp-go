package types

// Tuple represents a row as a map from column name to value.
type Tuple map[string]any

// TupleDelta represents a change to a tuple: Count +1 insert, -1 delete
type TupleDelta struct {
	Tuple Tuple
	Count int64
}

// Batch is a collection of TupleDelta items (a delta-batch)
type Batch []TupleDelta
