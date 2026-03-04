package ir

import "strings"

// KnownAggregates is the set of supported aggregate function names (upper-cased).
var KnownAggregates = map[string]bool{
	"SUM":   true,
	"COUNT": true,
	"AVG":   true,
	"MIN":   true,
	"MAX":   true,
}

// IsAggregate reports whether name is a supported aggregate function.
func IsAggregate(name string) bool {
	return KnownAggregates[strings.ToUpper(name)]
}
