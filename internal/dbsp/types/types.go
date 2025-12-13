package types

import (
	"fmt"
	"strconv"
	"strings"
)

// Tuple represents a row as a map from column name to value.
type Tuple map[string]any

// TupleDelta represents a change to a tuple: Count +1 insert, -1 delete
type TupleDelta struct {
	Tuple Tuple
	Count int64
}

// Batch is a collection of TupleDelta items (a delta-batch)
type Batch []TupleDelta

// Interval represents a time duration in milliseconds
type Interval struct {
	Millis int64
}

// ParseInterval parses interval strings like "5 minutes", "1 hour", "30 seconds"
func ParseInterval(s string) (Interval, error) {
	parts := strings.Fields(strings.TrimSpace(s))
	if len(parts) != 2 {
		return Interval{}, fmt.Errorf("invalid interval format: %s", s)
	}

	value, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return Interval{}, fmt.Errorf("invalid interval value: %s", parts[0])
	}

	unit := strings.ToLower(parts[1])
	// Remove trailing 's' for plural forms
	unit = strings.TrimSuffix(unit, "s")

	var millis int64
	switch unit {
	case "millisecond", "ms":
		millis = value
	case "second", "sec":
		millis = value * 1000
	case "minute", "min":
		millis = value * 60 * 1000
	case "hour", "hr", "h":
		millis = value * 60 * 60 * 1000
	case "day", "d":
		millis = value * 24 * 60 * 60 * 1000
	default:
		return Interval{}, fmt.Errorf("unknown interval unit: %s", unit)
	}

	return Interval{Millis: millis}, nil
}

// String returns a human-readable representation of the interval
func (i Interval) String() string {
	if i.Millis == 0 {
		return "0 milliseconds"
	}

	const (
		day    = 24 * 60 * 60 * 1000
		hour   = 60 * 60 * 1000
		minute = 60 * 1000
		second = 1000
	)

	ms := i.Millis
	if ms%day == 0 {
		return fmt.Sprintf("%d day(s)", ms/day)
	}
	if ms%hour == 0 {
		return fmt.Sprintf("%d hour(s)", ms/hour)
	}
	if ms%minute == 0 {
		return fmt.Sprintf("%d minute(s)", ms/minute)
	}
	if ms%second == 0 {
		return fmt.Sprintf("%d second(s)", ms/second)
	}
	return fmt.Sprintf("%d millisecond(s)", ms)
}
