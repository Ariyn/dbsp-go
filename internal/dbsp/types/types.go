package types

import (
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"reflect"
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

// EqualAny compares two values safely.
//
// It avoids panics on uncomparable values, compares maps/slices structurally,
// and treats numeric values as equal across types (e.g. 1 == 1.0), including
// int/uint/float and json.Number.
func EqualAny(a, b any) bool {
	return equalValue(reflect.ValueOf(a), reflect.ValueOf(b))
}

// TuplesEqual compares two tuples using EqualAny for values.
func TuplesEqual(a, b Tuple) bool {
	if len(a) != len(b) {
		return false
	}
	for k, av := range a {
		bv, ok := b[k]
		if !ok || !EqualAny(av, bv) {
			return false
		}
	}
	return true
}

func equalValue(a, b reflect.Value) bool {
	a = unwrapInterface(a)
	b = unwrapInterface(b)

	if !a.IsValid() || !b.IsValid() {
		return !a.IsValid() && !b.IsValid()
	}

	if a.CanInterface() && b.CanInterface() {
		if ar, ok := toRational(a.Interface()); ok {
			if br, ok := toRational(b.Interface()); ok {
				return ar.Cmp(br) == 0
			}
		}
	}

	if a.Kind() != b.Kind() {
		if a.CanInterface() && b.CanInterface() {
			return reflect.DeepEqual(a.Interface(), b.Interface())
		}
		return false
	}

	switch a.Kind() {
	case reflect.Bool:
		return a.Bool() == b.Bool()
	case reflect.String:
		return a.String() == b.String()
	case reflect.Slice, reflect.Array:
		if a.Kind() == reflect.Slice && (a.IsNil() != b.IsNil()) {
			return false
		}
		if a.Len() != b.Len() {
			return false
		}
		for i := 0; i < a.Len(); i++ {
			if !equalValue(a.Index(i), b.Index(i)) {
				return false
			}
		}
		return true
	case reflect.Map:
		if a.IsNil() != b.IsNil() {
			return false
		}
		if a.Len() != b.Len() {
			return false
		}
		keysA := a.MapKeys()
		keysB := b.MapKeys()
		used := make([]bool, len(keysB))
		for _, ka := range keysA {
			matched := -1
			for j, kb := range keysB {
				if used[j] {
					continue
				}
				if equalValue(ka, kb) {
					matched = j
					break
				}
			}
			if matched < 0 {
				return false
			}
			used[matched] = true
			if !equalValue(a.MapIndex(ka), b.MapIndex(keysB[matched])) {
				return false
			}
		}
		return true
	case reflect.Struct:
		if a.Type() != b.Type() {
			return false
		}
		for i := 0; i < a.NumField(); i++ {
			if !equalValue(a.Field(i), b.Field(i)) {
				return false
			}
		}
		return true
	case reflect.Ptr:
		if a.IsNil() || b.IsNil() {
			return a.IsNil() && b.IsNil()
		}
		return equalValue(a.Elem(), b.Elem())
	case reflect.Func:
		return a.IsNil() && b.IsNil()
	default:
		if a.Type() == b.Type() && a.Type().Comparable() {
			return a.Interface() == b.Interface()
		}
		if a.CanInterface() && b.CanInterface() {
			return reflect.DeepEqual(a.Interface(), b.Interface())
		}
		return false
	}
}

func unwrapInterface(v reflect.Value) reflect.Value {
	for v.IsValid() && v.Kind() == reflect.Interface {
		if v.IsNil() {
			return reflect.Value{}
		}
		v = v.Elem()
	}
	return v
}

func toRational(v any) (*big.Rat, bool) {
	switch n := v.(type) {
	case json.Number:
		if i, err := n.Int64(); err == nil {
			return new(big.Rat).SetInt64(i), true
		}
		f, err := n.Float64()
		if err != nil || math.IsNaN(f) || math.IsInf(f, 0) {
			return nil, false
		}
		r, ok := new(big.Rat).SetString(n.String())
		if ok {
			return r, true
		}
		return nil, false
	case int:
		return new(big.Rat).SetInt64(int64(n)), true
	case int8:
		return new(big.Rat).SetInt64(int64(n)), true
	case int16:
		return new(big.Rat).SetInt64(int64(n)), true
	case int32:
		return new(big.Rat).SetInt64(int64(n)), true
	case int64:
		return new(big.Rat).SetInt64(n), true
	case uint:
		return new(big.Rat).SetUint64(uint64(n)), true
	case uint8:
		return new(big.Rat).SetUint64(uint64(n)), true
	case uint16:
		return new(big.Rat).SetUint64(uint64(n)), true
	case uint32:
		return new(big.Rat).SetUint64(uint64(n)), true
	case uint64:
		return new(big.Rat).SetUint64(n), true
	case uintptr:
		return new(big.Rat).SetUint64(uint64(n)), true
	case float32:
		f := float64(n)
		if math.IsNaN(f) || math.IsInf(f, 0) {
			return nil, false
		}
		r, ok := new(big.Rat).SetString(strconv.FormatFloat(f, 'g', -1, 32))
		if !ok {
			return nil, false
		}
		return r, true
	case float64:
		if math.IsNaN(n) || math.IsInf(n, 0) {
			return nil, false
		}
		r, ok := new(big.Rat).SetString(strconv.FormatFloat(n, 'g', -1, 64))
		if !ok {
			return nil, false
		}
		return r, true
	default:
		return nil, false
	}
}
