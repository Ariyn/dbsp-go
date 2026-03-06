package types

import (
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"reflect"
	"strconv"
	"strings"
	"time"
)

// Tuple represents a row as a map from column name to value.
type Tuple map[string]any

type PackedSchema struct {
	Columns []string
	index   map[string]int
}

type PackedTuple struct {
	Schema  *PackedSchema
	Values  []any
	Present []bool
	Extras  Tuple
}

// TupleDelta represents a change to a tuple: Count +1 insert, -1 delete
type TupleDelta struct {
	Tuple  Tuple
	Packed *PackedTuple
	Count  int64
}

// Batch is a collection of TupleDelta items (a delta-batch)
type Batch []TupleDelta

func NewPackedSchema(columns []string) *PackedSchema {
	if len(columns) == 0 {
		return nil
	}
	index := make(map[string]int, len(columns))
	cloned := make([]string, len(columns))
	copy(cloned, columns)
	for i, col := range cloned {
		index[col] = i
	}
	return &PackedSchema{Columns: cloned, index: index}
}

func NewPackedTuple(schema *PackedSchema, values []any) *PackedTuple {
	if schema == nil {
		return nil
	}
	present := make([]bool, len(values))
	for idx := range present {
		present[idx] = true
	}
	return NewPackedTupleWithPresence(schema, values, present)
}

func NewPackedTupleWithPresence(schema *PackedSchema, values []any, present []bool) *PackedTuple {
	if schema == nil {
		return nil
	}
	cloned := make([]any, len(values))
	copy(cloned, values)
	var clonedPresent []bool
	if len(present) > 0 {
		clonedPresent = make([]bool, len(present))
		copy(clonedPresent, present)
	}
	return &PackedTuple{Schema: schema, Values: cloned, Present: clonedPresent}
}

func (p *PackedTuple) Get(col string) (any, bool) {
	if p == nil {
		return nil, false
	}
	if p.Extras != nil {
		if value, ok := p.Extras[col]; ok {
			return value, true
		}
	}
	if p.Schema == nil {
		return nil, false
	}
	p.Schema.ensureIndex()
	idx, ok := p.Schema.index[col]
	if !ok || idx < 0 || idx >= len(p.Values) {
		return nil, false
	}
	if len(p.Present) > 0 {
		if idx >= len(p.Present) || !p.Present[idx] {
			return nil, false
		}
		return p.Values[idx], true
	}
	value := p.Values[idx]
	if value == nil {
		return nil, false
	}
	return value, true
}

func (s *PackedSchema) ensureIndex() {
	if s == nil || s.index != nil {
		return
	}
	s.index = make(map[string]int, len(s.Columns))
	for idx, col := range s.Columns {
		s.index[col] = idx
	}
}

func (p *PackedTuple) WithExtra(col string, value any) *PackedTuple {
	if p == nil {
		return nil
	}
	return p.WithExtras(Tuple{col: value})
}

func (p *PackedTuple) WithExtras(extras Tuple) *PackedTuple {
	if p == nil {
		return nil
	}
	out := &PackedTuple{Schema: p.Schema, Values: p.Values, Present: p.Present}
	if len(p.Extras) == 0 && len(extras) == 0 {
		return out
	}
	merged := make(Tuple, len(p.Extras)+len(extras))
	for key, existing := range p.Extras {
		merged[key] = existing
	}
	for key, value := range extras {
		merged[key] = value
	}
	out.Extras = merged
	return out
}

func (p *PackedTuple) Materialize() Tuple {
	if p == nil {
		return nil
	}
	baseLen := 0
	if p.Schema != nil {
		baseLen = len(p.Schema.Columns)
	}
	out := make(Tuple, baseLen+len(p.Extras))
	if p.Schema != nil {
		for idx, col := range p.Schema.Columns {
			if idx >= len(p.Values) {
				break
			}
			if len(p.Present) > 0 {
				if idx >= len(p.Present) || !p.Present[idx] {
					continue
				}
				out[col] = p.Values[idx]
				continue
			}
			if value := p.Values[idx]; value != nil {
				out[col] = value
			}
		}
	}
	for key, value := range p.Extras {
		out[key] = value
	}
	return out
}

func (p *PackedTuple) Clone() *PackedTuple {
	if p == nil {
		return nil
	}
	out := &PackedTuple{Schema: p.Schema, Values: p.Values, Present: p.Present}
	if len(p.Extras) > 0 {
		extra := make(Tuple, len(p.Extras))
		for key, value := range p.Extras {
			extra[key] = value
		}
		out.Extras = extra
	}
	return out
}

func (p *PackedTuple) Project(columns []string) *PackedTuple {
	if p == nil {
		return nil
	}
	if len(columns) == 0 {
		return &PackedTuple{Schema: NewPackedSchema(nil)}
	}
	baseColumns := make([]string, 0, len(columns))
	values := make([]any, 0, len(columns))
	present := make([]bool, 0, len(columns))
	var extras Tuple
	for _, col := range columns {
		if p.Schema != nil {
			p.Schema.ensureIndex()
			if idx, ok := p.Schema.index[col]; ok {
				baseColumns = append(baseColumns, col)
				if idx < len(p.Values) {
					values = append(values, p.Values[idx])
				} else {
					values = append(values, nil)
				}
				if len(p.Present) > 0 && idx < len(p.Present) {
					present = append(present, p.Present[idx])
				} else {
					present = append(present, idx < len(p.Values))
				}
				continue
			}
		}
		if value, ok := p.Get(col); ok {
			if extras == nil {
				extras = make(Tuple)
			}
			extras[col] = value
		}
	}
	projected := NewPackedTupleWithPresence(NewPackedSchema(baseColumns), values, present)
	if projected == nil {
		if len(extras) == 0 {
			return nil
		}
		return &PackedTuple{Extras: extras}
	}
	projected.Extras = extras
	return projected
}

func (td *TupleDelta) Get(col string) (any, bool) {
	if td == nil {
		return nil, false
	}
	if td.Tuple != nil {
		value, ok := td.Tuple[col]
		return value, ok
	}
	if td.Packed != nil {
		return td.Packed.Get(col)
	}
	return nil, false
}

func (td *TupleDelta) EnsureTuple() Tuple {
	if td == nil {
		return nil
	}
	if td.Tuple != nil {
		return td.Tuple
	}
	if td.Packed == nil {
		return nil
	}
	td.Tuple = td.Packed.Materialize()
	return td.Tuple
}

func MaterializeBatch(batch Batch) Batch {
	for idx := range batch {
		batch[idx].EnsureTuple()
		batch[idx].Packed = nil
	}
	return batch
}

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

// ParseFlexibleDuration parses human-readable duration strings like "5m", "1h", "1 hour", "30 seconds".
// It tries time.ParseDuration first (e.g. "5m", "1h30m"), then falls back to ParseInterval.
func ParseFlexibleDuration(s string) (time.Duration, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, nil
	}
	if d, err := time.ParseDuration(s); err == nil {
		return d, nil
	}
	iv, err := ParseInterval(s)
	if err != nil {
		return 0, err
	}
	return time.Duration(iv.Millis) * time.Millisecond, nil
}

// EqualAny compares two values safely.
//
// It avoids panics on uncomparable values, compares maps/slices structurally,
// and treats numeric values as equal across types (e.g. 1 == 1.0), including
// int/uint/float and json.Number.
func EqualAny(a, b any) bool {
	if fast, ok := fastEqualAny(a, b); ok {
		return fast
	}
	return equalValue(reflect.ValueOf(a), reflect.ValueOf(b))
}

func fastEqualAny(a, b any) (bool, bool) {
	if a == nil || b == nil {
		return a == b, true
	}

	switch av := a.(type) {
	case bool:
		bv, ok := b.(bool)
		if !ok {
			return false, false
		}
		return av == bv, true
	case string:
		bv, ok := b.(string)
		if !ok {
			return false, false
		}
		return av == bv, true
	case time.Time:
		bv, ok := b.(time.Time)
		if !ok {
			return false, false
		}
		return av.Equal(bv), true
	}

	if isNumericType(a) && isNumericType(b) {
		if ai, aok := signedInt64Exact(a); aok {
			if bi, bok := signedInt64Exact(b); bok {
				return ai == bi, true
			}
		}
		if au, aok := unsignedInt64Exact(a); aok {
			if bu, bok := unsignedInt64Exact(b); bok {
				return au == bu, true
			}
		}
		if af, aok := ToFloat64Safe(a); aok {
			if bf, bok := ToFloat64Safe(b); bok {
				return af == bf, true
			}
		}
	}

	return false, false
}

func isNumericType(v any) bool {
	switch v.(type) {
	case json.Number,
		int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64, uintptr,
		float32, float64:
		return true
	default:
		return false
	}
}

func signedInt64Exact(v any) (int64, bool) {
	switch n := v.(type) {
	case int:
		return int64(n), true
	case int8:
		return int64(n), true
	case int16:
		return int64(n), true
	case int32:
		return int64(n), true
	case int64:
		return n, true
	default:
		return 0, false
	}
}

func unsignedInt64Exact(v any) (uint64, bool) {
	switch n := v.(type) {
	case uint:
		return uint64(n), true
	case uint8:
		return uint64(n), true
	case uint16:
		return uint64(n), true
	case uint32:
		return uint64(n), true
	case uint64:
		return n, true
	case uintptr:
		return uint64(n), true
	default:
		return 0, false
	}
}

// CloneTuple returns a shallow copy of the tuple.
func CloneTuple(t Tuple) Tuple {
	if t == nil {
		return nil
	}
	out := make(Tuple, len(t))
	for k, v := range t {
		out[k] = v
	}
	return out
}

func ClonePackedTuple(p *PackedTuple) *PackedTuple {
	if p == nil {
		return nil
	}
	return p.Clone()
}

// CloneConfigMap returns a shallow copy of a configuration map.
func CloneConfigMap(in map[string]any) map[string]any {
	if in == nil {
		return nil
	}
	out := make(map[string]any, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

// CloneBatch returns a shallow copy of the batch.
func CloneBatch(b Batch) Batch {
	if b == nil {
		return nil
	}
	out := make(Batch, 0, len(b))
	for _, td := range b {
		out = append(out, TupleDelta{Tuple: CloneTuple(td.Tuple), Packed: ClonePackedTuple(td.Packed), Count: td.Count})
	}
	return out
}

// ToFloat64 attempts to coerce any numeric or string value to float64.
// ToFloat64 attempts to coerce any numeric or string value to float64.
func ToFloat64(v any) float64 {
	f, _ := ToFloat64Safe(v)
	return f
}

// ToFloat64Safe attempts to coerce any numeric or string value to float64, returning success.
func ToFloat64Safe(v any) (float64, bool) {
	switch x := v.(type) {
	case float64:
		return x, true
	case float32:
		return float64(x), true
	case int:
		return float64(x), true
	case int64:
		return float64(x), true
	case int32:
		return float64(x), true
	case uint:
		return float64(x), true
	case uint64:
		return float64(x), true
	case uint32:
		return float64(x), true
	case string:
		trimmed := strings.TrimSpace(x)
		f, err := strconv.ParseFloat(trimmed, 64)
		if err == nil {
			return f, true
		}
		// Accept common timestamp strings and map to UnixNano for numeric casts.
		layouts := []string{
			"2006-01-02 15:04:05",
			"2006-01-02T15:04:05Z07:00",
			"2006-01-02",
			time.RFC3339,
		}
		for _, layout := range layouts {
			if t, perr := time.Parse(layout, trimmed); perr == nil {
				return float64(t.UnixNano()), true
			}
		}
		return 0, false
	case json.Number:
		f, err := x.Float64()
		if err != nil {
			return 0, false
		}
		return f, true
	case time.Time:
		return float64(x.UnixNano()), true
	default:
		return 0, false
	}
}

// ToInt64 attempts to coerce any numeric or string value to int64.
func ToInt64(v any) int64 {
	i, _ := ToInt64Safe(v)
	return i
}

// ToInt64Safe attempts to coerce any numeric or string value to int64, returning success.
func ToInt64Safe(v any) (int64, bool) {
	switch x := v.(type) {
	case int64:
		return x, true
	case int:
		return int64(x), true
	case int32:
		return int64(x), true
	case uint:
		return int64(x), true
	case uint64:
		return int64(x), true
	case uint32:
		return int64(x), true
	case float64:
		return int64(x), true
	case float32:
		return int64(x), true
	case string:
		i, err := strconv.ParseInt(strings.TrimSpace(x), 10, 64)
		if err != nil {
			return 0, false
		}
		return i, true
	case json.Number:
		i, err := x.Int64()
		if err != nil {
			return 0, false
		}
		return i, true
	case time.Time:
		return x.UnixNano(), true
	default:
		return 0, false
	}
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
		if a.Type() == b.Type() && a.Type().Comparable() && a.CanInterface() && b.CanInterface() {
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
