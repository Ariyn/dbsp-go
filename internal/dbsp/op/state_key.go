package op

import (
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ariyn/dbsp/internal/dbsp/types"
)

type tupleSchemaSignature struct {
	count    int
	totalLen int
	sum      uint64
	xor      uint64
}

var tupleSchemaColumnsCache sync.Map

func stableAnyKey(key any) string {
	if fast, ok := stableAnyKeyFast(key); ok {
		return fast
	}
	b, err := json.Marshal(key)
	if err == nil {
		return "j:" + string(b)
	}
	return "x:" + fmt.Sprintf("%#v", key)
}

func decodeAnyKey(encoded string) (any, error) {
	if len(encoded) >= 2 && encoded[1] == ':' {
		switch encoded[:2] {
		case "n:":
			return nil, nil
		case "s:":
			return encoded[2:], nil
		case "b:":
			return strconv.ParseBool(encoded[2:])
		case "i:":
			return strconv.ParseInt(encoded[2:], 10, 64)
		case "u:":
			return strconv.ParseUint(encoded[2:], 10, 64)
		case "f:":
			return strconv.ParseFloat(encoded[2:], 64)
		case "t:":
			return time.Parse(time.RFC3339Nano, encoded[2:])
		case "j:":
			var out any
			if err := json.Unmarshal([]byte(encoded[2:]), &out); err != nil {
				return encoded[2:], nil
			}
			return out, nil
		case "m:", "x:":
			return encoded[2:], nil
		}
	}
	var out any
	if err := json.Unmarshal([]byte(encoded), &out); err != nil {
		return encoded, nil
	}
	return out, nil
}

func stableAnyKeyFast(key any) (string, bool) {
	var buf [32]byte
	switch v := key.(type) {
	case nil:
		return "n:", true
	case string:
		return "s:" + v, true
	case bool:
		b := append(buf[:0], "b:"...)
		return string(strconv.AppendBool(b, v)), true
	case int:
		b := append(buf[:0], "i:"...)
		return string(strconv.AppendInt(b, int64(v), 10)), true
	case int8:
		b := append(buf[:0], "i:"...)
		return string(strconv.AppendInt(b, int64(v), 10)), true
	case int16:
		b := append(buf[:0], "i:"...)
		return string(strconv.AppendInt(b, int64(v), 10)), true
	case int32:
		b := append(buf[:0], "i:"...)
		return string(strconv.AppendInt(b, int64(v), 10)), true
	case int64:
		b := append(buf[:0], "i:"...)
		return string(strconv.AppendInt(b, v, 10)), true
	case uint:
		b := append(buf[:0], "u:"...)
		return string(strconv.AppendUint(b, uint64(v), 10)), true
	case uint8:
		b := append(buf[:0], "u:"...)
		return string(strconv.AppendUint(b, uint64(v), 10)), true
	case uint16:
		b := append(buf[:0], "u:"...)
		return string(strconv.AppendUint(b, uint64(v), 10)), true
	case uint32:
		b := append(buf[:0], "u:"...)
		return string(strconv.AppendUint(b, uint64(v), 10)), true
	case uint64:
		b := append(buf[:0], "u:"...)
		return string(strconv.AppendUint(b, v, 10)), true
	case float32:
		b := append(buf[:0], "f:"...)
		return string(strconv.AppendFloat(b, float64(v), 'g', -1, 32)), true
	case float64:
		b := append(buf[:0], "f:"...)
		return string(strconv.AppendFloat(b, v, 'g', -1, 64)), true
	case time.Time:
		b := append(buf[:0], "t:"...)
		return string(v.UTC().AppendFormat(b, time.RFC3339Nano)), true
	case types.Tuple:
		return "m:" + stableTupleKeyCanonical(v), true
	case map[string]any:
		return "m:" + stableTupleKeyCanonical(types.Tuple(v)), true
	default:
		return "", false
	}
}

func compactAnyOrderKey(key any) string {
	var buf [32]byte
	switch v := key.(type) {
	case nil:
		return "n:"
	case string:
		return "s:" + v
	case bool:
		b := append(buf[:0], "b:"...)
		return string(strconv.AppendBool(b, v))
	case int:
		b := append(buf[:0], "i:"...)
		return string(strconv.AppendInt(b, int64(v), 10))
	case int8:
		b := append(buf[:0], "i:"...)
		return string(strconv.AppendInt(b, int64(v), 10))
	case int16:
		b := append(buf[:0], "i:"...)
		return string(strconv.AppendInt(b, int64(v), 10))
	case int32:
		b := append(buf[:0], "i:"...)
		return string(strconv.AppendInt(b, int64(v), 10))
	case int64:
		b := append(buf[:0], "i:"...)
		return string(strconv.AppendInt(b, v, 10))
	case uint:
		b := append(buf[:0], "u:"...)
		return string(strconv.AppendUint(b, uint64(v), 10))
	case uint8:
		b := append(buf[:0], "u:"...)
		return string(strconv.AppendUint(b, uint64(v), 10))
	case uint16:
		b := append(buf[:0], "u:"...)
		return string(strconv.AppendUint(b, uint64(v), 10))
	case uint32:
		b := append(buf[:0], "u:"...)
		return string(strconv.AppendUint(b, uint64(v), 10))
	case uint64:
		b := append(buf[:0], "u:"...)
		return string(strconv.AppendUint(b, v, 10))
	case float32:
		b := append(buf[:0], "f:"...)
		return string(strconv.AppendFloat(b, float64(v), 'g', -1, 32))
	case float64:
		b := append(buf[:0], "f:"...)
		return string(strconv.AppendFloat(b, v, 'g', -1, 64))
	case time.Time:
		b := append(buf[:0], "t:"...)
		return string(v.UTC().AppendFormat(b, time.RFC3339Nano))
	case types.Tuple:
		return "h:" + stableTupleOrderHashHex(v)
	case *types.PackedTuple:
		return "h:" + stablePackedTupleOrderHashHex(v)
	case map[string]any:
		return "h:" + stableTupleOrderHashHex(types.Tuple(v))
	default:
		if fast, ok := stableAnyKeyFast(key); ok {
			return fast
		}
		return stableAnyKey(key)
	}
}

func stableTupleOrderHashHex(t types.Tuple) string {
	var raw [8]byte
	var encoded [16]byte
	binary.BigEndian.PutUint64(raw[:], stableTupleOrderHash(t))
	hex.Encode(encoded[:], raw[:])
	return string(encoded[:])
}

func stablePackedTupleOrderHashHex(p *types.PackedTuple) string {
	var raw [8]byte
	var encoded [16]byte
	binary.BigEndian.PutUint64(raw[:], stablePackedTupleOrderHash(p))
	hex.Encode(encoded[:], raw[:])
	return string(encoded[:])
}

func stableTupleOrderHash(t types.Tuple) uint64 {
	if t == nil {
		return 0
	}
	var columns []string
	if cols, ok := loadCachedTupleColumns(t); ok {
		columns = cols
	} else {
		columns = sortedTupleColumns(t)
		tupleSchemaColumnsCache.Store(tupleSchemaSignatureForTuple(t), columns)
	}
	hash := uint64(1469598103934665603)
	for _, col := range columns {
		hashStableBytes(&hash, []byte(col))
		hashStableByte(&hash, '=')
		hashStableValue(&hash, t[col])
		hashStableByte(&hash, '|')
	}
	return hash
}

func stablePackedTupleOrderHash(p *types.PackedTuple) uint64 {
	if p == nil {
		return 0
	}
	hash := uint64(1469598103934665603)
	if p.Schema != nil {
		for idx, col := range p.Schema.Columns {
			hashStableBytes(&hash, []byte(col))
			hashStableByte(&hash, '=')
			present := idx < len(p.Present) && p.Present[idx]
			hashStableByte(&hash, '0')
			if present {
				hash ^= 1
			}
			hash *= 1099511628211
			if idx < len(p.Values) {
				hashStableValue(&hash, p.Values[idx])
			} else {
				hashStableValue(&hash, nil)
			}
			hashStableByte(&hash, '|')
		}
	}
	if len(p.Extras) > 0 {
		extraCols := make([]string, 0, len(p.Extras))
		for col := range p.Extras {
			extraCols = append(extraCols, col)
		}
		sort.Strings(extraCols)
		for _, col := range extraCols {
			hashStableBytes(&hash, []byte(col))
			hashStableByte(&hash, '=')
			hashStableValue(&hash, p.Extras[col])
			hashStableByte(&hash, '|')
		}
	}
	return hash
}

func stableTupleKeyCanonical(t types.Tuple) string {
	if t == nil {
		return ""
	}
	if cols, ok := loadCachedTupleColumns(t); ok {
		return stableTupleKeyForColumns(t, cols)
	}
	keys := sortedTupleColumns(t)
	tupleSchemaColumnsCache.Store(tupleSchemaSignatureForTuple(t), keys)
	return stableTupleKeyForColumns(t, keys)
}

func stableTupleKeyForColumns(t types.Tuple, columns []string) string {
	if t == nil || len(columns) == 0 {
		return ""
	}
	var b strings.Builder
	b.Grow(estimateStableTupleKeyCapacity(columns))
	for idx, col := range columns {
		if idx > 0 {
			b.WriteByte('|')
		}
		b.WriteString(col)
		b.WriteByte('=')
		writeStableValue(&b, t[col])
	}
	return b.String()
}

func writeStableValue(b *strings.Builder, value any) {
	var buf [32]byte
	switch v := value.(type) {
	case nil:
		b.WriteString("null")
	case string:
		b.WriteString("s:")
		b.WriteString(v)
	case bool:
		b.WriteString("b:")
		b.Write(strconv.AppendBool(buf[:0], v))
	case int:
		b.WriteString("i:")
		b.Write(strconv.AppendInt(buf[:0], int64(v), 10))
	case int8:
		b.WriteString("i:")
		b.Write(strconv.AppendInt(buf[:0], int64(v), 10))
	case int16:
		b.WriteString("i:")
		b.Write(strconv.AppendInt(buf[:0], int64(v), 10))
	case int32:
		b.WriteString("i:")
		b.Write(strconv.AppendInt(buf[:0], int64(v), 10))
	case int64:
		b.WriteString("i:")
		b.Write(strconv.AppendInt(buf[:0], v, 10))
	case uint:
		b.WriteString("u:")
		b.Write(strconv.AppendUint(buf[:0], uint64(v), 10))
	case uint8:
		b.WriteString("u:")
		b.Write(strconv.AppendUint(buf[:0], uint64(v), 10))
	case uint16:
		b.WriteString("u:")
		b.Write(strconv.AppendUint(buf[:0], uint64(v), 10))
	case uint32:
		b.WriteString("u:")
		b.Write(strconv.AppendUint(buf[:0], uint64(v), 10))
	case uint64:
		b.WriteString("u:")
		b.Write(strconv.AppendUint(buf[:0], v, 10))
	case float32:
		b.WriteString("f:")
		b.Write(strconv.AppendFloat(buf[:0], float64(v), 'g', -1, 32))
	case float64:
		b.WriteString("f:")
		b.Write(strconv.AppendFloat(buf[:0], v, 'g', -1, 64))
	case time.Time:
		b.WriteString("t:")
		b.Write(v.UTC().AppendFormat(buf[:0], time.RFC3339Nano))
	default:
		b.WriteString("x:")
		b.WriteString(fmt.Sprintf("%#v", value))
	}
}

func loadCachedTupleColumns(t types.Tuple) ([]string, bool) {
	sig := tupleSchemaSignatureForTuple(t)
	if cached, ok := tupleSchemaColumnsCache.Load(sig); ok {
		cols := cached.([]string)
		if tupleHasColumns(t, cols) {
			return cols, true
		}
	}
	return nil, false
}

func tupleSchemaSignatureForTuple(t types.Tuple) tupleSchemaSignature {
	sig := tupleSchemaSignature{count: len(t)}
	for key := range t {
		hash := hashTupleColumnName(key)
		sig.totalLen += len(key)
		sig.sum += hash
		sig.xor ^= hash
	}
	return sig
}

func tupleHasColumns(t types.Tuple, columns []string) bool {
	if len(t) != len(columns) {
		return false
	}
	for _, col := range columns {
		if _, ok := t[col]; !ok {
			return false
		}
	}
	return true
}

func sortedTupleColumns(t types.Tuple) []string {
	keys := make([]string, 0, len(t))
	for key := range t {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func estimateStableTupleKeyCapacity(columns []string) int {
	capacity := 0
	for _, col := range columns {
		capacity += len(col) + 12
	}
	return capacity
}

func hashTupleColumnName(name string) uint64 {
	const (
		offset64 = 1469598103934665603
		prime64  = 1099511628211
	)
	hash := uint64(offset64)
	for i := 0; i < len(name); i++ {
		hash ^= uint64(name[i])
		hash *= prime64
	}
	return hash
}

func hashStableByte(hash *uint64, b byte) {
	*hash ^= uint64(b)
	*hash *= 1099511628211
}

func hashStableBytes(hash *uint64, data []byte) {
	for _, b := range data {
		hashStableByte(hash, b)
	}
}

func hashStableValue(hash *uint64, value any) {
	switch v := value.(type) {
	case nil:
		hashStableBytes(hash, []byte("null"))
	case string:
		hashStableBytes(hash, []byte("s:"))
		hashStableBytes(hash, []byte(v))
	case bool:
		hashStableBytes(hash, []byte("b:"))
		if v {
			hashStableBytes(hash, []byte("true"))
		} else {
			hashStableBytes(hash, []byte("false"))
		}
	case int:
		hashStableInt64(hash, int64(v))
	case int8:
		hashStableInt64(hash, int64(v))
	case int16:
		hashStableInt64(hash, int64(v))
	case int32:
		hashStableInt64(hash, int64(v))
	case int64:
		hashStableInt64(hash, v)
	case uint:
		hashStableUint64(hash, uint64(v))
	case uint8:
		hashStableUint64(hash, uint64(v))
	case uint16:
		hashStableUint64(hash, uint64(v))
	case uint32:
		hashStableUint64(hash, uint64(v))
	case uint64:
		hashStableUint64(hash, v)
	case float32:
		hashStableUint64(hash, uint64(math.Float32bits(v)))
	case float64:
		hashStableUint64(hash, math.Float64bits(v))
	case time.Time:
		hashStableInt64(hash, v.UTC().UnixNano())
	case types.Tuple:
		hashStableBytes(hash, []byte("m:"))
		hashStableUint64(hash, stableTupleOrderHash(v))
	case map[string]any:
		hashStableBytes(hash, []byte("m:"))
		hashStableUint64(hash, stableTupleOrderHash(types.Tuple(v)))
	default:
		hashStableBytes(hash, []byte("x:"))
		hashStableBytes(hash, []byte(fmt.Sprintf("%#v", value)))
	}
}

func hashStableInt64(hash *uint64, value int64) {
	hashStableBytes(hash, []byte("i:"))
	var raw [8]byte
	binary.BigEndian.PutUint64(raw[:], uint64(value))
	hashStableBytes(hash, raw[:])
}

func hashStableUint64(hash *uint64, value uint64) {
	hashStableBytes(hash, []byte("u:"))
	var raw [8]byte
	binary.BigEndian.PutUint64(raw[:], value)
	hashStableBytes(hash, raw[:])
}
