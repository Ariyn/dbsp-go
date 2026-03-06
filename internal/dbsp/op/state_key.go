package op

import (
	"encoding/json"
	"fmt"
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
	switch v := key.(type) {
	case nil:
		return "n:", true
	case string:
		return "s:" + v, true
	case bool:
		return "b:" + strconv.FormatBool(v), true
	case int:
		return "i:" + strconv.FormatInt(int64(v), 10), true
	case int8:
		return "i:" + strconv.FormatInt(int64(v), 10), true
	case int16:
		return "i:" + strconv.FormatInt(int64(v), 10), true
	case int32:
		return "i:" + strconv.FormatInt(int64(v), 10), true
	case int64:
		return "i:" + strconv.FormatInt(v, 10), true
	case uint:
		return "u:" + strconv.FormatUint(uint64(v), 10), true
	case uint8:
		return "u:" + strconv.FormatUint(uint64(v), 10), true
	case uint16:
		return "u:" + strconv.FormatUint(uint64(v), 10), true
	case uint32:
		return "u:" + strconv.FormatUint(uint64(v), 10), true
	case uint64:
		return "u:" + strconv.FormatUint(v, 10), true
	case float32:
		return "f:" + strconv.FormatFloat(float64(v), 'g', -1, 32), true
	case float64:
		return "f:" + strconv.FormatFloat(v, 'g', -1, 64), true
	case time.Time:
		return "t:" + v.UTC().Format(time.RFC3339Nano), true
	case types.Tuple:
		return "m:" + stableTupleKeyCanonical(v), true
	case map[string]any:
		return "m:" + stableTupleKeyCanonical(types.Tuple(v)), true
	default:
		return "", false
	}
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
	switch v := value.(type) {
	case nil:
		b.WriteString("null")
	case string:
		b.WriteString("s:")
		b.WriteString(v)
	case bool:
		b.WriteString("b:")
		b.WriteString(strconv.FormatBool(v))
	case int:
		b.WriteString("i:")
		b.WriteString(strconv.FormatInt(int64(v), 10))
	case int8:
		b.WriteString("i:")
		b.WriteString(strconv.FormatInt(int64(v), 10))
	case int16:
		b.WriteString("i:")
		b.WriteString(strconv.FormatInt(int64(v), 10))
	case int32:
		b.WriteString("i:")
		b.WriteString(strconv.FormatInt(int64(v), 10))
	case int64:
		b.WriteString("i:")
		b.WriteString(strconv.FormatInt(v, 10))
	case uint:
		b.WriteString("u:")
		b.WriteString(strconv.FormatUint(uint64(v), 10))
	case uint8:
		b.WriteString("u:")
		b.WriteString(strconv.FormatUint(uint64(v), 10))
	case uint16:
		b.WriteString("u:")
		b.WriteString(strconv.FormatUint(uint64(v), 10))
	case uint32:
		b.WriteString("u:")
		b.WriteString(strconv.FormatUint(uint64(v), 10))
	case uint64:
		b.WriteString("u:")
		b.WriteString(strconv.FormatUint(v, 10))
	case float32:
		b.WriteString("f:")
		b.WriteString(strconv.FormatFloat(float64(v), 'g', -1, 32))
	case float64:
		b.WriteString("f:")
		b.WriteString(strconv.FormatFloat(v, 'g', -1, 64))
	case time.Time:
		b.WriteString("t:")
		b.WriteString(v.UTC().Format(time.RFC3339Nano))
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
