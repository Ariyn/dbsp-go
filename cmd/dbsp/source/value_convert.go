package source

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/buger/jsonparser"
)

type fieldTypeKind uint8

const (
	fieldTypeUnknown fieldTypeKind = iota
	fieldTypeInt
	fieldTypeFloat
	fieldTypeBool
	fieldTypeJSON
	fieldTypeTimestamp
	fieldTypeString
)

func parseFieldTypeKind(colType string) fieldTypeKind {
	switch colType {
	case "int":
		return fieldTypeInt
	case "float":
		return fieldTypeFloat
	case "bool":
		return fieldTypeBool
	case "json":
		return fieldTypeJSON
	case "timestamp":
		return fieldTypeTimestamp
	case "string":
		return fieldTypeString
	default:
		return fieldTypeUnknown
	}
}

func parseValueByTypeBytes(raw []byte, valueType jsonparser.ValueType, colType string, timestampUnit string) (any, error) {
	return parseValueByFieldKind(raw, valueType, parseFieldTypeKind(colType), timestampUnit)
}

func parseValueByFieldKind(raw []byte, valueType jsonparser.ValueType, kind fieldTypeKind, timestampUnit string) (any, error) {
	switch kind {
	case fieldTypeInt:
		switch valueType {
		case jsonparser.Number:
			i, err := jsonparser.ParseInt(raw)
			if err != nil {
				f, ferr := jsonparser.ParseFloat(raw)
				if ferr != nil {
					return nil, err
				}
				return int(f), nil
			}
			return int(i), nil
		case jsonparser.String:
			return strconv.Atoi(string(bytes.TrimSpace(raw)))
		default:
			return nil, fmt.Errorf("expected int, got %s", valueType)
		}
	case fieldTypeFloat:
		switch valueType {
		case jsonparser.Number:
			return jsonparser.ParseFloat(raw)
		case jsonparser.String:
			return strconv.ParseFloat(string(bytes.TrimSpace(raw)), 64)
		default:
			return nil, fmt.Errorf("expected float, got %s", valueType)
		}
	case fieldTypeBool:
		switch valueType {
		case jsonparser.Boolean:
			return bytes.Equal(bytes.TrimSpace(raw), []byte("true")), nil
		case jsonparser.Number:
			f, err := jsonparser.ParseFloat(raw)
			if err != nil {
				return nil, err
			}
			if f == 1 {
				return true, nil
			}
			if f == 0 {
				return false, nil
			}
			return nil, fmt.Errorf("expected bool (0/1), got %v", f)
		case jsonparser.String:
			return strconv.ParseBool(string(bytes.TrimSpace(raw)))
		default:
			return nil, fmt.Errorf("expected bool, got %s", valueType)
		}
	case fieldTypeJSON:
		return parseJSONValueBytes(raw, valueType)
	case fieldTypeTimestamp:
		switch valueType {
		case jsonparser.String:
			trimmedBytes := bytes.TrimSpace(raw)
			if len(trimmedBytes) == 0 {
				return nil, fmt.Errorf("empty timestamp")
			}
			trimmed := string(trimmedBytes)
			if ts, err := time.Parse(time.RFC3339, trimmed); err == nil {
				return ts, nil
			}
			n, err := strconv.ParseInt(trimmed, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("invalid timestamp: %s", trimmed)
			}
			return parseTimestampValue(n, timestampUnit)
		case jsonparser.Number:
			n, err := jsonparser.ParseInt(raw)
			if err != nil {
				f, ferr := jsonparser.ParseFloat(raw)
				if ferr != nil {
					return nil, err
				}
				n = int64(f)
			}
			return parseTimestampValue(n, timestampUnit)
		default:
			return nil, fmt.Errorf("expected timestamp (string or number), got %s", valueType)
		}
	case fieldTypeString:
		return string(raw), nil
	default:
		return parseJSONValueBytes(raw, valueType)
	}
}

func parseJSONValueBytes(raw []byte, valueType jsonparser.ValueType) (any, error) {
	switch valueType {
	case jsonparser.String:
		return string(raw), nil
	case jsonparser.Number:
		if i, err := jsonparser.ParseInt(raw); err == nil {
			return i, nil
		}
		f, err := jsonparser.ParseFloat(raw)
		if err != nil {
			return nil, err
		}
		return f, nil
	case jsonparser.Boolean:
		return bytes.Equal(bytes.TrimSpace(raw), []byte("true")), nil
	case jsonparser.Null:
		return nil, nil
	case jsonparser.Object:
		out := make(map[string]any, 8)
		err := jsonparser.ObjectEach(raw, func(key []byte, value []byte, dataType jsonparser.ValueType, offset int) error {
			decoded, err := parseJSONValueBytes(value, dataType)
			if err != nil {
				return err
			}
			out[string(key)] = decoded
			return nil
		})
		if err != nil {
			return nil, err
		}
		return out, nil
	case jsonparser.Array:
		out := make([]any, 0, 4)
		var parseErr error
		_, err := jsonparser.ArrayEach(raw, func(value []byte, dataType jsonparser.ValueType, offset int, err error) {
			if parseErr != nil || err != nil {
				if err != nil {
					parseErr = err
				}
				return
			}
			decoded, derr := parseJSONValueBytes(value, dataType)
			if derr != nil {
				parseErr = derr
				return
			}
			out = append(out, decoded)
		})
		if parseErr != nil {
			return nil, parseErr
		}
		if err != nil {
			return nil, err
		}
		return out, nil
	default:
		return nil, fmt.Errorf("unsupported json value type %s", valueType)
	}
}

func parseValueByType(raw any, colType string, timestampUnit string) (any, error) {
	switch colType {
	case "int":
		switch val := raw.(type) {
		case int:
			return val, nil
		case int64:
			return int(val), nil
		case float64:
			return int(val), nil
		case json.Number:
			i, err := val.Int64()
			if err != nil {
				return nil, err
			}
			return int(i), nil
		case string:
			return strconv.Atoi(strings.TrimSpace(val))
		default:
			return nil, fmt.Errorf("expected int, got %T", raw)
		}
	case "float":
		switch val := raw.(type) {
		case float64:
			return val, nil
		case float32:
			return float64(val), nil
		case int:
			return float64(val), nil
		case int64:
			return float64(val), nil
		case json.Number:
			f, err := val.Float64()
			if err != nil {
				return nil, err
			}
			return f, nil
		case string:
			return strconv.ParseFloat(strings.TrimSpace(val), 64)
		default:
			return nil, fmt.Errorf("expected float, got %T", raw)
		}
	case "bool":
		switch val := raw.(type) {
		case bool:
			return val, nil
		case float64:
			if val == 1 {
				return true, nil
			} else if val == 0 {
				return false, nil
			}
			return nil, fmt.Errorf("expected bool (0/1), got %v", val)
		case json.Number:
			f, err := val.Float64()
			if err != nil {
				return nil, err
			}
			if f == 1 {
				return true, nil
			} else if f == 0 {
				return false, nil
			}
			return nil, fmt.Errorf("expected bool (0/1), got %v", f)
		case string:
			return strconv.ParseBool(strings.TrimSpace(val))
		default:
			return nil, fmt.Errorf("expected bool, got %T", raw)
		}
	case "json":
		switch val := raw.(type) {
		case string:
			var out any
			if err := json.Unmarshal([]byte(val), &out); err != nil {
				return nil, err
			}
			return out, nil
		default:
			return raw, nil
		}
	case "timestamp":
		switch val := raw.(type) {
		case time.Time:
			return val, nil
		case string:
			trimmed := strings.TrimSpace(val)
			if trimmed == "" {
				return nil, fmt.Errorf("empty timestamp")
			}
			if ts, err := time.Parse(time.RFC3339, trimmed); err == nil {
				return ts, nil
			}
			if n, err := strconv.ParseInt(trimmed, 10, 64); err == nil {
				return parseTimestampValue(n, timestampUnit)
			}
			return nil, fmt.Errorf("invalid timestamp: %s", trimmed)
		case float64:
			return parseTimestampValue(int64(val), timestampUnit)
		case int64:
			return parseTimestampValue(val, timestampUnit)
		case int:
			return parseTimestampValue(int64(val), timestampUnit)
		case json.Number:
			i, err := val.Int64()
			if err != nil {
				return nil, err
			}
			return parseTimestampValue(i, timestampUnit)
		default:
			return nil, fmt.Errorf("expected timestamp (string or number), got %T", raw)
		}
	case "string":
		return fmt.Sprintf("%v", raw), nil
	default:
		return raw, nil
	}
}

func parseTimestampValue(val int64, unit string) (time.Time, error) {
	resolved := unit
	if resolved == "" || resolved == "auto" {
		switch {
		case val > 1e16:
			resolved = "ns"
		case val > 1e14:
			resolved = "us"
		case val > 1e11:
			resolved = "ms"
		default:
			resolved = "s"
		}
	}

	switch resolved {
	case "s":
		return time.Unix(val, 0), nil
	case "ms":
		return time.Unix(0, val*1e6), nil
	case "us":
		return time.Unix(0, val*1e3), nil
	case "ns":
		return time.Unix(0, val), nil
	default:
		return time.Unix(val, 0), nil
	}
}
