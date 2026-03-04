package source

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"
)

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
