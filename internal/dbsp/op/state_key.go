package op

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
)

func stableAnyKey(key any) string {
	// NOTE: This encoding is stable for persistence, but it is not guaranteed to
	// be type-preserving when decoded back into `any`.
	// For example, json.Unmarshal into an `any` decodes all numbers as float64.
	// If the in-memory key is int/int64, restoring from a backend may produce
	// float64 keys, causing map lookups to miss and state to diverge.
	b, err := json.Marshal(key)
	if err == nil {
		return base64.RawURLEncoding.EncodeToString(b)
	}
	return base64.RawURLEncoding.EncodeToString([]byte(fmt.Sprintf("%#v", key)))
}

func decodeAnyKey(encoded string) (any, error) {
	raw, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil {
		return nil, err
	}
	// NOTE: Decoding into an `any` uses the default encoding/json rules:
	// - numbers become float64
	// - objects become map[string]any
	// - arrays become []any
	// This is convenient but can break key equality with the original in-memory
	// keys used by operators (e.g., GroupAgg/WindowAgg/Join backends).
	var out any
	if err := json.Unmarshal(raw, &out); err != nil {
		return string(raw), nil
	}
	return out, nil
}
