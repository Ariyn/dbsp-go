package op

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
)

func stableAnyKey(key any) string {
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
	var out any
	if err := json.Unmarshal(raw, &out); err != nil {
		return string(raw), nil
	}
	return out, nil
}
