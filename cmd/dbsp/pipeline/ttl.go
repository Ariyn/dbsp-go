package pipeline

import (
	"fmt"
	"time"

	"github.com/ariyn/dbsp/internal/dbsp/types"
)

func ParseTTL(s string) (time.Duration, error) {
	if s == "" {
		return 0, nil
	}
	if d, err := time.ParseDuration(s); err == nil {
		return d, nil
	}
	iv, err := types.ParseInterval(s)
	if err != nil {
		return 0, fmt.Errorf("invalid ttl %q: %w", s, err)
	}
	return time.Duration(iv.Millis) * time.Millisecond, nil
}
