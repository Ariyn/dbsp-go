package pipeline

import (
	"time"

	"github.com/ariyn/dbsp/internal/dbsp/types"
)

func ParseTTL(s string) (time.Duration, error) {
	return types.ParseFlexibleDuration(s)
}
