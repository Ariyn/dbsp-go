package op

import "time"

const defaultTTLCheckInterval = 5 * time.Second

func ttlCheckInterval(interval time.Duration) time.Duration {
	if interval > 0 {
		return interval
	}
	return defaultTTLCheckInterval
}

func shouldRunTTLCheck(next *time.Time, now time.Time, ttl time.Duration, interval time.Duration) bool {
	if ttl <= 0 || next == nil {
		return false
	}
	if !next.IsZero() && now.Before(*next) {
		return false
	}
	*next = now.Add(ttlCheckInterval(interval))
	return true
}
