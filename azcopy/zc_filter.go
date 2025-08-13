package azcopy

import "time"

// FormatAsUTC is inverse of parseISO8601 (and always uses the most detailed format)
func FormatAsUTC(t time.Time) string {
	return t.UTC().Format(time.RFC3339)
}
