package utils

import "time"

// NowMillis returns the current UTC time as Unix epoch milliseconds (int64).
// Use this wherever a timestamp must be transmitted as a long (Java long / JSON number).
func NowMillis() int64 {
	return time.Now().UnixMilli()
}
 
// MillisToTime converts a Unix epoch milliseconds value back to a time.Time (UTC).
func MillisToTime(ms int64) time.Time {
	return time.UnixMilli(ms).UTC()
}
 
// MillisToRFC3339 converts epoch milliseconds to an RFC3339 string (UTC).
// Useful for human-readable logging only — do NOT use for inter-service payloads.
func MillisToRFC3339(ms int64) string {
	return MillisToTime(ms).Format(time.RFC3339Nano)
}