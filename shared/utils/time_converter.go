package utils

import "time"

// UTC time -> 밀리초
func NowMillis() int64 {
	return time.Now().UnixMilli()
}
 
//  밀리초 -> UTC time
func MillisToTime(ms int64) time.Time {
	return time.UnixMilli(ms).UTC()
}