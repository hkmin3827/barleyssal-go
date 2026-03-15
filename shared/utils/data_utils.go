// Package utils provides shared utility functions.
package utils

import (
	"fmt"
	"time"
)

// FormatYYYYMMDD formats a time.Time as "YYYYMMDD".
func FormatYYYYMMDD(t time.Time) string {
	return t.Format("20060102")
}

// ChartDateRange holds the start/end date strings for a chart query.
type ChartDateRange struct {
	StartDate string // YYYYMMDD
	EndDate   string // YYYYMMDD
}

// GetChartDateRange returns the appropriate date range for the given period code.
//
//	D  → 5 months back (daily candles)
//	W  → 700 days back (weekly)
//	M  → 100 months back (monthly)
//	Y  → 100 years back (yearly)
func GetChartDateRange(period string) ChartDateRange {
	now := time.Now()
	end := now
	var start time.Time

	switch period {
	case "D":
		start = now.AddDate(0, -5, 0)
	case "W":
		start = now.AddDate(0, 0, -100*7)
	case "M":
		start = now.AddDate(0, -100, 0)
	case "Y":
		start = now.AddDate(-100, 0, 0)
	default:
		start = now.AddDate(0, -5, 0)
	}

	return ChartDateRange{
		StartDate: FormatYYYYMMDD(start),
		EndDate:   FormatYYYYMMDD(end),
	}
}

// BuildMinuteKey returns "YYYYMMDDHHmm" for the given time (mirrors buildMinuteKey in chartService.js).
func BuildMinuteKey(t time.Time) string {
	return fmt.Sprintf("%04d%02d%02d%02d%02d",
		t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute())
}
