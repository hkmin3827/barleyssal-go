package utils

import (
	"fmt"
	"time"
)

func FormatYYYYMMDD(t time.Time) string {
	return t.Format("20060102")
}

type ChartDateRange struct {
	StartDate string 
	EndDate   string
}

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

func BuildMinuteKey(t time.Time) string {
	return fmt.Sprintf("%04d%02d%02d%02d%02d",
		t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute())
}
