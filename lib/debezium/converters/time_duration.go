package converters

import "time"

func getTimeDuration(timeVal time.Time, timeDuration time.Duration) int64 {
	hours := time.Duration(timeVal.Hour()) * time.Hour
	minutes := time.Duration(timeVal.Minute()) * time.Minute
	seconds := time.Duration(timeVal.Second()) * time.Second
	ns := time.Duration(timeVal.Nanosecond())
	return int64((hours + minutes + seconds + ns) / timeDuration)
}
