package converters

import "time"

func getTimeDuration(timeVal time.Time, timeDuration time.Duration) any {
	hours := time.Duration(timeVal.Hour()) * time.Hour
	minutes := time.Duration(timeVal.Minute()) * time.Minute
	seconds := time.Duration(timeVal.Second()) * time.Second

	switch timeDuration {
	case time.Millisecond:
		return int32((hours + minutes + seconds) / timeDuration)
	default:
		return int64((hours + minutes + seconds) / timeDuration)
	}
}
