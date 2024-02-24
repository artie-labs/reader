package timeutil

import "time"

// ConvertTimeToString - will check if the type is time.Time, if so, it will return it in a string format
// Else it will not doing anything. This is a special case for our row based pagination.
func ConvertTimeToString(val any) any {
	timeVal, isTime := val.(time.Time)
	if isTime {
		return timeVal.Format(time.RFC3339)
	}

	return val
}
