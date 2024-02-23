package timeutil

import "time"

// ParseValue - will check if the type is time.Time, if so, it will return it in a string format
// Else it will not doing anything. This is a special case for our row based pagination.
func ParseValue(val any) any {
	timeVal, isTime := val.(time.Time)
	if isTime {
		return timeVal.Format(time.RFC3339)
	}

	return val
}
