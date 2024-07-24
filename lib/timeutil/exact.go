package timeutil

import (
	"fmt"
	"time"
)

func ParseExact(value string, layouts []string) (time.Time, error) {
	for _, layout := range layouts {
		// If the value is parsed successfully and the parsed value is the same as the original value, return the parsed value.
		parsed, err := time.Parse(layout, value)
		if err == nil && parsed.Format(layout) == value {
			return parsed, nil
		}
	}

	return time.Time{}, fmt.Errorf("failed to parse exact time value: %q", value)

}
