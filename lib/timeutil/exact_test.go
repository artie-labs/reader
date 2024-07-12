package timeutil

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestParseExact(t *testing.T) {
	{
		// Bad
		layouts := []string{time.TimeOnly}
		_, err := ParseExact("2021-01-01", layouts)
		assert.ErrorContains(t, err, `failed to parse exact time value: "2021-01-01"`)
	}
	{
		// Both should be bad.
		layouts := []string{time.DateTime, time.TimeOnly}
		_, err := ParseExact("2021-01-01", layouts)
		assert.ErrorContains(t, err, `failed to parse exact time value: "2021-01-01"`)
	}
	{
		// Good
		layouts := []string{time.DateTime, time.DateOnly, time.TimeOnly}
		_, err := ParseExact("2021-01-01", layouts)
		assert.NoError(t, err)
	}
}
