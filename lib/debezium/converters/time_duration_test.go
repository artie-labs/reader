package converters

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestGetTimeDuration(t *testing.T) {
	// Test with time at midnight
	{
		timeVal := time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC)
		assert.Equal(t, int64(0), getTimeDuration(timeVal, time.Millisecond))
	}

	// Test with time including only hours
	{
		timeVal := time.Date(2021, 1, 1, 3, 0, 0, 0, time.UTC)
		assert.Equal(t, int64(10800000), getTimeDuration(timeVal, time.Millisecond))
		assert.Equal(t, int64(3), getTimeDuration(timeVal, time.Hour))
	}

	// Test with time including hours and minutes
	{
		timeVal := time.Date(2021, 1, 1, 1, 30, 0, 0, time.UTC)
		assert.Equal(t, int64(5400000), getTimeDuration(timeVal, time.Millisecond))
		assert.Equal(t, int64(90), getTimeDuration(timeVal, time.Minute))
	}

	// Test with time including hours, minutes, and seconds
	{
		timeVal := time.Date(2021, 1, 1, 2, 45, 30, 0, time.UTC)
		assert.Equal(t, int64(9930030), getTimeDuration(timeVal, time.Millisecond))
		assert.Equal(t, int64(165), getTimeDuration(timeVal, time.Minute))
		assert.Equal(t, int64(9930), getTimeDuration(timeVal, time.Second))
	}

	// Test with time including hours, minutes, seconds, and milliseconds
	{
		timeVal := time.Date(2021, 1, 1, 4, 20, 15, 500000000, time.UTC)
		assert.Equal(t, int64(15615015), getTimeDuration(timeVal, time.Millisecond))
		assert.Equal(t, int64(15615), getTimeDuration(timeVal, time.Second))
		assert.Equal(t, int64(260), getTimeDuration(timeVal, time.Minute))
	}
}
