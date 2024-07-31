package throttler

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestThrottler(t *testing.T) {
	{
		_, err := NewThrottler(0)
		assert.ErrorContains(t, err, "Throttler limit should be greater than 0")
	}
	{
		throttler, err := NewThrottler(1)
		assert.NoError(t, err)
		assert.True(t, throttler.Allowed())
		throttler.Start()
		assert.False(t, throttler.Allowed())
		throttler.Done()
		assert.True(t, throttler.Allowed())
	}
	{
		throttler, err := NewThrottler(2)
		assert.NoError(t, err)
		assert.True(t, throttler.Allowed())
		throttler.Start()
		assert.True(t, throttler.Allowed())
		throttler.Start()
		assert.False(t, throttler.Allowed())
		throttler.Done()
		assert.True(t, throttler.Allowed())
	}
}
