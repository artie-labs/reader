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
		assert.Error(t, err)
	}
	{
		// Expected
		layouts := []string{time.DateOnly}
		_, err := ParseExact("2021-01-01", layouts)
		assert.NoError(t, err)
	}
}
