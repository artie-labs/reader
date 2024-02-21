package utils

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWithJitteredRetries(t *testing.T) {
	{
		// 0 max attempts - still runs
		calls := 0
		value, err := WithJitteredRetries(0, 0, 0, func(attempt int) (int, error) {
			calls++
			return 100, nil
		})
		assert.NoError(t, err)
		assert.Equal(t, value, 100)
		assert.Equal(t, calls, 1)
	}
	{
		// 1 max attempts - succeedes
		calls := 0
		value, err := WithJitteredRetries(0, 0, 1, func(attempt int) (int, error) {
			calls++
			return 100, nil
		})
		assert.NoError(t, err)
		assert.Equal(t, value, 100)
		assert.Equal(t, calls, 1)
	}
	{
		// 1 max attempts - fails
		calls := 0
		_, err := WithJitteredRetries(0, 0, 1, func(attempt int) (int, error) {
			calls++
			return 0, fmt.Errorf("oops I failed again")
		})
		assert.ErrorContains(t, err, "oops I failed again")
		assert.Equal(t, calls, 1)
	}
	{
		// 2 max attempts - first fails and second succeedes
		calls := 0
		value, err := WithJitteredRetries(1, 1, 2, func(attempt int) (int, error) {
			calls++
			if attempt == 0 {
				return 0, fmt.Errorf("oops I failed again")
			}
			return 100, nil
		})
		assert.NoError(t, err)
		assert.Equal(t, value, 100)
		assert.Equal(t, calls, 2)
	}
}
