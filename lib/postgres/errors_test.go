package postgres

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNoRowsError(t *testing.T) {
	type _tc struct {
		name     string
		err      error
		expected bool
	}

	tcs := []_tc{
		{
			name:     "Test Case 1: nil error",
			err:      nil,
			expected: false,
		},
		{
			name:     "Test Case 2: no rows error",
			err:      fmt.Errorf("sql: no rows in result set"),
			expected: true,
		},
		{
			name:     "Test Case 3: other error",
			err:      fmt.Errorf("other error"),
			expected: false,
		},
	}

	for _, tc := range tcs {
		actualData := NoRowsError(tc.err)
		assert.Equal(t, tc.expected, actualData, tc.name)
	}
}
