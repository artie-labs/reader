package kafkalib

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIsExceedMaxMessageBytesErr(t *testing.T) {
	type _tc struct {
		err      error
		expected bool
	}

	tcs := []_tc{
		{
			err: fmt.Errorf(""),
		},
		{
			err: nil,
		},
		{
			err:      fmt.Errorf("Message Size Too Large: the server has a configurable maximum message size to avoid unbounded memory allocation and the client attempted to produce a message larger than this maximum, bytes: 1223213213"),
			expected: true,
		},
	}

	for _, tc := range tcs {
		actual := IsExceedMaxMessageBytesErr(tc.err)
		assert.Equal(t, tc.expected, actual, tc.err)
	}
}
