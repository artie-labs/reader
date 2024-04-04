package kafkalib

import (
	"context"
	"fmt"
	"testing"

	"github.com/segmentio/kafka-go"

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
		{
			err:      kafka.TopicAuthorizationFailed,
			expected: false,
		},
		{
			err:      kafka.MessageSizeTooLarge,
			expected: true,
		},
	}

	for _, tc := range tcs {
		actual := isExceedMaxMessageBytesErr(tc.err)
		assert.Equal(t, tc.expected, actual, tc.err)
	}
}

func TestIsRetryableError(t *testing.T) {
	type _tc struct {
		err      error
		expected bool
	}

	tcs := []_tc{
		{
			err:      fmt.Errorf(""),
			expected: false,
		},
		{
			err:      nil,
			expected: false,
		},
		{
			err:      kafka.TopicAuthorizationFailed,
			expected: true,
		},
		{
			err:      context.DeadlineExceeded,
			expected: true,
		},
		{
			err:      kafka.MessageSizeTooLarge,
			expected: false,
		},
	}

	for _, tc := range tcs {
		actual := isRetryableError(tc.err)
		assert.Equal(t, tc.expected, actual, tc.err)
	}
}
