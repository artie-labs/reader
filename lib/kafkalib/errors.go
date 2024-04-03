package kafkalib

import (
	"context"
	"errors"
	"github.com/segmentio/kafka-go"
	"strings"
)

func isExceedMaxMessageBytesErr(err error) bool {
	return err != nil && strings.Contains(err.Error(),
		"Message Size Too Large: the server has a configurable maximum message size to avoid unbounded memory allocation and the client attempted to produce a message larger than this maximum")
}

// isRetryableError - returns true if the error is retryable
// If it's retryable, you need to reload the Kafka client.
func isRetryableError(err error) bool {
	if err == nil {
		return false
	}

	retryableErrs := []error{
		context.DeadlineExceeded,
		kafka.TopicAuthorizationFailed,
	}

	for _, retryableErr := range retryableErrs {
		if errors.Is(err, retryableErr) {
			return true
		}
	}

	return false
}
