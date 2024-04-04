package kafkalib

import (
	"errors"
	"log/slog"
	"strings"

	"github.com/segmentio/kafka-go"
)

func isExceedMaxMessageBytesErr(err error) bool {
	if err == nil {
		return false
	}

	if errors.Is(err, kafka.MessageSizeTooLarge) {
		return true
	}

	if strings.Contains(err.Error(),
		"Message Size Too Large: the server has a configurable maximum message size to avoid unbounded memory allocation and the client attempted to produce a message larger than this maximum") {
		// TODO: Remove this if we don't see it in the logs
		slog.Error("Matched 'Message Size Too Large' error but not kafka.MessageSizeTooLarge")
		return true
	}

	return false
}

// isRetryableError - returns true if the error is retryable
// If it's retryable, you need to reload the Kafka client.
func isRetryableError(err error) bool {
	if err == nil {
		return false
	}

	retryableErrs := []error{
		kafka.TopicAuthorizationFailed,
	}

	for _, retryableErr := range retryableErrs {
		if errors.Is(err, retryableErr) {
			return true
		}
	}

	return false
}
