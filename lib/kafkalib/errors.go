package kafkalib

import (
	"errors"
	"github.com/segmentio/kafka-go"
)

func isExceedMaxMessageBytesErr(err error) bool {
	var e kafka.MessageTooLargeError
	if err != nil && errors.As(err, &e) {
		return true
	}

	return false
}

// isRetryableError - returns true if the error is retryable
// If it's retryable, you need to reload the Kafka client.
func isRetryableError(err error) bool {
	return err != nil && errors.Is(err, kafka.TopicAuthorizationFailed)
}
