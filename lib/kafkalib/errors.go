package kafkalib

import (
	"errors"
	"github.com/segmentio/kafka-go"
)

// isRetryableError - returns true if the error is retryable
// If it's retryable, you need to reload the Kafka client.
func isRetryableError(err error) bool {
	return err != nil && errors.Is(err, kafka.TopicAuthorizationFailed)
}
