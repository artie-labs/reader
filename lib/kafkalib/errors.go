package kafkalib

import "strings"

func IsExceedMaxMessageBytesErr(err error) bool {
	return err != nil && strings.Contains(err.Error(),
		"Message Size Too Large: the server has a configurable maximum message size to avoid unbounded memory allocation and the client attempted to produce a message larger than this maximum")
}

func IsBatchEmptyErr(err error) bool {
	return err != nil && err.Error() == "batch is empty"
}

// RetryableError - returns true if the error is retryable
// If it's retryable, you need to reload the Kafka client.
func RetryableError(err error) bool {
	return err != nil && strings.Contains(err.Error(), "Topic Authorization Failed: the client is not authorized to access the requested topic")
}
