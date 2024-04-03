package kafkalib

import "strings"

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

	retryableErrs := []string{
		"Topic Authorization Failed: the client is not authorized to access the requested topic",
		"i/o timeout",
	}

	for _, retryableErr := range retryableErrs {
		if strings.Contains(err.Error(), retryableErr) {
			return true
		}
	}

	return false
}
