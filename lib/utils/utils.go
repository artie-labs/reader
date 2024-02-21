package utils

import (
	"log/slog"
	"time"

	"github.com/artie-labs/transfer/lib/jitter"
)

func WithJitteredRetries[T any](baseMs, maxMs, maxAttempts int, f func(attempt int) (T, error)) (T, error) {
	maxAttempts = max(maxAttempts, 1)
	var result T
	var err error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		if attempt > 0 {
			sleepDuration := jitter.Jitter(baseMs, maxMs, attempt+1)
			slog.Info("An error occurred, retrying after delay...",
				slog.Duration("sleep", sleepDuration),
				slog.Any("attemptsLeft", maxAttempts-attempt),
				slog.Any("err", err),
			)
			time.Sleep(sleepDuration)
		}
		result, err = f(attempt)
		if err == nil {
			return result, nil
		}
	}
	return result, err
}
