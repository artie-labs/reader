package writer

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/artie-labs/reader/destinations"
	"github.com/artie-labs/reader/lib"
)

type RawMessageIterator interface {
	HasNext() bool
	Next() ([]lib.RawMessage, error)
}

type Writer struct {
	destination destinations.Destination
}

func New(destination destinations.Destination) Writer {
	return Writer{destination}
}

// Write writes all the messages from an iterator to the destination.
func (w *Writer) Write(ctx context.Context, iter RawMessageIterator) (int, error) {
	start := time.Now()
	var count int
	for iter.HasNext() {
		msgs, err := iter.Next()
		if err != nil {
			return 0, fmt.Errorf("failed to iterate over messages: %w", err)

		} else if len(msgs) > 0 {
			if err = w.destination.WriteRawMessages(ctx, msgs); err != nil {
				return 0, fmt.Errorf("failed to write messages: %w", err)
			}
			count += len(msgs)
		}
		slog.Info("Write progress",
			slog.Duration("timing", time.Since(start)),
			slog.Int("batchSize", len(msgs)),
			slog.Int("total", count),
		)
	}
	return count, nil
}
