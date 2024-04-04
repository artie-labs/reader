package writer

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/artie-labs/reader/destinations"
	"github.com/artie-labs/reader/lib"
	"github.com/artie-labs/reader/lib/iterator"
)

type Writer struct {
	destination destinations.DestinationWriter
	logProgress bool
}

func New(destination destinations.DestinationWriter, logProgress bool) Writer {
	return Writer{destination, logProgress}
}

// Write writes all the messages from an iterator to the destination.
func (w *Writer) Write(ctx context.Context, iter iterator.Iterator[[]lib.RawMessage]) (int, error) {
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
		if w.logProgress {
			slog.Info("Write progress",
				slog.Duration("timing", time.Since(start)),
				slog.Int("batchSize", len(msgs)),
				slog.Int("total", count),
			)
		}
	}

	if err := w.destination.OnFinish(); err != nil {
		return 0, fmt.Errorf("failed running destination OnFinish: %w", err)
	}

	return count, nil
}
