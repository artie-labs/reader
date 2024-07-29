package writers

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/artie-labs/reader/lib"
	"github.com/artie-labs/reader/lib/iterator"
)

type DestinationWriter interface {
	Write(ctx context.Context, rawMsgs []lib.RawMessage) error
	OnComplete() error
}

type Writer struct {
	destinationWriter DestinationWriter
	logProgress       bool
}

func New(destinationWriter DestinationWriter, logProgress bool) Writer {
	return Writer{destinationWriter, logProgress}
}

// Write writes all the messages from an iterator to the destination.
func (w *Writer) Write(ctx context.Context, iter iterator.Iterator[[]lib.RawMessage]) (int, error) {
	start := time.Now()
	var count int
	for iter.HasNext() {
		iterStart := time.Now()
		msgs, err := iter.Next()
		if err != nil {
			return 0, fmt.Errorf("failed to iterate over messages: %w", err)
		} else if len(msgs) > 0 {
			if err = w.destinationWriter.Write(ctx, msgs); err != nil {
				return 0, fmt.Errorf("failed to write messages: %w", err)
			}

			// Is it a streaming iterator? if so, let's commit the offset.
			if streamingIter, isOk := iter.(iterator.StreamingIterator[[]lib.RawMessage]); isOk {
				streamingIter.CommitOffset()
			}

			count += len(msgs)
		}
		if w.logProgress {
			slog.Info("Write progress",
				slog.Int("totalSize", count),
				slog.Duration("totalDuration", time.Since(start)),
				slog.Int("batchSize", len(msgs)),
				slog.Duration("batchDuration", time.Since(iterStart)),
			)
		}
	}

	if err := w.destinationWriter.OnComplete(); err != nil {
		return 0, fmt.Errorf("failed running destination OnComplete: %w", err)
	}

	return count, nil
}
