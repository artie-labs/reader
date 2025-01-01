package writers

import (
	"context"
	"fmt"
	"github.com/artie-labs/reader/lib/kafkalib"
	"log/slog"
	"time"

	"github.com/artie-labs/transfer/lib/typing/columns"

	"github.com/artie-labs/reader/lib/iterator"
	"github.com/artie-labs/reader/lib/logger"
)

type DestinationWriter interface {
	CreateTable(ctx context.Context, tableName string, columns []columns.Column) error
	Write(ctx context.Context, rawMsgs []kafkalib.Message) error
	OnComplete(ctx context.Context) error
}

type Writer struct {
	destinationWriter DestinationWriter
	logProgress       bool
}

func New(destinationWriter DestinationWriter, logProgress bool) Writer {
	return Writer{destinationWriter: destinationWriter, logProgress: logProgress}
}

// Write writes all the messages from an iterator to the destination.
func (w *Writer) Write(ctx context.Context, iter iterator.Iterator[[]kafkalib.Message]) (int, error) {
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
			if streamingIter, isOk := iter.(iterator.StreamingIterator[[]kafkalib.Message]); isOk {
				if err = streamingIter.CommitOffset(); err != nil {
					logger.Panic("Failed to commit offset", slog.Any("err", err))
				}
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

	// Only run [OnComplete] if we wrote messages out. Otherwise, primary keys may not be loaded.
	if count > 0 {
		if err := w.destinationWriter.OnComplete(ctx); err != nil {
			return 0, fmt.Errorf("failed running destination OnComplete: %w", err)
		}
	}

	return count, nil
}

func (w *Writer) OnComplete(ctx context.Context) error {
	if err := w.destinationWriter.OnComplete(ctx); err != nil {
		return fmt.Errorf("failed running destination OnComplete: %w", err)
	}

	return nil
}

func (w *Writer) CreateTable(ctx context.Context, tableName string, columns []columns.Column) error {
	if err := w.destinationWriter.CreateTable(ctx, tableName, columns); err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	return nil
}
