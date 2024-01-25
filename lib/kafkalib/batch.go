package kafkalib

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/artie-labs/reader/lib/mtr"
	"github.com/artie-labs/transfer/lib/jitter"
	"github.com/segmentio/kafka-go"
)

const (
	MaxRetries   = 5
	RetryDelayMs = 250
)

var ErrEmptyBatch = fmt.Errorf("batch is empty")

type Batch struct {
	msgs        []kafka.Message
	chunkSize   uint
	iteratorIdx uint
}

func (b *Batch) IsValid() error {
	if len(b.msgs) == 0 {
		return ErrEmptyBatch
	}

	if b.chunkSize < 1 {
		return fmt.Errorf("chunk size is too small")
	}

	return nil
}

func NewBatch(messages []kafka.Message, chunkSize uint) *Batch {
	return &Batch{
		msgs:      messages,
		chunkSize: chunkSize,
	}
}

func (b *Batch) HasNext() bool {
	return uint(len(b.msgs)) > b.iteratorIdx
}

func (b *Batch) NextChunk() []kafka.Message {
	start := b.iteratorIdx
	b.iteratorIdx += b.chunkSize
	end := b.iteratorIdx

	if end > uint(len(b.msgs)) {
		end = uint(len(b.msgs))
	}

	if start > end {
		return nil
	}

	return b.msgs[start:end]
}

func (b *Batch) Publish(ctx context.Context) error {
	for b.HasNext() {
		var err error
		var count int64
		tags := map[string]string{
			"what": "error",
		}

		chunk := b.NextChunk()
		count = int64(len(chunk))

		for attempts := 0; attempts < MaxRetries; attempts++ {

			err = FromContext(ctx).WriteMessages(ctx, chunk...)
			if err == nil {
				tags["what"] = "success"
				break
			}

			sleepDuration := time.Duration(jitter.JitterMs(RetryDelayMs, attempts)) * time.Millisecond
			slog.Warn("Failed to publish message, jitter sleeping before retrying...",
				slog.Any("err", err),
				slog.Int("attempts", attempts),
				slog.Int("maxAttempts", MaxRetries),
			)
			time.Sleep(sleepDuration)
		}

		mtr.FromContext(ctx).Count("kafka.publish", count, tags)
		if err != nil {
			return err
		}
	}

	return nil
}
