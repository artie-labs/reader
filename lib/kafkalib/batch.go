package kafkalib

import (
	"context"
	"fmt"
	"github.com/artie-labs/reader/lib/logger"
	"github.com/artie-labs/reader/lib/mtr"
	"github.com/artie-labs/transfer/lib/jitter"
	"github.com/segmentio/kafka-go"
	"time"
)

const (
	MaxRetries   = 5
	RetryDelayMs = 250
)

var ErrEmptyBatch = fmt.Errorf("batch is empty")

type Batch struct {
	msgs        []kafka.Message
	chunkSize   int
	iteratorIdx int
}

func (b *Batch) IsValid() error {
	if len(b.msgs) == 0 {
		return ErrEmptyBatch
	}

	if b.chunkSize < 1 {
		return fmt.Errorf("chunk size is too small")
	}

	if b.iteratorIdx < 0 {
		return fmt.Errorf("iterator cannot be less than 0")
	}

	return nil
}

func NewBatch(messages []kafka.Message, chunkSize int) *Batch {
	return &Batch{
		msgs:      messages,
		chunkSize: chunkSize,
	}
}

func (b *Batch) HasNext() bool {
	return len(b.msgs) > b.iteratorIdx
}

func (b *Batch) NextChunk() []kafka.Message {
	start := b.iteratorIdx
	b.iteratorIdx += b.chunkSize
	end := b.iteratorIdx

	if end > len(b.msgs) {
		end = len(b.msgs)
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

		for attempts := 0; attempts < MaxRetries; attempts++ {
			chunk := b.NextChunk()
			count = int64(len(chunk))
			err = FromContext(ctx).WriteMessages(ctx, chunk...)
			if err == nil {
				tags["what"] = "success"
				break
			}

			sleepDuration := time.Duration(jitter.JitterMs(RetryDelayMs, attempts)) * time.Millisecond
			logger.FromContext(ctx).WithError(err).WithFields(map[string]interface{}{
				"attempts":    attempts,
				"maxAttempts": MaxRetries,
			}).Warn("failed to publish message, jitter sleeping before retrying...")
			time.Sleep(sleepDuration)
		}

		mtr.FromContext(ctx).Count("kafka.publish", count, tags)
		if err != nil {
			return err
		}
	}

	return nil
}
