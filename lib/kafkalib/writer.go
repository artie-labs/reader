package kafkalib

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/artie-labs/transfer/lib/size"
	"github.com/segmentio/kafka-go"

	"github.com/artie-labs/reader/config"
	"github.com/artie-labs/reader/lib"
	"github.com/artie-labs/reader/lib/iterator"
)

const (
	baseJitterMs = 300
	maxJitterMs  = 5000
)

type BatchWriter struct {
	writer *kafka.Writer
	cfg    config.Kafka
}

func NewBatchWriter(ctx context.Context, cfg config.Kafka, writer *kafka.Writer) BatchWriter {
	return BatchWriter{writer, cfg}
}

func (w *BatchWriter) reload(ctx context.Context) error {
	if err := w.writer.Close(); err != nil {
		return err
	}

	writer, err := NewWriter(ctx, w.cfg)
	if err != nil {
		return err
	}

	w.writer = writer
	return nil
}

func buildKafkaMessages(cfg *config.Kafka, msgs []lib.RawMessage) ([]kafka.Message, error) {
	result := make([]kafka.Message, len(msgs))
	for i, msg := range msgs {
		topic := fmt.Sprintf("%s.%s", cfg.TopicPrefix, msg.TopicSuffix)
		kMsg, err := NewMessage(topic, msg.PartitionKey, msg.Payload)
		if err != nil {
			return nil, err
		}
		result[i] = kMsg
	}
	return result, nil
}

func (w *BatchWriter) WriteRawMessages(ctx context.Context, rawMsgs []lib.RawMessage) error {
	msgs, err := buildKafkaMessages(&w.cfg, rawMsgs)
	if err != nil {
		return fmt.Errorf("failed to build to kafka messages: %w", err)
	}
	return w.WriteMessages(ctx, msgs)
}

func (w *BatchWriter) WriteMessages(ctx context.Context, msgs []kafka.Message) error {
	chunkSize := w.cfg.GetPublishSize()
	if chunkSize < 1 {
		return fmt.Errorf("chunk size is too small")
	}

	if len(msgs) == 0 {
		return nil
	}

	iter := iterator.NewBatchIterator(msgs, int(chunkSize))
	for iter.HasNext() {
		var kafkaErr error
		chunk := iter.Next()
		for attempts := 0; attempts < 10; attempts++ {
			kafkaErr = w.writer.WriteMessages(ctx, chunk...)
			if kafkaErr == nil {
				break
			}

			if IsExceedMaxMessageBytesErr(kafkaErr) {
				slog.Info("Skipping this chunk since the batch exceeded the server")
				kafkaErr = nil
				break
			}

			if RetryableError(kafkaErr) {
				if reloadErr := w.reload(ctx); reloadErr != nil {
					slog.Warn("Failed to reload kafka writer", slog.Any("err", reloadErr))
				}
			} else {
				sleepMs := lib.JitterMs(baseJitterMs, maxJitterMs, attempts)
				slog.Info("Failed to publish to kafka",
					slog.Any("err", kafkaErr),
					slog.Int("attempts", attempts),
					slog.Int("sleepMs", sleepMs),
				)
				time.Sleep(time.Duration(sleepMs) * time.Millisecond)
			}
		}

		if kafkaErr != nil {
			return fmt.Errorf("failed to write message: %w, approxSize: %d", kafkaErr, size.GetApproxSize(chunk))
		}
	}
	return nil
}

type messageIterator interface {
	HasNext() bool
	Next() ([]lib.RawMessage, error)
}

func (w *BatchWriter) WriteIterator(ctx context.Context, iter messageIterator) (int, error) {
	start := time.Now()
	var count int
	for iter.HasNext() {
		msgs, err := iter.Next()
		if err != nil {
			return 0, fmt.Errorf("failed to iterate over messages, err: %w", err)

		} else if len(msgs) > 0 {
			if err = w.WriteRawMessages(ctx, msgs); err != nil {
				return 0, fmt.Errorf("failed to write messages to kafka, err: %w", err)
			}
			count += len(msgs)
			slog.Info("Scanning progress", slog.Duration("timing", time.Since(start)), slog.Int("count", count))
		}
	}
	return count, nil
}
