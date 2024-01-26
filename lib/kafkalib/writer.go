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
)

const (
	baseJitterMs = 300
	maxJitterMs  = 5000
)

type BatchWriter struct {
	*kafka.Writer

	ctx context.Context
	cfg config.Kafka
}

func NewBatchWriter(ctx context.Context, cfg config.Kafka, writer *kafka.Writer) BatchWriter {
	return BatchWriter{writer, ctx, cfg}
}

func (w *BatchWriter) reload() error {
	if err := w.Writer.Close(); err != nil {
		return err
	}

	writer, err := NewWriter(w.ctx, w.cfg)
	if err != nil {
		return err
	}

	w.Writer = writer
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

func (w *BatchWriter) Write(rawMsgs []lib.RawMessage) error {
	msgs, err := buildKafkaMessages(&w.cfg, rawMsgs)
	if err != nil {
		return fmt.Errorf("failed to build to kafka messages: %w", err)
	}

	chunkSize := w.cfg.GetPublishSize()

	b := NewBatch(msgs, chunkSize)
	if batchErr := b.IsValid(); batchErr != nil {
		if IsBatchEmptyErr(batchErr) {
			return nil
		}

		return fmt.Errorf("batch is not valid: %w", batchErr)
	}

	for b.HasNext() {
		var kafkaErr error
		chunk := b.NextChunk()
		for attempts := 0; attempts < 10; attempts++ {
			kafkaErr = w.WriteMessages(w.ctx, chunk...)
			if kafkaErr == nil {
				break
			}

			if IsExceedMaxMessageBytesErr(kafkaErr) {
				slog.Info("Skipping this chunk since the batch exceeded the server")
				kafkaErr = nil
				break
			}

			if RetryableError(kafkaErr) {
				if reloadErr := w.reload(); reloadErr != nil {
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
