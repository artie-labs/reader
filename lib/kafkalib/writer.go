package kafkalib

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/artie-labs/transfer/lib/jitter"
	"github.com/artie-labs/transfer/lib/size"
	"github.com/segmentio/kafka-go"

	"github.com/artie-labs/reader/config"
	"github.com/artie-labs/reader/lib"
	"github.com/artie-labs/reader/lib/iterator"
	"github.com/artie-labs/reader/lib/mtr"
)

const (
	baseJitterMs = 300
	maxJitterMs  = 5000
)

type BatchWriter struct {
	writer *kafka.Writer
	cfg    config.Kafka
	statsD mtr.Client
}

func NewBatchWriter(ctx context.Context, cfg config.Kafka, statsD mtr.Client) (*BatchWriter, error) {
	if cfg.TopicPrefix == "" {
		return nil, fmt.Errorf("kafka topic prefix cannot be empty")
	}

	writer, err := newWriter(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return &BatchWriter{writer, cfg, statsD}, nil
}

func (b *BatchWriter) reload(ctx context.Context) error {
	slog.Info("Reloading kafka writer")
	if err := b.writer.Close(); err != nil {
		return err
	}

	writer, err := newWriter(ctx, b.cfg)
	if err != nil {
		return err
	}

	b.writer = writer
	return nil
}

func (b *BatchWriter) buildKafkaMessages(rawMsgs []lib.RawMessage) ([]kafka.Message, error) {
	var kafkaMsgs []kafka.Message
	for _, rawMsg := range rawMsgs {
		topic := fmt.Sprintf("%s.%s", b.cfg.TopicPrefix, rawMsg.TopicSuffix)
		kafkaMsg, err := newMessage(topic, rawMsg.PartitionKey, rawMsg.GetPayload())
		if err != nil {
			return nil, err
		}

		kafkaMsgs = append(kafkaMsgs, kafkaMsg)
	}

	return kafkaMsgs, nil
}

func (b *BatchWriter) WriteRawMessages(ctx context.Context, rawMsgs []lib.RawMessage) error {
	kafkaMsgs, err := b.buildKafkaMessages(rawMsgs)
	if err != nil {
		return fmt.Errorf("failed to encode kafka messages: %w", err)
	}

	return b.WriteMessages(ctx, kafkaMsgs)
}

func (b *BatchWriter) WriteMessages(ctx context.Context, msgs []kafka.Message) error {
	chunkSize := b.cfg.GetPublishSize()
	if chunkSize < 1 {
		return fmt.Errorf("chunk size is too small")
	}

	if len(msgs) == 0 {
		return nil
	}

	iter := iterator.BatchIterator(msgs, int(chunkSize))
	for iter.HasNext() {
		tags := map[string]string{
			"what": "error",
		}

		var kafkaErr error
		chunk, err := iter.Next()
		if err != nil {
			return err
		}
		for attempts := 0; attempts < 10; attempts++ {
			if attempts > 0 {
				sleepDuration := jitter.Jitter(baseJitterMs, maxJitterMs, attempts-1)
				slog.Info("Failed to publish to kafka",
					slog.Any("err", kafkaErr),
					slog.Int("attempts", attempts),
					slog.Duration("sleep", sleepDuration),
				)
				time.Sleep(sleepDuration)

				if isRetryableError(kafkaErr) {
					if reloadErr := b.reload(ctx); reloadErr != nil {
						slog.Warn("Failed to reload kafka writer", slog.Any("err", reloadErr))
					}
				}
			}

			kafkaErr = b.writer.WriteMessages(ctx, chunk...)
			if kafkaErr == nil {
				tags["what"] = "success"
				break
			}

			if isExceedMaxMessageBytesErr(kafkaErr) {
				slog.Info("Skipping this chunk since the batch exceeded the server")
				kafkaErr = nil
				break
			}
		}

		b.statsD.Count("kafka.publish", int64(len(chunk)), tags)
		if kafkaErr != nil {
			return fmt.Errorf("failed to write message: %w, approxSize: %d", kafkaErr, size.GetApproxSize(chunk))
		}
	}
	return nil
}
