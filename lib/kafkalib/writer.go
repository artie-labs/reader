package kafkalib

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/artie-labs/transfer/lib/jitter"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/size"
	"github.com/segmentio/kafka-go"

	"github.com/artie-labs/reader/config"
	"github.com/artie-labs/reader/lib"
	"github.com/artie-labs/reader/lib/mtr"
)

const (
	baseJitterMs = 300
	maxJitterMs  = 5000
)

func newWriter(ctx context.Context, cfg config.Kafka) (*kafka.Writer, error) {
	slog.Info("Setting kafka bootstrap URLs", slog.Any("urls", cfg.BootstrapAddresses()))
	kafkaConn := kafkalib.NewConnection(cfg.AwsEnabled, cfg.DisableTLS, cfg.Username, cfg.Password)
	transport, err := kafkaConn.Transport(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create kafka transport: %w", err)
	}

	writer := &kafka.Writer{
		Addr:                   kafka.TCP(cfg.BootstrapAddresses()...),
		AllowAutoTopicCreation: true,
		Balancer:               &kafka.LeastBytes{},
		Compression:            kafka.Gzip,
		Transport:              transport,
		WriteTimeout:           5 * time.Second,
	}

	if cfg.MaxRequestSize > 0 {
		writer.BatchBytes = int64(cfg.MaxRequestSize)
	}

	return writer, nil
}

type BatchWriter struct {
	writer *kafka.Writer
	cfg    config.Kafka
	statsD mtr.Client
}

func NewBatchWriter(ctx context.Context, cfg config.Kafka, statsD mtr.Client) (*BatchWriter, error) {
	if cfg.TopicPrefix == "" {
		return nil, fmt.Errorf("kafka topic prefix cannot be empty")
	}

	if cfg.GetPublishSize() < 1 {
		return nil, fmt.Errorf("kafka publish size must be greater than zero")
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

func (b *BatchWriter) Write(ctx context.Context, rawMsgs []lib.RawMessage) error {
	if len(rawMsgs) == 0 {
		return nil
	}

	var msgs []kafka.Message
	var sampleExecutionTime time.Time
	for _, rawMsg := range rawMsgs {
		sampleExecutionTime = rawMsg.Event().GetExecutionTime()
		kafkaMsg, err := newMessage(b.cfg.TopicPrefix, rawMsg)
		if err != nil {
			return fmt.Errorf("failed to encode kafka message: %w", err)
		}
		msgs = append(msgs, kafkaMsg)
	}

	for _, batch := range batched(msgs, int(b.cfg.GetPublishSize())) {
		tags := map[string]string{
			"what": "error",
		}

		var kafkaErr error
		for attempts := range 10 {
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

			kafkaErr = b.writer.WriteMessages(ctx, batch...)
			if kafkaErr == nil {
				tags["what"] = "success"
				break
			}

			if isExceedMaxMessageBytesErr(kafkaErr) {
				slog.Info("Skipping this batch since the message size exceeded the server max")
				kafkaErr = nil
				break
			}
		}

		if b.statsD != nil {
			b.statsD.Count("kafka.publish", int64(len(batch)), tags)
			b.statsD.Gauge("kafka.lag_ms", float64(time.Since(sampleExecutionTime).Milliseconds()), tags)
		}

		if kafkaErr != nil {
			return fmt.Errorf("failed to write message: %w, approxSize: %d", kafkaErr, size.GetApproxSize(batch))
		}
	}
	return nil
}

func (b *BatchWriter) OnComplete() error {
	return nil
}

func newMessage(topicPrefix string, rawMessage lib.RawMessage) (kafka.Message, error) {
	valueBytes, err := json.Marshal(rawMessage.Event())
	if err != nil {
		return kafka.Message{}, err
	}

	keyBytes, err := json.Marshal(rawMessage.PartitionKey())
	if err != nil {
		return kafka.Message{}, err
	}

	return kafka.Message{
		Topic: fmt.Sprintf("%s.%s", topicPrefix, rawMessage.TopicSuffix()),
		Key:   keyBytes,
		Value: valueBytes,
	}, nil
}
