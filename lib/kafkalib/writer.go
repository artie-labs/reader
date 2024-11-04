package kafkalib

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/artie-labs/transfer/lib/batch"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/retry"
	"github.com/segmentio/kafka-go"

	"github.com/artie-labs/reader/config"
	"github.com/artie-labs/reader/lib"
	"github.com/artie-labs/reader/lib/mtr"
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

func buildKafkaMessageWrapper(topicPrefix string, rawMessage lib.RawMessage) (KafkaMessageWrapper, error) {
	valueBytes, err := json.Marshal(rawMessage.Event())
	if err != nil {
		return KafkaMessageWrapper{}, err
	}

	keyBytes, err := json.Marshal(rawMessage.PartitionKey())
	if err != nil {
		return KafkaMessageWrapper{}, err
	}

	return KafkaMessageWrapper{
		Topic:        fmt.Sprintf("%s.%s", topicPrefix, rawMessage.TopicSuffix()),
		MessageKey:   keyBytes,
		MessageValue: valueBytes,
	}, nil
}

// KafkaMessageWrapper is a wrapper around a Kafka message. We did this so that we can marshal and unmarshal the message
type KafkaMessageWrapper struct {
	Topic        string `json:"topic"`
	MessageKey   []byte `json:"messageKey"`
	MessageValue []byte `json:"messageValue"`
}

func (k KafkaMessageWrapper) Key() string {
	return string(k.MessageKey)
}

func (k KafkaMessageWrapper) toKafkaMessage() kafka.Message {
	return kafka.Message{
		Topic: k.Topic,
		Key:   k.MessageKey,
		Value: k.MessageValue,
	}
}

var encoder = func(msg KafkaMessageWrapper) ([]byte, error) {
	return json.Marshal(msg)
}

func (b *BatchWriter) write(ctx context.Context, messages []KafkaMessageWrapper, sampleExecutionTime time.Time) error {
	retryCfg, err := retry.NewJitterRetryConfig(100, 5000, 10, retry.AlwaysRetry)
	if err != nil {
		return err
	}

	return batch.BySize[KafkaMessageWrapper](messages, int(b.writer.BatchBytes), false, encoder, func(chunk [][]byte) error {
		tags := map[string]string{"what": "error"}
		defer func() {
			if b.statsD != nil {
				b.statsD.Count("kafka.publish", int64(len(chunk)), tags)
				b.statsD.Gauge("kafka.lag_ms", float64(time.Since(sampleExecutionTime).Milliseconds()), tags)
			}
		}()

		var kafkaMessages []kafka.Message
		for _, bytes := range chunk {
			var msg KafkaMessageWrapper
			if err = json.Unmarshal(bytes, &msg); err != nil {
				return fmt.Errorf("failed to unmarshal message: %w", err)
			}

			kafkaMessages = append(kafkaMessages, msg.toKafkaMessage())
		}

		err = retry.WithRetries(retryCfg, func(_ int, _ error) error {
			publishErr := b.writer.WriteMessages(ctx, kafkaMessages...)
			if isRetryableError(publishErr) {
				if err = b.reload(ctx); err != nil {
					return fmt.Errorf("failed to reload kafka writer: %w", err)
				}
			}

			return publishErr
		})

		if err != nil {
			return fmt.Errorf("failed to write messages: %w", err)
		}

		tags["what"] = "success"
		return nil
	})
}

func (b *BatchWriter) Write(ctx context.Context, rawMsgs []lib.RawMessage) error {
	if len(rawMsgs) == 0 {
		return nil
	}

	var msgs []KafkaMessageWrapper
	var sampleExecutionTime time.Time
	for _, rawMsg := range rawMsgs {
		sampleExecutionTime = rawMsg.Event().GetExecutionTime()
		msg, err := buildKafkaMessageWrapper(b.cfg.TopicPrefix, rawMsg)
		if err != nil {
			return fmt.Errorf("failed to build kafka message: %w", err)
		}

		msgs = append(msgs, msg)
	}

	return b.write(ctx, msgs, sampleExecutionTime)
}

func (b *BatchWriter) OnComplete(_ context.Context) error {
	return nil
}
