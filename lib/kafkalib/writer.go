package kafkalib

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"time"

	"github.com/artie-labs/transfer/lib/jitter"
	"github.com/artie-labs/transfer/lib/size"
	awsCfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/aws_msk_iam_v2"

	"github.com/artie-labs/reader/config"
	"github.com/artie-labs/reader/lib"
	"github.com/artie-labs/reader/lib/iterator"
	"github.com/artie-labs/reader/lib/mtr"
)

const (
	baseJitterMs = 300
	maxJitterMs  = 5000
)

func newWriter(ctx context.Context, cfg config.Kafka) (*kafka.Writer, error) {
	slog.Info("Setting kafka bootstrap URLs", slog.Any("urls", cfg.BootstrapAddresses()))
	writer := &kafka.Writer{
		Addr:                   kafka.TCP(cfg.BootstrapAddresses()...),
		Compression:            kafka.Gzip,
		Balancer:               &kafka.LeastBytes{},
		WriteTimeout:           5 * time.Second,
		AllowAutoTopicCreation: true,
	}

	if cfg.MaxRequestSize > 0 {
		writer.BatchBytes = int64(cfg.MaxRequestSize)
	}

	if cfg.AwsEnabled {
		saslCfg, err := awsCfg.LoadDefaultConfig(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to load AWS configuration: %w", err)
		}

		writer.Transport = &kafka.Transport{
			DialTimeout: 10 * time.Second,
			SASL:        aws_msk_iam_v2.NewMechanism(saslCfg),
			TLS:         &tls.Config{},
		}
	}

	return writer, nil
}

type BatchWriter struct {
	writer    *kafka.Writer
	batchSize uint
	cfg       config.Kafka
	statsD    mtr.Client
}

func NewBatchWriter(ctx context.Context, cfg config.Kafka, statsD mtr.Client) (*BatchWriter, error) {
	if cfg.TopicPrefix == "" {
		return nil, fmt.Errorf("kafka topic prefix cannot be empty")
	}

	batchSize := cfg.GetPublishSize()
	if batchSize < 1 {
		return nil, fmt.Errorf("kafka publish size must be greater than zero")
	}

	writer, err := newWriter(ctx, cfg)
	if err != nil {
		return nil, err
	}

	return &BatchWriter{writer, batchSize, cfg, statsD}, nil
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

func (b *BatchWriter) WriteRawMessages(ctx context.Context, rawMsgs []lib.RawMessage) error {
	if len(rawMsgs) == 0 {
		return nil
	}

	var msgs []kafka.Message
	for _, rawMsg := range rawMsgs {
		kafkaMsg, err := newMessage(b.cfg.TopicPrefix, rawMsg)
		if err != nil {
			return fmt.Errorf("failed to encode kafka message: %w", err)
		}
		msgs = append(msgs, kafkaMsg)
	}

	iter := iterator.Batched(msgs, int(b.batchSize))
	for iter.HasNext() {
		tags := map[string]string{
			"what": "error",
		}

		var kafkaErr error
		chunk, err := iter.Next()
		if err != nil {
			return err
		}
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
