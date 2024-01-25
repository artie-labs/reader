package kafkalib

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"time"

	awsCfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/aws_msk_iam_v2"

	"github.com/artie-labs/reader/config"
	"github.com/artie-labs/reader/constants"
	"github.com/artie-labs/reader/lib/logger"
)

func FromContext(ctx context.Context) *kafka.Writer {
	kafkaVal := ctx.Value(constants.KafkaKey)
	if kafkaVal == nil {
		logger.Fatal("Kafka is not set in context.Context")
	}

	kafkaWriter, isOk := kafkaVal.(*kafka.Writer)
	if !isOk {
		logger.Fatal("Kafka writer is not type *kafka.Writer")
	}

	return kafkaWriter
}

func InjectIntoContext(ctx context.Context) context.Context {
	cfg := config.FromContext(ctx)
	if cfg == nil || cfg.Kafka == nil {
		logger.Fatal("Kafka configuration is not set")
	}
	writer, err := NewWriter(ctx, *cfg.Kafka)
	if err != nil {
		logger.Fatal("Failed to create kafka writer", slog.Any("err", err))
	}
	return context.WithValue(ctx, constants.KafkaKey, writer)
}

func NewWriter(ctx context.Context, cfg config.Kafka) (*kafka.Writer, error) {
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

type ReloadableWriter struct {
	*kafka.Writer

	cfg config.Kafka
}

func (w *ReloadableWriter) Reload(ctx context.Context) error {
	if err := w.Writer.Close(); err != nil {
		return err
	}

	writer, err := NewWriter(ctx, w.cfg)
	if err != nil {
		return err
	}

	w.Writer = writer
	return nil
}
