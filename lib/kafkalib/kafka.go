package kafkalib

import (
	"context"
	"crypto/tls"
	"log/slog"
	"strings"
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

	slog.Info("Setting bootstrap url", slog.Any("url", strings.Split(cfg.Kafka.BootstrapServers, ",")))

	writer := &kafka.Writer{
		Addr:                   kafka.TCP(strings.Split(cfg.Kafka.BootstrapServers, ",")...),
		Compression:            kafka.Gzip,
		Balancer:               &kafka.LeastBytes{},
		WriteTimeout:           5 * time.Second,
		AllowAutoTopicCreation: true,
	}

	if cfg.Kafka.MaxRequestSize > 0 {
		writer.BatchBytes = cfg.Kafka.MaxRequestSize
	}

	if cfg.Kafka.AwsEnabled {
		saslCfg, err := awsCfg.LoadDefaultConfig(ctx)
		if err != nil {
			logger.Fatal("Failed to load AWS configuration")
		}

		writer.Transport = &kafka.Transport{
			DialTimeout: 10 * time.Second,
			SASL:        aws_msk_iam_v2.NewMechanism(saslCfg),
			TLS:         &tls.Config{},
		}
	}

	return context.WithValue(ctx, constants.KafkaKey, writer)
}
