package kafkalib

import (
	"context"
	"crypto/tls"
	"github.com/artie-labs/reader/config"
	"github.com/artie-labs/reader/lib/logger"
	awsCfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/aws_msk_iam_v2"
	"strings"
	"time"
)

const (
	ctxKey = "_kafka"
)

func FromContext(ctx context.Context) *kafka.Writer {
	log := logger.FromContext(ctx)
	kafkaVal := ctx.Value(ctxKey)
	if kafkaVal == nil {
		log.Fatal("kafka is not set in context.Context")
	}

	kafkaWriter, isOk := kafkaVal.(*kafka.Writer)
	if !isOk {
		log.Fatal("kafka writer is not type *kafka.Writer")
	}

	return kafkaWriter
}

func InjectIntoContext(ctx context.Context) context.Context {
	log := logger.FromContext(ctx)
	cfg := config.FromContext(ctx)

	if cfg == nil || cfg.Kafka == nil {
		log.Fatal("Kafka configuration is not set")
	}

	logger.FromContext(ctx).WithField("url", strings.Split(cfg.Kafka.BootstrapServers, ",")).Info("setting bootstrap url")

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
			log.Fatal("Failed to load AWS configuration")
		}

		writer.Transport = &kafka.Transport{
			DialTimeout: 10 * time.Second,
			SASL:        aws_msk_iam_v2.NewMechanism(saslCfg),
			TLS:         &tls.Config{},
		}
	}

	return context.WithValue(ctx, ctxKey, writer)
}
