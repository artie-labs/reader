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
)

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
