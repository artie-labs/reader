package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"time"

	"github.com/artie-labs/reader/config"
	"github.com/artie-labs/reader/lib/kafkalib"
	"github.com/artie-labs/reader/lib/logger"
	"github.com/artie-labs/reader/lib/mtr"
	"github.com/artie-labs/reader/sources/dynamodb"
	"github.com/artie-labs/reader/sources/postgres"
	"github.com/getsentry/sentry-go"
	"github.com/segmentio/kafka-go"
)

func setUpMetrics(cfg *config.Metrics) (*mtr.Client, error) {
	if cfg == nil {
		return nil, nil
	}

	slog.Info("Creating metrics client")
	client, err := mtr.New(cfg.Namespace, cfg.Tags, 0.5)
	if err != nil {
		return nil, err
	}
	return &client, nil
}

func setUpKafka(ctx context.Context, cfg *config.Kafka) (*kafka.Writer, error) {
	if cfg == nil {
		return nil, fmt.Errorf("kafka configuration is not set")
	}
	slog.Info("Kafka config",
		slog.Bool("aws", cfg.AwsEnabled),
		slog.String("kafkaBootstrapServer", cfg.BootstrapServers),
		slog.Any("publishSize", cfg.GetPublishSize()),
		slog.Uint64("maxRequestSize", cfg.MaxRequestSize),
	)
	return kafkalib.NewWriter(ctx, *cfg)
}

func main() {
	var configFilePath string
	flag.StringVar(&configFilePath, "config", "", "path to config file")
	flag.Parse()

	cfg, err := config.ReadConfig(configFilePath)
	if err != nil {
		logger.Fatal("Failed to read config file", slog.Any("err", err))
	}

	_logger, usingSentry := logger.NewLogger(cfg)
	slog.SetDefault(_logger)
	if usingSentry {
		defer sentry.Flush(2 * time.Second)
		slog.Info("Sentry logger enabled")
	}

	ctx := context.Background()

	statsD, err := setUpMetrics(cfg.Metrics)
	if err != nil {
		logger.Fatal("Failed to set up metrics", slog.Any("err", err))
	}

	kafka, err := setUpKafka(ctx, cfg.Kafka)
	if err != nil {
		logger.Fatal("Failed to set up kafka", slog.Any("err", err))
	}

	switch cfg.Source {
	case "", config.SourceDynamo:
		ddb := dynamodb.Load(*cfg, statsD, kafka)
		ddb.Run(ctx)
	case config.SourcePostgreSQL:
		postgres.Run(ctx, *cfg, statsD, kafka)
	}
}
