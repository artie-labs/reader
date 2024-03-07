package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/artie-labs/reader/config"
	"github.com/artie-labs/reader/lib/kafkalib"
	"github.com/artie-labs/reader/lib/logger"
	"github.com/artie-labs/reader/lib/mtr"
	"github.com/artie-labs/reader/sources"
	"github.com/artie-labs/reader/sources/dynamodb"
	"github.com/artie-labs/reader/sources/mongo"
	"github.com/artie-labs/reader/sources/mysql"
	"github.com/artie-labs/reader/sources/postgres"
	"github.com/artie-labs/transfer/lib/telemetry/metrics"
	"log/slog"
)

func setUpMetrics(cfg *config.Metrics) (mtr.Client, error) {
	if cfg == nil {
		return &metrics.NullMetricsProvider{}, nil
	}

	slog.Info("Creating metrics client")
	client, err := mtr.New(cfg.Namespace, cfg.Tags, 0.5)
	if err != nil {
		return nil, err
	}

	return client, nil
}

func setUpKafka(ctx context.Context, cfg *config.Kafka, statsD mtr.Client) (*kafkalib.BatchWriter, error) {
	if cfg == nil {
		return nil, fmt.Errorf("kafka configuration is not set")
	}
	slog.Info("Kafka config",
		slog.Bool("aws", cfg.AwsEnabled),
		slog.String("kafkaBootstrapServer", cfg.BootstrapServers),
		slog.Any("publishSize", cfg.GetPublishSize()),
		slog.Uint64("maxRequestSize", cfg.MaxRequestSize),
	)
	return kafkalib.NewBatchWriter(ctx, *cfg, statsD)
}

func buildSource(cfg *config.Settings) (sources.Source, error) {
	switch cfg.Source {
	case "", config.SourceDynamo:
		return dynamodb.Load(*cfg.DynamoDB)
	case config.SourceMongoDB:
		return mongo.Load(*cfg.MongoDB)
	case config.SourceMySQL:
		return mysql.Load(*cfg.MySQL)
	case config.SourcePostgreSQL:
		return postgres.Load(*cfg.PostgreSQL)
	}
	panic(fmt.Sprintf("Unknown source: %s", cfg.Source)) // should never happen
}

func main() {
	var configFilePath string
	flag.StringVar(&configFilePath, "config", "", "path to config file")
	flag.Parse()

	cfg, err := config.ReadConfig(configFilePath)
	if err != nil {
		logger.Fatal("Failed to read config file", slog.Any("err", err))
	}

	_logger, cleanUpHandlers := logger.NewLogger(cfg)
	defer cleanUpHandlers()
	slog.SetDefault(_logger)
	ctx := context.Background()

	statsD, err := setUpMetrics(cfg.Metrics)
	if err != nil {
		logger.Fatal("Failed to set up metrics", slog.Any("err", err))
	}

	writer, err := setUpKafka(ctx, cfg.Kafka, statsD)
	if err != nil {
		logger.Fatal("Failed to set up kafka", slog.Any("err", err))
	}

	source, err := buildSource(cfg)
	if err != nil {
		logger.Fatal(fmt.Sprintf("Failed to init %s", cfg.Source), slog.Any("err", err))
	}
	defer source.Close()

	err = source.Run(ctx, *writer)
	if err != nil {
		logger.Fatal(fmt.Sprintf("Failed to run %s snapshot", cfg.Source), slog.Any("err", err))
	}
}
