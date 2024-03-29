package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"

	"github.com/artie-labs/transfer/lib/telemetry/metrics"

	"github.com/artie-labs/reader/config"
	"github.com/artie-labs/reader/destinations"
	"github.com/artie-labs/reader/lib/kafkalib"
	"github.com/artie-labs/reader/lib/logger"
	"github.com/artie-labs/reader/lib/mtr"
	"github.com/artie-labs/reader/lib/writer"
	"github.com/artie-labs/reader/sources"
	"github.com/artie-labs/reader/sources/dynamodb"
	"github.com/artie-labs/reader/sources/mongo"
	"github.com/artie-labs/reader/sources/mysql"
	"github.com/artie-labs/reader/sources/postgres"
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

func buildSource(cfg *config.Settings) (sources.Source, error) {
	switch cfg.Source {
	case config.SourceDynamo:
		return dynamodb.Load(*cfg.DynamoDB)
	case config.SourceMongoDB:
		return mongo.Load(*cfg.MongoDB)
	case config.SourceMySQL:
		return mysql.Load(*cfg.MySQL)
	case config.SourcePostgreSQL:
		return postgres.Load(*cfg.PostgreSQL)
	default:
		panic(fmt.Sprintf("unknown source: %s", cfg.Source)) // should never happen
	}
}

func buildDestination(ctx context.Context, cfg *config.Settings, statsD mtr.Client) (destinations.Destination, error) {
	switch cfg.Destination {
	case config.DestinationKafka:
		kafkaCfg := cfg.Kafka
		if kafkaCfg == nil {
			return nil, fmt.Errorf("kafka configuration is not set")
		}
		slog.Info("Kafka config",
			slog.Bool("aws", kafkaCfg.AwsEnabled),
			slog.String("kafkaBootstrapServer", kafkaCfg.BootstrapServers),
			slog.Any("publishSize", kafkaCfg.GetPublishSize()),
			slog.Uint64("maxRequestSize", kafkaCfg.MaxRequestSize),
		)
		return kafkalib.NewBatchWriter(ctx, *kafkaCfg, statsD)
	default:
		panic(fmt.Sprintf("unknown destination: %s", cfg.Destination)) // should never happen
	}
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

	destination, err := buildDestination(ctx, cfg, statsD)
	if err != nil {
		logger.Fatal(fmt.Sprintf("Failed to init '%s' destination", cfg.Destination), slog.Any("err", err))
	}

	source, err := buildSource(cfg)
	if err != nil {
		logger.Fatal(fmt.Sprintf("Failed to init '%s' source", cfg.Source), slog.Any("err", err))
	}
	defer source.Close()

	if err = source.Run(ctx, writer.New(destination)); err != nil {
		logger.Fatal("Failed to run",
			slog.Any("err", err),
			slog.String("source", string(cfg.Source)),
			slog.String("destination", string(cfg.Destination)),
		)
	}
}
