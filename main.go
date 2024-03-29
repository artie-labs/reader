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

func buildSource(cfg *config.Settings) (sources.Source, bool, error) {
	var source sources.Source
	var isStreamingMode bool
	var err error
	switch cfg.Source {
	case config.SourceDynamo:
		source, isStreamingMode, err = dynamodb.Load(*cfg.DynamoDB)
	case config.SourceMongoDB:
		source, err = mongo.Load(*cfg.MongoDB)
	case config.SourceMySQL:
		source, err = mysql.Load(*cfg.MySQL)
	case config.SourcePostgreSQL:
		source, err = postgres.Load(*cfg.PostgreSQL)
	default:
		panic(fmt.Sprintf("unknown source: %s", cfg.Source)) // should never happen
	}
	return source, isStreamingMode, err
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

	source, isStreamingMode, err := buildSource(cfg)
	if err != nil {
		logger.Fatal(fmt.Sprintf("Failed to init '%s' source", cfg.Source), slog.Any("err", err))
	}
	defer source.Close()

	logProgress := !isStreamingMode
	_writer := writer.New(destination, logProgress)

	mode := "snapshot"
	if isStreamingMode {
		mode = "stream"
	}

	slog.Info(fmt.Sprintf("Starting %s...", mode))

	if err = source.Run(ctx, _writer); err != nil {
		logger.Fatal(fmt.Sprintf("Failed to %s", mode),
			slog.Any("err", err),
			slog.String("source", string(cfg.Source)),
			slog.String("destination", string(cfg.Destination)),
		)
	}
}
