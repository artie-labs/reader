package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"

	"github.com/artie-labs/reader/config"
	"github.com/artie-labs/reader/lib/kafkalib"
	"github.com/artie-labs/reader/lib/logger"
	"github.com/artie-labs/reader/lib/mtr"
	"github.com/artie-labs/reader/sources"
	"github.com/artie-labs/reader/sources/dynamodb"
	"github.com/artie-labs/reader/sources/mongo"
	"github.com/artie-labs/reader/sources/mssql"
	"github.com/artie-labs/reader/sources/mysql"
	"github.com/artie-labs/reader/sources/postgres"
	"github.com/artie-labs/reader/writers"
	"github.com/artie-labs/reader/writers/transfer"
)

func setUpMetrics(cfg *config.Metrics) (mtr.Client, error) {
	if cfg == nil {
		return nil, nil
	}

	slog.Info("Creating metrics client")
	client, err := mtr.New(cfg.Namespace, cfg.Tags, 0.5)
	if err != nil {
		return nil, err
	}

	return client, nil
}

func buildSource(ctx context.Context, cfg *config.Settings) (sources.Source, bool, error) {
	var source sources.Source
	var err error
	switch cfg.Source {
	case config.SourceDynamo:
		return dynamodb.Load(ctx, *cfg.DynamoDB)
	case config.SourceMongoDB:
		return mongo.Load(ctx, *cfg.MongoDB)
	case config.SourceMySQL:
		return mysql.Load(*cfg.MySQL)
	case config.SourceMSSQL:
		source, err = mssql.Load(*cfg.MSSQL)
	case config.SourcePostgreSQL:
		source, err = postgres.Load(*cfg.PostgreSQL)
	default:
		panic(fmt.Sprintf("unknown source %q", cfg.Source)) // should never happen
	}
	return source, false, err
}

func buildDestinationWriter(ctx context.Context, cfg *config.Settings, statsD mtr.Client) (writers.DestinationWriter, error) {
	switch cfg.Destination {
	case config.DestinationKafka:
		kafkaCfg := cfg.Kafka
		if kafkaCfg == nil {
			return nil, fmt.Errorf("kafka configuration is not set")
		}

		slog.Info("Kafka config",
			slog.Any("authMechanism", kafkaCfg.Mechanism()),
			slog.String("kafkaBootstrapServer", kafkaCfg.BootstrapServers),
			slog.Any("publishSize", kafkaCfg.GetPublishSize()),
			slog.Uint64("maxRequestSize", kafkaCfg.MaxRequestSize),
		)
		return kafkalib.NewBatchWriter(ctx, *kafkaCfg, statsD)
	case config.DestinationTransfer:
		return transfer.NewWriter(*cfg.Transfer, statsD)
	default:
		panic(fmt.Sprintf("unknown destination %q", cfg.Destination)) // should never happen
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
	slog.SetDefault(_logger)

	statsD, err := setUpMetrics(cfg.Metrics)
	if err != nil {
		logger.Fatal("Failed to set up metrics", slog.Any("err", err))
	}

	defer func() {
		cleanUpHandlers()
		if statsD != nil {
			statsD.Flush()
		}
	}()

	ctx := context.Background()
	destinationWriter, err := buildDestinationWriter(ctx, cfg, statsD)
	if err != nil {
		logger.Fatal(fmt.Sprintf("Failed to init %q destination writer", cfg.Destination), slog.Any("err", err))
	}

	source, isStreamingMode, err := buildSource(ctx, cfg)
	if err != nil {
		logger.Fatal(fmt.Sprintf("Failed to init %q source", cfg.Source), slog.Any("err", err))
	}
	defer source.Close()

	logProgress := !isStreamingMode
	writer := writers.New(destinationWriter, logProgress)

	mode := "snapshot"
	if isStreamingMode {
		mode = "stream"
	}

	slog.Info(fmt.Sprintf("Starting %s...", mode))

	if err = source.Run(ctx, writer); err != nil {
		logger.Fatal(fmt.Sprintf("Failed to %s", mode),
			slog.Any("err", err),
			slog.String("source", string(cfg.Source)),
			slog.String("destination", string(cfg.Destination)),
		)
	}
}
