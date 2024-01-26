package main

import (
	"context"
	"flag"
	"log/slog"
	"time"

	"github.com/artie-labs/reader/config"
	"github.com/artie-labs/reader/lib/kafkalib"
	"github.com/artie-labs/reader/lib/logger"
	"github.com/artie-labs/reader/lib/mtr"
	"github.com/artie-labs/reader/lib/postgres"
	"github.com/artie-labs/reader/sources/dynamodb"
	"github.com/getsentry/sentry-go"
)

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

	ctx := config.InjectIntoContext(context.Background(), cfg)

	var statsD *mtr.Client
	if cfg.Metrics != nil {
		slog.Info("Injecting datadog")
		_statsD, err := mtr.New(cfg.Metrics.Namespace, cfg.Metrics.Tags, 0.5)
		if err != nil {
			logger.Fatal("Failed to create datadog client", slog.Any("err", err))
		}
		statsD = &_statsD
	}

	switch cfg.Source {
	case "", config.SourceDynamo:
		// TODO: pull kafkalib out of context
		ctx = kafkalib.InjectIntoContext(ctx)
		ddb := dynamodb.Load(*cfg, statsD)
		ddb.Run(ctx)
	case config.SourcePostgreSQL:
		postgres.Run(ctx, *cfg, statsD)
	}
}
