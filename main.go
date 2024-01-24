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
	ctx = kafkalib.InjectIntoContext(ctx)
	if cfg.Metrics != nil {
		slog.Info("Injecting datadog")
		ctx = mtr.InjectDatadogIntoCtx(ctx, cfg.Metrics.Namespace, cfg.Metrics.Tags, 0.5)
	}

	ddb := dynamodb.Load(*cfg)
	ddb.Run(ctx)
}
