package main

import (
	"context"
	"flag"
	"github.com/artie-labs/reader/config"
	"github.com/artie-labs/reader/lib/kafkalib"
	"github.com/artie-labs/reader/lib/logger"
	"github.com/artie-labs/reader/sources/dynamodb"
	"github.com/artie-labs/transfer/lib/telemetry/metrics"
	"github.com/artie-labs/transfer/lib/telemetry/metrics/datadog"
	"log"
)

func main() {
	var configFilePath string
	flag.StringVar(&configFilePath, "config", "", "path to config file")
	flag.Parse()

	cfg, err := config.ReadConfig(configFilePath)
	if err != nil {
		log.Fatalf("failed to read config file, err: %v", err)
	}

	ctx := config.InjectIntoContext(context.Background(), cfg)
	ctx = logger.InjectLoggerIntoCtx(ctx)
	ctx = kafkalib.InjectIntoContext(ctx)
	if cfg.Metrics != nil {
		logger.FromContext(ctx).Info("injecting datadog")
		client, err := datadog.NewDatadogClient(ctx, map[string]interface{}{
			datadog.Namespace: cfg.Metrics.Namespace,
			datadog.Tags:      cfg.Metrics.Tags,
			// Sample 50% to start, we can make this configurable later.
			datadog.Sampling: 0.5,
		})

		if err != nil {
			logger.FromContext(ctx).WithError(err).Fatal("failed to create datadog client")
		}

		ctx = metrics.InjectMetricsClientIntoCtx(ctx, client)
	}

	ddb := dynamodb.Load(ctx)
	ddb.Run(ctx)
}
