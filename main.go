package main

import (
	"context"
	"flag"
	"github.com/artie-labs/reader/config"
	"github.com/artie-labs/reader/lib/kafkalib"
	"github.com/artie-labs/reader/lib/logger"
	"github.com/artie-labs/reader/lib/mtr"
	"github.com/artie-labs/reader/sources/dynamodb"
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
		ctx = mtr.InjectDatadogIntoCtx(ctx, cfg.Metrics.Namespace, cfg.Metrics.Tags, 0.5)
	}

	ddb := dynamodb.Load(ctx)
	ddb.Run(ctx)
}
