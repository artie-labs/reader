package main

import (
	"context"
	"flag"
	"github.com/artie-labs/reader/config"
	"github.com/artie-labs/reader/lib/kafka"
	"github.com/artie-labs/reader/lib/logger"
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
	ctx = kafka.InjectIntoContext(ctx)

	ddb := dynamodb.Load(ctx)
	ddb.Run(ctx)
}
