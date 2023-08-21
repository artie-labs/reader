package main

import (
	"context"
	"flag"
	"github.com/artie-labs/reader/config"
	"github.com/artie-labs/reader/lib/kafka"
	"github.com/artie-labs/reader/lib/logger"
	"github.com/artie-labs/reader/sources/dynamodb"
)

func main() {
	var configFilePath string
	flag.StringVar(&configFilePath, "config", "", "path to config file")
	flag.Parse()

	// Logger as well
	ctx := config.InjectIntoContext(context.Background(), configFilePath)
	ctx = logger.InjectLoggerIntoCtx(ctx)
	ctx = kafka.InjectIntoContext(ctx)

	ddb := dynamodb.Load(ctx)
	ddb.Run(ctx)
}
