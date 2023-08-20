package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/artie-labs/reader/config"
	"github.com/artie-labs/reader/lib/logger"
)

func main() {
	var configFilePath string
	flag.StringVar(&configFilePath, "config", "", "path to config file")
	flag.Parse()

	// Logger as well
	ctx := config.InjectIntoContext(context.Background(), configFilePath)
	ctx = logger.InjectLoggerIntoCtx(ctx)
	fmt.Println(ctx)

}
