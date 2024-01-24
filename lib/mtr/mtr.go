package mtr

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/DataDog/datadog-go/statsd"
	"github.com/artie-labs/transfer/lib/stringutil"

	"github.com/artie-labs/reader/constants"
	"github.com/artie-labs/reader/lib/logger"
)

func InjectDatadogIntoCtx(ctx context.Context, namespace string, tags []string, samplingRate float64) context.Context {
	host := os.Getenv("TELEMETRY_HOST")
	port := os.Getenv("TELEMETRY_PORT")
	address := DefaultAddr
	if !stringutil.Empty(host, port) {
		address = fmt.Sprintf("%s:%s", host, port)
		slog.Info("Overriding telemetry address with env vars", slog.String("address", address))
	}

	datadogClient, err := statsd.New(address)
	if err != nil {
		logger.Fatal("Failed to create datadog client", slog.Any("err", err))
	}

	datadogClient.Tags = tags
	datadogClient.Namespace = stringutil.Override(DefaultNamespace, namespace)
	return context.WithValue(ctx, constants.MtrKey, &statsClient{
		client: datadogClient,
		rate:   samplingRate,
	})
}

func FromContext(ctx context.Context) Client {
	metricsClientVal := ctx.Value(constants.MtrKey)
	if metricsClientVal == nil {
		logger.Fatal("Metrics client is nil")
	}

	metricsClient, isOk := metricsClientVal.(Client)
	if !isOk {
		logger.Fatal("Metrics client is not mtr.Client type")
	}

	return metricsClient
}
