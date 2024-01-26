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

func New(namespace string, tags []string, samplingRate float64) (Client, error) {
	host := os.Getenv("TELEMETRY_HOST")
	port := os.Getenv("TELEMETRY_PORT")
	address := DefaultAddr
	if !stringutil.Empty(host, port) {
		address = fmt.Sprintf("%s:%s", host, port)
		slog.Info("Overriding telemetry address with env vars", slog.String("address", address))
	}

	datadogClient, err := statsd.New(address,
		statsd.WithNamespace(stringutil.Override(DefaultNamespace, namespace)),
		statsd.WithTags(tags),
	)
	if err != nil {
		return nil, err
	}
	return &statsClient{
		client: datadogClient,
		rate:   samplingRate,
	}, nil
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
