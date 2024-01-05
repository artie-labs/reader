package mtr

import (
	"context"
	"fmt"
	"github.com/DataDog/datadog-go/statsd"
	"github.com/artie-labs/transfer/lib/stringutil"
	"os"

	"github.com/artie-labs/reader/constants"
	"github.com/artie-labs/reader/lib/logger"
)

func InjectDatadogIntoCtx(ctx context.Context, namespace string, tags []string, samplingRate float64) context.Context {
	host := os.Getenv("TELEMETRY_HOST")
	port := os.Getenv("TELEMETRY_PORT")
	address := DefaultAddr
	if !stringutil.Empty(host, port) {
		address = fmt.Sprintf("%s:%s", host, port)
		logger.FromContext(ctx).WithField("address", address).Info("overriding telemetry address with env vars")
	}

	datadogClient, err := statsd.New(address)
	if err != nil {
		logger.FromContext(ctx).WithError(err).Fatal("failed to create datadog client")
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
		logger.FromContext(ctx).Fatal("metrics client is nil")
	}

	metricsClient, isOk := metricsClientVal.(Client)
	if !isOk {
		logger.FromContext(ctx).Fatal("metrics client is not mtr.Client type")
	}

	return metricsClient
}
