package mtr

import (
	"cmp"
	"fmt"
	"log/slog"
	"os"

	"github.com/DataDog/datadog-go/v5/statsd"
	"github.com/artie-labs/transfer/lib/stringutil"
)

func New(namespace string, tags []string, samplingRate float64) (*statsClient, error) {
	host := os.Getenv("TELEMETRY_HOST")
	port := os.Getenv("TELEMETRY_PORT")
	address := DefaultAddr
	if !stringutil.Empty(host, port) {
		address = fmt.Sprintf("%s:%s", host, port)
		slog.Info("Overriding telemetry address with env vars", slog.String("address", address))
	}

	datadogClient, err := statsd.New(address,
		statsd.WithNamespace(cmp.Or(namespace, DefaultNamespace)),
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
