package sources

import (
	"context"

	"github.com/artie-labs/reader/destinations"
)

type Source interface {
	Close() error
	Run(ctx context.Context, writer destinations.DestinationWriter) error
}
