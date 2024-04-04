package sources

import (
	"context"

	"github.com/artie-labs/reader/writers"
)

type Source interface {
	Close() error
	Run(ctx context.Context, writer writers.Writer) error
}
