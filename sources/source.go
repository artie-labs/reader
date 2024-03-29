package sources

import (
	"context"

	"github.com/artie-labs/reader/lib/writer"
)

type Source interface {
	Close() error
	Run(ctx context.Context, _writer writer.Writer) error
}
