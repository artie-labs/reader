package sources

import (
	"context"

	"github.com/artie-labs/reader/lib/kafkalib"
)

type Source interface {
	Close() error
	Run(ctx context.Context, writer kafkalib.BatchWriter) error
}
