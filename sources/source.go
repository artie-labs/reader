package sources

import (
	"context"

	"github.com/artie-labs/reader/lib/kafkalib"
	"github.com/artie-labs/reader/lib/mtr"
)

type Source interface {
	Close() error
	Run(ctx context.Context, writer kafkalib.BatchWriter, statsD *mtr.Client) error
}
