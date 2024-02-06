package mongo

import (
	"context"
	"github.com/artie-labs/reader/config"
	"github.com/artie-labs/reader/lib/kafkalib"
	"github.com/artie-labs/reader/lib/mtr"
)

func Run(ctx context.Context, cfg config.Settings, statsD *mtr.Client, writer kafkalib.BatchWriter) error {
	return nil
}
