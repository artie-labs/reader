package transfer

import (
	"context"
	"fmt"
	"github.com/artie-labs/reader/lib"
	"github.com/artie-labs/transfer/lib/artie"
	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/destination"
	"github.com/artie-labs/transfer/lib/destination/utils"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/models"
	"github.com/artie-labs/transfer/models/event"
)

type BatchWriter struct {
	// TODO: How do we generate this
	cfg         config.Config
	inMemDB     *models.DatabaseData
	tc          *kafkalib.TopicConfig
	destination destination.DataWarehouse
}

func NewBatchWriter(cfg config.Config) (*BatchWriter, error) {
	if cfg.Kafka == nil {
		return nil, fmt.Errorf("kafka config should not be nil")
	}

	if len(cfg.Kafka.TopicConfigs) != 1 {
		return nil, fmt.Errorf("kafka config should have exactly one topic config")
	}

	return &BatchWriter{
		cfg:         cfg,
		inMemDB:     models.NewMemoryDB(),
		tc:          cfg.Kafka.TopicConfigs[0],
		destination: utils.DataWarehouse(cfg, nil),
	}, nil
}

func (b *BatchWriter) WriteRawMessages(_ context.Context, rawMsgs []lib.RawMessage) error {
	if len(rawMsgs) == 0 {
		return nil
	}

	for _, rawMsg := range rawMsgs {
		evt := event.ToMemoryEvent(rawMsg.GetPayload(), rawMsg.PartitionKey, b.tc, config.Replication)
		shouldFlush, _, err := evt.Save(b.cfg, b.inMemDB, b.tc, artie.Message{})
		if err != nil {
			return fmt.Errorf("failed to save event: %w", err)
		}

		if shouldFlush {
			// TODO: Include the `flushReason` into statsD.
			for _, tableData := range b.inMemDB.TableData() {
				if err = b.destination.Append(tableData.TableData); err != nil {
					return fmt.Errorf("failed to append data to destination: %w", err)
				}
			}
		}
	}

	return nil
}

func (b *BatchWriter) OnFinish() error {
	// TODO: Run de-duplicate logic here.
	return nil
}
