package transfer

import (
	"context"
	"fmt"
	"time"

	"github.com/artie-labs/reader/lib"
	"github.com/artie-labs/reader/lib/mtr"
	"github.com/artie-labs/transfer/lib/artie"
	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/destination"
	"github.com/artie-labs/transfer/lib/destination/utils"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/models"
	"github.com/artie-labs/transfer/models/event"
)

type Writer struct {
	cfg         config.Config
	statsD      mtr.Client
	inMemDB     *models.DatabaseData
	tc          *kafkalib.TopicConfig
	destination destination.DataWarehouse
}

func NewWriter(cfg config.Config, statsD mtr.Client) (*Writer, error) {
	if cfg.Kafka == nil {
		return nil, fmt.Errorf("kafka config should not be nil")
	}

	if len(cfg.Kafka.TopicConfigs) != 1 {
		return nil, fmt.Errorf("kafka config should have exactly one topic config")
	}

	return &Writer{
		cfg:         cfg,
		statsD:      statsD,
		inMemDB:     models.NewMemoryDB(),
		tc:          cfg.Kafka.TopicConfigs[0],
		destination: utils.DataWarehouse(cfg, nil),
	}, nil
}

func (w *Writer) Write(_ context.Context, messages []lib.RawMessage) error {
	if len(messages) == 0 {
		return nil
	}

	var events []event.Event
	for _, message := range messages {
		events = append(events, event.ToMemoryEvent(message.Event(), message.PartitionKey(), w.tc, config.Replication))
	}

	tags := map[string]string{
		"mode":     w.cfg.Mode.String(),
		"op":       "r",
		"what":     "success",
		"database": w.tc.Database,
		"schema":   w.tc.Schema,
		"table":    events[0].Table,
	}
	defer func() {
		w.statsD.Count("process.message", int64(len(events)), tags)
	}()

	for _, evt := range events {
		shouldFlush, flushReason, err := evt.Save(w.cfg, w.inMemDB, w.tc, artie.Message{})
		if err != nil {
			return fmt.Errorf("failed to save event: %w", err)
		}

		if shouldFlush {
			if err := w.flush(flushReason); err != nil {
				return err
			}
		}
	}

	return nil
}

func (w *Writer) getTableData() (string, *models.TableData, error) {
	tableData := w.inMemDB.TableData()
	if len(tableData) != 1 {
		return "", nil, fmt.Errorf("expected exactly one table")
	}
	for k, v := range tableData {
		return k, v, nil
	}
	return "", nil, fmt.Errorf("expected exactly one table")
}

func (w *Writer) flush(reason string) error {
	tableName, tableData, err := w.getTableData()
	if err != nil {
		return err
	}

	if tableData.ShouldSkipUpdate() {
		return nil // No need to flush.
	}

	start := time.Now()
	tags := map[string]string{
		"what":     "success",
		"mode":     tableData.Mode().String(),
		"table":    tableName,
		"database": tableData.TopicConfig.Database,
		"schema":   tableData.TopicConfig.Schema,
		"reason":   reason,
	}
	defer func() {
		w.statsD.Timing("flush", time.Since(start), tags)
	}()

	if !w.tc.SoftDelete {
		columns := tableData.ReadOnlyInMemoryCols()
		columns.DeleteColumn(constants.DeleteColumnMarker)
		tableData.SetInMemoryColumns(columns)
	}

	tableData.ResetTempTableSuffix()
	if err := w.destination.Append(tableData.TableData); err != nil {
		tags["what"] = "merge_fail"
		tags["retryable"] = fmt.Sprint(w.destination.IsRetryableError(err))
		return fmt.Errorf("failed to append data to destination: %w", err)
	}
	w.inMemDB.ClearTableConfig(tableName)
	return nil
}

func (w *Writer) OnComplete() error {
	if err := w.flush("complete"); err != nil {
		return err
	}
	// TODO: Run de-duplicate logic here as long as some amount of messages were written to the destination.
	return nil
}