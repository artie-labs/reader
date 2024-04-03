package transfer

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/artie-labs/reader/lib"
	"github.com/artie-labs/reader/lib/mtr"
	"github.com/artie-labs/transfer/lib/artie"
	"github.com/artie-labs/transfer/lib/cdc/util"
	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/destination"
	"github.com/artie-labs/transfer/lib/destination/utils"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/models"
	"github.com/artie-labs/transfer/models/event"
	"golang.org/x/exp/maps"
)

// toJSONTypes converts data to JSON and back so that the format is consistent with what is in Kafka.
func toJSONTypes(data map[string]any) (map[string]any, error) {
	dataBytes, err := json.Marshal(data)
	if err != nil {
		return nil, err
	}
	var result map[string]any
	if err = json.Unmarshal(dataBytes, &result); err != nil {
		return nil, err
	}
	return result, nil
}

type Writer struct {
	// TODO: How do we generate this
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

func (w *Writer) WriteRawMessages(_ context.Context, rawMsgs []lib.RawMessage) error {
	if len(rawMsgs) == 0 {
		return nil
	}

	var events []event.Event
	for _, rawMsg := range rawMsgs {
		evt := rawMsg.Event()
		if payload, ok := evt.(*util.SchemaEventPayload); ok {
			var err error
			payload.Payload.After, err = toJSONTypes(payload.Payload.After)
			if err != nil {
				return err
			}
		}

		events = append(events, event.ToMemoryEvent(evt, rawMsg.PartitionKey(), w.tc, config.Replication))
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
			if err := w.Flush(flushReason); err != nil {
				return err
			}
		}
	}

	return nil
}

func (w *Writer) GetTableData() (string, *models.TableData) {
	tableData := w.inMemDB.TableData()
	keys := maps.Keys(tableData)
	if len(keys) != 1 {
		panic("expected exactly one table")
	}
	return keys[0], tableData[keys[0]]
}

func (w *Writer) Flush(reason string) error {
	tableName, tableData := w.GetTableData()

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

func (w *Writer) OnFinish() error {
	if err := w.Flush("complete"); err != nil {
		return err
	}
	// TODO: Run de-duplicate logic here.
	return nil
}
