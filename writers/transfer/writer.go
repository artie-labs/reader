package transfer

import (
    "context"
    "encoding/json"
    "fmt"
    "log/slog"
    "time"

    bqDialect "github.com/artie-labs/transfer/clients/bigquery/dialect"
    "github.com/artie-labs/transfer/clients/mssql/dialect"
    "github.com/artie-labs/transfer/lib/artie"
    "github.com/artie-labs/transfer/lib/cdc/mongo"
    "github.com/artie-labs/transfer/lib/config"
    "github.com/artie-labs/transfer/lib/config/constants"
    "github.com/artie-labs/transfer/lib/destination"
    "github.com/artie-labs/transfer/lib/destination/utils"
    "github.com/artie-labs/transfer/lib/kafkalib"
    "github.com/artie-labs/transfer/models"
    "github.com/artie-labs/transfer/models/event"

    "github.com/artie-labs/reader/lib"
    "github.com/artie-labs/reader/lib/mtr"
)

type Writer struct {
    cfg         config.Config
    statsD      mtr.Client
    inMemDB     *models.DatabaseData
    tc          *kafkalib.TopicConfig
    destination destination.Baseline

    primaryKeys []string
}

func NewWriter(cfg config.Config, statsD mtr.Client) (*Writer, error) {
    if cfg.Kafka == nil {
        return nil, fmt.Errorf("kafka config should not be nil")
    }

    if len(cfg.Kafka.TopicConfigs) != 1 {
        return nil, fmt.Errorf("kafka config should have exactly one topic config")
    }

    writer := &Writer{
        cfg:     cfg,
        statsD:  statsD,
        inMemDB: models.NewMemoryDB(),
        tc:      cfg.Kafka.TopicConfigs[0],
    }

    if utils.IsOutputBaseline(cfg) {
        baseline, err := utils.LoadBaseline(cfg)
        if err != nil {
            return nil, err
        }

        writer.destination = baseline
    } else {
        _destination, err := utils.LoadDataWarehouse(cfg, nil)
        if err != nil {
            return nil, err
        }

        writer.destination = _destination
    }

    return writer, nil
}

func (w *Writer) messageToEvent(message lib.RawMessage) (event.Event, error) {
    evt := message.Event()
    if mongoEvt, ok := evt.(*mongo.SchemaEventPayload); ok {
        bytes, err := json.Marshal(mongoEvt)
        if err != nil {
            return event.Event{}, err
        }

        var dbz mongo.Debezium
        evt, err = dbz.GetEventFromBytes(w.cfg.SharedTransferConfig.TypingSettings, bytes)
        if err != nil {
            return event.Event{}, err
        }

        partitionKeyBytes, err := json.Marshal(message.PartitionKey())
        if err != nil {
            return event.Event{}, err
        }

        partitionKey, err := dbz.GetPrimaryKey(partitionKeyBytes, w.tc)
        if err != nil {
            return event.Event{}, err
        }

        return event.ToMemoryEvent(evt, partitionKey, w.tc, config.Replication)
    }

    memoryEvent, err := event.ToMemoryEvent(evt, message.PartitionKey(), w.tc, config.Replication)
    if err != nil {
        return event.Event{}, err
    }

    // Setting the deleted column flag.
    memoryEvent.Data[constants.DeleteColumnMarker] = false
    return memoryEvent, nil
}

func (w *Writer) Write(_ context.Context, messages []lib.RawMessage) error {
    if len(messages) == 0 {
        return nil
    }

    var events []event.Event
    for _, message := range messages {
        evt, err := w.messageToEvent(message)
        if err != nil {
            return err
        }
        events = append(events, evt)
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
        if w.statsD != nil {
            w.statsD.Count("process.message", int64(len(events)), tags)
        }
    }()

    for _, evt := range events {
        // Set the primary keys if it's not set already.
        if len(w.primaryKeys) == 0 {
            var pks []string
            for key := range evt.PrimaryKeyMap {
                pks = append(pks, key)
            }

            w.primaryKeys = pks
        }

        shouldFlush, flushReason, err := evt.Save(w.cfg, w.inMemDB, w.tc, artie.Message{})
        if err != nil {
            return fmt.Errorf("failed to save event: %w", err)
        }

        if shouldFlush {
            if err = w.flush(flushReason); err != nil {
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
        "database": tableData.TopicConfig().Database,
        "schema":   tableData.TopicConfig().Schema,
        "reason":   reason,
    }
    defer func() {
        if w.statsD != nil {
            w.statsD.Timing("flush", time.Since(start), tags)
        }
    }()

    tableData.ResetTempTableSuffix()
    if isMicrosoftSQLServer(w.destination) {
        // Microsoft SQL Server uses MERGE not append
        if err = w.destination.Merge(tableData.TableData); err != nil {
            tags["what"] = "merge_fail"
            tags["retryable"] = fmt.Sprint(w.destination.IsRetryableError(err))
            return fmt.Errorf("failed to merge data to destination: %w", err)
        }
    } else {
        // We should hide this column from getting added
        if !tableData.TopicConfig().SoftDelete {
            tableData.InMemoryColumns().DeleteColumn(constants.DeleteColumnMarker)
        }

        if err = w.destination.Append(tableData.TableData, isBigQuery(w.destination)); err != nil {
            tags["what"] = "merge_fail"
            tags["retryable"] = fmt.Sprint(w.destination.IsRetryableError(err))
            return fmt.Errorf("failed to append data to destination: %w", err)
        }
    }

    w.inMemDB.ClearTableConfig(tableName)
    return nil
}

func (w *Writer) OnComplete() error {
    if len(w.primaryKeys) == 0 {
        return fmt.Errorf("primary keys not set")
    }

    if err := w.flush("complete"); err != nil {
        return fmt.Errorf("failed to flush: %w", err)
    }

    tableName, _, err := w.getTableData()
    if err != nil {
        return err
    }

    if isMicrosoftSQLServer(w.destination) {
        // We don't need to run dedupe because it's just merging.
        return nil
    }

    slog.Info("Running dedupe...", slog.String("table", tableName))
    tableID := w.destination.IdentifierFor(*w.tc, tableName)
    start := time.Now()

    dwh, isOk := w.destination.(destination.DataWarehouse)
    if !isOk {
        return nil
    }

    if err = dwh.Dedupe(tableID, w.primaryKeys, w.tc.IncludeArtieUpdatedAt); err != nil {
        return err
    }

    slog.Info("Dedupe complete", slog.String("table", tableName), slog.Duration("duration", time.Since(start)))
    return nil
}

func isMicrosoftSQLServer(baseline destination.Baseline) bool {
    dwh, isOk := baseline.(destination.DataWarehouse)
    if !isOk {
        return false
    }

    _, isOk = dwh.Dialect().(dialect.MSSQLDialect)
    return isOk
}

func isBigQuery(baseline destination.Baseline) bool {
    dwh, isOk := baseline.(destination.DataWarehouse)
    if !isOk {
        return false
    }

    _, isOk = dwh.Dialect().(bqDialect.BigQueryDialect)
    return isOk
}
