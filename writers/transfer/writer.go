package transfer

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	bqDialect "github.com/artie-labs/transfer/clients/bigquery/dialect"
	"github.com/artie-labs/transfer/clients/mssql/dialect"
	"github.com/artie-labs/transfer/lib/artie"
	"github.com/artie-labs/transfer/lib/cdc/mongo"
	transferConfig "github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/destination"
	"github.com/artie-labs/transfer/lib/destination/ddl"
	"github.com/artie-labs/transfer/lib/destination/utils"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/sql"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
	"github.com/artie-labs/transfer/models"
	"github.com/artie-labs/transfer/models/event"

	"github.com/artie-labs/reader/config"
	readerKafkaLib "github.com/artie-labs/reader/lib/kafkalib"
	"github.com/artie-labs/reader/lib/mtr"
)

// buildColumns - This will append additional columns based on [kafkalib.TopicConfig]
func buildColumns(cols []columns.Column, tc kafkalib.TopicConfig) []columns.Column {
	if tc.IncludeArtieUpdatedAt {
		cols = append(cols, columns.NewColumn(constants.UpdateColumnMarker, typing.TimestampTZ))
	}

	if tc.IncludeDatabaseUpdatedAt {
		cols = append(cols, columns.NewColumn(constants.DatabaseUpdatedColumnMarker, typing.TimestampTZ))
	}

	if tc.SoftDelete {
		cols = append(cols, columns.NewColumn(constants.DeleteColumnMarker, typing.Boolean))
	}

	return cols
}

type Writer struct {
	cfg         transferConfig.Config
	statsD      mtr.Client
	inMemDB     *models.DatabaseData
	tc          kafkalib.TopicConfig
	destination destination.Baseline

	primaryKeys []string

	beforeBackfill     config.BeforeBackfill
	ranOnBackfillStart bool
}

func NewWriter(cfg transferConfig.Config, statsD mtr.Client, beforeBackfill config.BeforeBackfill) (*Writer, error) {
	if cfg.Kafka == nil {
		return nil, fmt.Errorf("kafka config should not be nil")
	}

	if len(cfg.Kafka.TopicConfigs) != 1 {
		return nil, fmt.Errorf("kafka config should have exactly one topic config")
	}

	writer := &Writer{
		cfg:            cfg,
		statsD:         statsD,
		inMemDB:        models.NewMemoryDB(),
		tc:             *cfg.Kafka.TopicConfigs[0],
		beforeBackfill: beforeBackfill,
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

func (w *Writer) messageToEvent(message readerKafkaLib.Message) (event.Event, error) {
	evt := message.Event()
	if mongoEvt, ok := evt.(*mongo.SchemaEventPayload); ok {
		bytes, err := json.Marshal(mongoEvt)
		if err != nil {
			return event.Event{}, err
		}

		var dbz mongo.Debezium
		evt, err = dbz.GetEventFromBytes(bytes)
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

		return event.ToMemoryEvent(evt, partitionKey, w.tc, transferConfig.Replication)
	}

	memoryEvent, err := event.ToMemoryEvent(evt, message.PartitionKeyValues(), w.tc, transferConfig.Replication)
	if err != nil {
		return event.Event{}, err
	}

	// Setting the deleted column flag.
	memoryEvent.Data[constants.DeleteColumnMarker] = false
	return memoryEvent, nil
}

func (w *Writer) CreateTable(ctx context.Context, tableName string, cols []columns.Column) error {
	dwh, ok := w.destination.(destination.DataWarehouse)
	if !ok {
		// Don't create the table if it's not a data warehouse.
		return nil
	}

	// We should include additional columns based in the typing config
	createTableSQL, err := ddl.BuildCreateTableSQL(w.cfg.SharedDestinationSettings.ColumnSettings, dwh.Dialect(), w.getTableID(tableName), false, w.cfg.Mode, buildColumns(cols, w.tc))
	if err != nil {
		return fmt.Errorf("failed to build create table SQL: %w", err)
	}

	if _, err = dwh.ExecContext(ctx, createTableSQL); err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	return nil
}

func (w *Writer) dropTable(ctx context.Context, tableName string) error {
	dwh, ok := w.destination.(destination.DataWarehouse)
	if !ok {
		return nil
	}

	tableID := w.getTableID(tableName)

	slog.Info("Dropping table before backfill...", slog.String("table", tableID.FullyQualifiedName()))
	_, err := dwh.ExecContext(ctx, dwh.Dialect().BuildDropTableQuery(tableID))
	return err
}

func (w *Writer) truncateTable(ctx context.Context, tableName string) error {
	dwh, ok := w.destination.(destination.DataWarehouse)
	if !ok {
		return nil
	}

	tableID := w.getTableID(tableName)

	slog.Info("Truncating table before backfill...", slog.String("table", tableID.FullyQualifiedName()))
	_, err := dwh.ExecContext(ctx, dwh.Dialect().BuildTruncateTableQuery(tableID))
	return err
}

func (w *Writer) Write(ctx context.Context, messages []readerKafkaLib.Message) error {
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

	if !w.ranOnBackfillStart {
		w.ranOnBackfillStart = true
		if err := w.onBackfillStart(ctx, events[0].Table); err != nil {
			return fmt.Errorf("failed running onBackfillStart: %w", err)
		}
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
			if err = w.flush(ctx, flushReason); err != nil {
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

func (w *Writer) flush(ctx context.Context, reason string) error {
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
		if err = w.destination.Merge(ctx, tableData.TableData); err != nil {
			tags["what"] = "merge_fail"
			tags["retryable"] = fmt.Sprint(w.destination.IsRetryableError(err))
			return fmt.Errorf("failed to merge data to destination: %w", err)
		}
	} else {
		// We should hide this column from getting added
		if !tableData.TopicConfig().SoftDelete {
			tableData.InMemoryColumns().DeleteColumn(constants.DeleteColumnMarker)
		}

		tableData.InMemoryColumns().DeleteColumn(constants.OnlySetDeleteColumnMarker)
		if err = w.destination.Append(ctx, tableData.TableData, isBigQuery(w.destination)); err != nil {
			tags["what"] = "merge_fail"
			tags["retryable"] = fmt.Sprint(w.destination.IsRetryableError(err))
			return fmt.Errorf("failed to append data to destination: %w", err)
		}
	}

	w.inMemDB.ClearTableConfig(tableName)
	return nil
}

func (w *Writer) getTableID(tableName string) sql.TableIdentifier {
	// [w.tc.TableName] could be empty, in that case we'll fall back on [tableName]
	return w.destination.IdentifierFor(w.tc, cmp.Or(w.tc.TableName, tableName))
}

func (w *Writer) onBackfillStart(ctx context.Context, tableName string) error {
	switch w.beforeBackfill {
	case config.BeforeBackfillDoNothing:
		return nil
	case config.BeforeBackfillTruncateTable:
		if err := w.truncateTable(ctx, tableName); err != nil {
			return fmt.Errorf("failed to truncate table: %w", err)
		}
		return nil
	case config.BeforeBackfillDropTable:
		if err := w.dropTable(ctx, tableName); err != nil {
			return fmt.Errorf("failed to drop table: %w", err)
		}
		return nil
	default:
		return fmt.Errorf("unsupported before backfill value: %q", w.beforeBackfill)
	}
}

func (w *Writer) OnComplete(ctx context.Context) error {
	if len(w.primaryKeys) == 0 {
		return fmt.Errorf("primary keys not set")
	}

	if err := w.flush(ctx, "complete"); err != nil {
		return fmt.Errorf("failed to flush: %w", err)
	}

	tableName, _, err := w.getTableData()
	if err != nil {
		return err
	}

	tableID := w.getTableID(tableName)
	if isMicrosoftSQLServer(w.destination) {
		// We don't need to run dedupe because it's just merging.
		return nil
	}

	slog.Info("Running dedupe...", slog.String("table", tableID.Table()))
	start := time.Now()

	dwh, isOk := w.destination.(destination.DataWarehouse)
	if !isOk {
		return nil
	}

	if err = dwh.Dedupe(tableID, w.primaryKeys, w.tc.IncludeArtieUpdatedAt); err != nil {
		return err
	}

	slog.Info("Dedupe complete",
		slog.String("table", tableID.Table()),
		slog.Duration("duration", time.Since(start)),
	)
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
