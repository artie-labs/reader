package streaming

import (
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/artie-labs/transfer/lib/typing"
	"github.com/go-mysql-org/go-mysql/replication"

	"github.com/artie-labs/reader/lib"
	"github.com/artie-labs/reader/lib/debezium/transformer"
)

func (i *Iterator) processDML(ts time.Time, event *replication.BinlogEvent, currentGTID *string) ([]lib.RawMessage, error) {
	rowsEvent, err := typing.AssertType[*replication.RowsEvent](event.Event)
	if err != nil {
		return nil, fmt.Errorf("failed to assert a rows event: %w", err)
	}

	if !strings.EqualFold(i.cfg.Database, string(rowsEvent.Table.Schema)) {
		slog.Debug("Skipping this event since the database does not match the configured database",
			slog.String("config_db", i.cfg.Database),
			slog.String("event_db", string(rowsEvent.Table.Schema)),
		)

		return nil, nil
	}

	tableName := string(rowsEvent.Table.Table)
	tblAdapter, ok := i.schemaAdapter.GetTableAdapter(tableName)
	if !ok {
		return nil, nil
	}

	if !tblAdapter.ShouldReplicate() {
		return nil, nil
	}

	if tblAdapter.GetUnixTs() > ts.Unix() {
		slog.Debug("Skipping this event since the event timestamp is older than the schema timestamp",
			slog.Int64("event_ts", ts.Unix()),
			slog.Int64("schema_ts", tblAdapter.GetUnixTs()),
		)

		return nil, nil
	}

	operation, err := convertHeaderToOperation(event.Header.EventType)
	if err != nil {
		return nil, fmt.Errorf("failed to convert header to operation: %w", err)
	}

	beforeAndAfters, err := splitIntoBeforeAndAfter(operation, rowsEvent.Rows)
	if err != nil {
		return nil, err
	}

	var rawMsgs []lib.RawMessage
	parsedColumns := tblAdapter.GetParsedColumns()
	if err != nil {
		return nil, fmt.Errorf("failed to get parsed columns: %w", err)
	}

	sourcePayload := buildDebeziumSourcePayload(i.cfg.Database, tableName, ts, i.position, currentGTID)
	dbz := transformer.NewLightDebeziumTransformer(tableName, tblAdapter.PartitionKeys(), tblAdapter.GetFieldConverters())
	for before, after := range beforeAndAfters {
		var beforeRow map[string]any
		if len(before) > 0 {
			beforeRow, err = zipSlicesToMap[string](tblAdapter.ColumnNames(), before)
			if err != nil {
				return nil, fmt.Errorf("failed to convert before row to map:%w", err)
			}
		}

		var afterRow map[string]any
		if len(after) > 0 {
			afterRow, err = zipSlicesToMap[string](tblAdapter.ColumnNames(), after)
			if err != nil {
				return nil, fmt.Errorf("failed to convert row to map for table %q: %w", tableName, err)
			}
		}

		// Preprocess
		beforeRow, err = preprocessRow(beforeRow, parsedColumns)
		if err != nil {
			return nil, fmt.Errorf("failed to preprocess before row: %w", err)
		}

		afterRow, err = preprocessRow(afterRow, parsedColumns)
		if err != nil {
			return nil, fmt.Errorf("failed to preprocess after row: %w", err)
		}

		dbzMessage, err := dbz.BuildEventPayload(sourcePayload, beforeRow, afterRow, operation)
		if err != nil {
			return nil, fmt.Errorf("failed to build event payload: %w", err)
		}

		primaryKeyPayload, err := dbz.BuildPartitionKey(beforeRow, afterRow)
		if err != nil {
			return nil, fmt.Errorf("failed to build partition key: %w", err)
		}

		if len(primaryKeyPayload.Payload) == 0 {
			return nil, fmt.Errorf("partition key is not set for table: %q", tableName)
		}

		rawMsgs = append(rawMsgs, lib.NewRawMessage(tblAdapter.TopicSuffix(), primaryKeyPayload.Schema, primaryKeyPayload.Payload, &dbzMessage))
	}

	return rawMsgs, nil
}
