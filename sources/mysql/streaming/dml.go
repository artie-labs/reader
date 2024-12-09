package streaming

import (
	"fmt"
	"log/slog"
	"time"

	"github.com/artie-labs/transfer/lib/typing"
	"github.com/go-mysql-org/go-mysql/replication"

	"github.com/artie-labs/reader/lib"
	"github.com/artie-labs/reader/lib/debezium/transformer"
)

func (i *Iterator) processDML(ts time.Time, event *replication.BinlogEvent) ([]lib.RawMessage, error) {
	rowsEvent, err := typing.AssertType[*replication.RowsEvent](event.Event)
	if err != nil {
		return nil, fmt.Errorf("failed to assert a rows event: %w", err)
	}

	tableName := string(rowsEvent.Table.Table)
	tblAdapter, ok := i.getTableAdapter(tableName)
	if !ok {
		return nil, nil
	}

	if tblAdapter.unixTs > ts.Unix() {
		slog.Warn("Skipping this event since the event timestamp is older than the schema timestamp",
			slog.Int64("event_ts", ts.Unix()),
			slog.Int64("schema_ts", tblAdapter.unixTs),
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
	fieldConverters, err := tblAdapter.GetFieldConverters()
	if err != nil {
		return nil, fmt.Errorf("failed to get field converters: %w", err)
	}

	parsedColumns, err := tblAdapter.GetParsedColumns()
	if err != nil {
		return nil, fmt.Errorf("failed to get parsed columns: %w", err)
	}

	dbz := transformer.NewLightDebeziumTransformer(tableName, tblAdapter.PartitionKeys(), fieldConverters)
	for before, after := range beforeAndAfters {
		var beforeRow map[string]any
		if len(before) > 0 {
			beforeRow, err = zipSlicesToMap[string](tblAdapter.ColumnNames(), before)
			if err != nil {
				return nil, fmt.Errorf("failed to convert row to map:%w", err)
			}
		}

		var afterRow map[string]any
		if len(after) > 0 {
			afterRow, err = zipSlicesToMap[string](tblAdapter.ColumnNames(), after)
			if err != nil {
				return nil, fmt.Errorf("failed to convert row to map:%w", err)
			}
		}

		fmt.Println("beforeRow", beforeRow, "afterRow", afterRow)

		// Preprocess
		beforeRow, err = preprocessRow(beforeRow, parsedColumns)
		if err != nil {
			return nil, fmt.Errorf("failed to preprocess before row: %w", err)
		}

		afterRow, err = preprocessRow(afterRow, parsedColumns)
		if err != nil {
			return nil, fmt.Errorf("failed to preprocess after row: %w", err)
		}

		dbzMessage, err := dbz.BuildEventPayload(beforeRow, afterRow, operation, ts)
		if err != nil {
			return nil, fmt.Errorf("failed to build event payload: %w", err)
		}

		pk, err := dbz.BuildPartitionKey(beforeRow, afterRow)
		if err != nil {
			return nil, fmt.Errorf("failed to build partition key: %w", err)
		}

		rawMsgs = append(rawMsgs, lib.NewRawMessage(tblAdapter.TopicSuffix(), pk, &dbzMessage))
	}

	return rawMsgs, nil
}
