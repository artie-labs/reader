package streaming

import (
	"fmt"
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
	_, ok := i.shouldProcessTable(tableName)
	if !ok {
		return nil, nil
	}

	// TODO: We should check that tableAdapter's timestamp is not greater than the event's timestamp.

	operation, err := convertHeaderToOperation(event.Header.EventType)
	if err != nil {
		return nil, fmt.Errorf("failed to convert header to operation: %w", err)
	}

	beforeAndAfters, err := splitIntoBeforeAndAfter(operation, rowsEvent.Rows)
	if err != nil {
		return nil, err
	}

	var rawMsgs []lib.RawMessage
	// TODO: Provide partitionKeys and fieldConverters
	dbz := transformer.NewLightDebeziumTransformer(tableName, nil, nil)
	for before, after := range beforeAndAfters {
		var beforeRow map[string]any
		if len(before) > 0 {
			// TODO - Provide columns
			beforeRow, err = zipSlicesToMap[string](nil, before)
			if err != nil {
				return nil, fmt.Errorf("failed to convert row to map:%w", err)
			}
		}

		var afterRow map[string]any
		if len(after) > 0 {
			// TODO - Provide columns
			afterRow, err = zipSlicesToMap[string](nil, after)
			if err != nil {
				return nil, fmt.Errorf("failed to convert row to map:%w", err)
			}
		}

		dbzMessage, err := dbz.BuildEventPayload(beforeRow, afterRow, operation, ts)
		if err != nil {
			return nil, fmt.Errorf("failed to build event payload: %w", err)
		}

		// TODO: Check afterRow exists for a deleted row.
		pk, err := dbz.BuildPartitionKey(afterRow)
		if err != nil {
			return nil, fmt.Errorf("failed to build partition key: %w", err)
		}

		// TODO - Suffix
		rawMsgs = append(rawMsgs, lib.NewRawMessage("", pk, &dbzMessage))
	}

	return rawMsgs, nil
}
