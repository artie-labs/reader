package streaming

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/artie-labs/transfer/lib/typing"
	"github.com/go-mysql-org/go-mysql/replication"

	"github.com/artie-labs/reader/lib"
	"github.com/artie-labs/reader/lib/debezium/transformer"
	"github.com/artie-labs/reader/lib/maputil"
	"github.com/artie-labs/reader/sources/mysql/adapter"
)

const offsetKey = "offset"

func (p *Position) UpdatePosition(evt *replication.BinlogEvent) error {
	// We should always update the log position
	p.Pos = evt.Header.LogPos
	if evt.Header.EventType == replication.ROTATE_EVENT {
		// When we encounter a rotate event, we'll then update the log file
		rotate, err := typing.AssertType[*replication.RotateEvent](evt.Event)
		if err != nil {
			return err
		}

		p.File = string(rotate.NextLogName)
	}

	return nil
}

func (i *Iterator) Close() error {
	i.syncer.Close()
	return nil
}

func (i *Iterator) Next() ([]lib.RawMessage, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var rawMsgs []lib.RawMessage
	for i.batchSize > int32(len(rawMsgs)) {
		select {
		case <-ctx.Done():
			return rawMsgs, nil
		default:
			event, err := i.streamer.GetEvent(ctx)
			if err != nil {
				if errors.Is(err, context.DeadlineExceeded) {
					return rawMsgs, nil
				}

				return nil, fmt.Errorf("failed to get binlog event: %w", err)
			}

			ts := getTimeFromEvent(event)

			// Update position
			if err = i.position.UpdatePosition(event); err != nil {
				return nil, fmt.Errorf("failed to update position: %w", err)
			}
			switch event.Header.EventType {
			case replication.QUERY_EVENT:
				query, err := typing.AssertType[*replication.QueryEvent](event.Event)
				if err != nil {
					return nil, fmt.Errorf("failed to assert a query event: %w", err)
				}

				fmt.Println("query", string(query.Query), "errorCode", query.ErrorCode)
				// TODO: Process the DDL event
			case replication.WRITE_ROWS_EVENTv2, replication.UPDATE_ROWS_EVENTv2, replication.DELETE_ROWS_EVENTv2:
				rowsEvent, err := typing.AssertType[*replication.RowsEvent](event.Event)
				if err != nil {
					return nil, fmt.Errorf("failed to assert a rows event: %w", err)
				}

				operation, err := convertHeaderToOperation(event.Header.EventType)
				if err != nil {
					return nil, fmt.Errorf("failed to convert header to operation: %w", err)
				}

				mrm, isOk := i.shouldProcessTable(rowsEvent.Table.Table)
				if !isOk {
					continue
				}

				beforeAndAfters, err := splitIntoBeforeAndAfter(operation, rowsEvent.Rows)
				if err != nil {
					return nil, err
				}

				tableAdapter, isOk := mrm.GetItem(ts.UnixMilli())
				if !isOk {
					slog.Info("Skipping event as table adapter not found", slog.Any("timestamp", ts.UnixMilli()))
					continue
				}

				fieldConverters, err := tableAdapter.GetFieldConverters()
				if err != nil {
					return nil, fmt.Errorf("failed to get field converters: %w", err)
				}

				dbz := transformer.NewLightDebeziumTransformer(string(rowsEvent.Table.Table), tableAdapter.GetPrimaryKeys(), fieldConverters)
				for before, after := range beforeAndAfters {
					var beforeRow map[string]any
					if len(before) > 0 {
						beforeRow, err = zipSlicesToMap(tableAdapter.GetTableColumnNames(), before)
						if err != nil {
							return nil, fmt.Errorf("failed to convert row to map:%w", err)
						}
					}

					var afterRow map[string]any
					if len(after) > 0 {
						afterRow, err = zipSlicesToMap(tableAdapter.GetTableColumnNames(), after)
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

					rawMsgs = append(rawMsgs, lib.NewRawMessage(tableAdapter.TopicSuffix(), pk, &dbzMessage))
				}
			default:
				slog.Info("Skipping event", slog.Any("event", event.Header.EventType))
			}
		}
	}

	if len(rawMsgs) == 0 {
		// If there are no messages, let's sleep a bit before we try again
		time.Sleep(2 * time.Second)
	}

	return rawMsgs, nil
}
func (i *Iterator) shouldProcessTable(tableName []byte) (*maputil.MostRecentMap[adapter.Table], bool) {
	mrm, isOk := i.includedTablesAdapter[string(tableName)]
	return mrm, isOk
}

func (i *Iterator) HasNext() bool {
	return true
}

func (i *Iterator) CommitOffset() {
	slog.Info("Committing offset", slog.String("position", i.position.String()))
	i.offsets.Set(offsetKey, i.position)
}
