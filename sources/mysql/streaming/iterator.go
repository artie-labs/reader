package streaming

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/artie-labs/transfer/lib/typing"
	"github.com/go-mysql-org/go-mysql/replication"

	"github.com/artie-labs/reader/config"
	"github.com/artie-labs/reader/lib"
	"github.com/artie-labs/reader/lib/debezium/transformer"
	"github.com/artie-labs/reader/lib/maputil"
	"github.com/artie-labs/reader/lib/storage/persistedmap"
	"github.com/artie-labs/reader/sources/mysql/adapter"
)

const offsetKey = "offset"

type Iterator struct {
	batchSize             int32
	offsets               *persistedmap.PersistedMap[Position]
	streamer              *replication.BinlogStreamer
	position              Position
	includedTablesAdapter map[string]*maputil.MostRecentMap[adapter.Table]

	// TODO
	schemaHistory *persistedmap.PersistedMap[any]
}

func (i *Iterator) Next() ([]lib.RawMessage, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var rawMsgs []lib.RawMessage
	for i.batchSize > int32(len(rawMsgs)) {
		select {
		case <-ctx.Done():
			fmt.Println("returning")
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
			fmt.Println("ts", ts)

			// Update the position
			i.position.Pos = event.Header.LogPos
			switch event.Header.EventType {
			case replication.ROTATE_EVENT:
				rotate, err := typing.AssertType[*replication.RotateEvent](event.Event)
				if err != nil {
					return nil, fmt.Errorf("failed to assert a rotate event: %w", err)
				}

				i.position = Position{File: string(rotate.NextLogName)}
			case replication.QUERY_EVENT:
				query, err := typing.AssertType[*replication.QueryEvent](event.Event)
				if err != nil {
					return nil, fmt.Errorf("failed to assert a query event: %w", err)
				}

				// TODO: Ensure DDL status
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

					fmt.Println("rawMsgs", rawMsgs)
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
	i.offsets.Set(offsetKey, i.position)
	slog.Info("Committing offset", slog.Any("position", i.position))
}

func BuildStreamingIterator(cfg config.MySQL) (*Iterator, error) {
	var pos Position
	offsets := persistedmap.NewPersistedMap[Position](cfg.StreamingSettings.OffsetFile)
	value, isOk := offsets.Get(offsetKey)
	if isOk {
		_pos, err := typing.AssertType[Position](value)
		if err != nil {
			return nil, err
		}

		slog.Info("Found offsets", slog.String("offset", pos.String()))
		pos = _pos
	}

	includedTablesAdapter, err := BuildTablesAdapter(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to build schema history: %w", err)
	}

	syncer := replication.NewBinlogSyncer(
		replication.BinlogSyncerConfig{
			ServerID: cfg.StreamingSettings.ServerID,
			Flavor:   "mysql",
			Host:     cfg.Host,
			Port:     uint16(cfg.Port),
			User:     cfg.Username,
			Password: cfg.Password,
		},
	)

	streamer, err := syncer.StartSync(pos.ToMySQLPosition())
	if err != nil {
		return nil, fmt.Errorf("failed to start sync: %w", err)
	}

	return &Iterator{
		batchSize:             cfg.GetStreamingBatchSize(),
		position:              pos,
		streamer:              streamer,
		includedTablesAdapter: includedTablesAdapter,
		offsets:               offsets,
	}, nil
}
