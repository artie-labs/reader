package streaming

import (
	"context"
	"fmt"
	"log/slog"
	"time"
	
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/go-mysql-org/go-mysql/replication"

	"github.com/artie-labs/reader/config"
	"github.com/artie-labs/reader/lib"
	"github.com/artie-labs/reader/lib/maputil"
	"github.com/artie-labs/reader/lib/storage/persistedmap"
	"github.com/artie-labs/reader/sources/mysql/adapter"
)

const offsetKey = "offset"

type Iterator struct {
	batchSize             int32
	offsets               *persistedmap.PersistedMap
	streamer              *replication.BinlogStreamer
	position              Position
	includedTablesAdapter map[string]*maputil.MostRecentMap[adapter.Table]

	// TODO
	schemaHistory *persistedmap.PersistedMap
}

func (i *Iterator) Next() ([]lib.RawMessage, error) {
	fmt.Println("next here")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
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
				return nil, fmt.Errorf("failed to get binlog event: %w", err)
			}

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

				fmt.Println("query", query)
				// TODO: Process the DDL event
			case replication.WRITE_ROWS_EVENTv2, replication.UPDATE_ROWS_EVENTv2, replication.DELETE_ROWS_EVENTv2:
				rowsEvent, err := typing.AssertType[*replication.RowsEvent](event.Event)
				if err != nil {
					return nil, fmt.Errorf("failed to assert a rows event: %w", err)
				}

				if !i.shouldProcessTable(rowsEvent.Table.Table) {
					continue
				}

				// TODO: Process the event and convert it to lib.RawMessage
				// rawMessage := processRowsEvent(rowsEvent)
				// rawMsgs = append(rawMsgs, rawMessage)
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
func (i *Iterator) shouldProcessTable(tableName []byte) bool {
	_, isOk := i.includedTablesAdapter[string(tableName)]
	return isOk
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
	offsets := persistedmap.NewPersistedMap(cfg.StreamingSettings.OffsetFile)
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
