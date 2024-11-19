package mysql

import (
	"context"
	"fmt"
	"github.com/artie-labs/reader/lib/maputil"
	"github.com/artie-labs/reader/sources/mysql/adapter"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/go-mysql-org/go-mysql/replication"
	"log/slog"

	"github.com/artie-labs/reader/config"
	"github.com/artie-labs/reader/lib/storage/persistedmap"
	"github.com/artie-labs/reader/sources/mysql/streaming"
	"github.com/artie-labs/reader/writers"
)

const offsetKey = "offset"

type Streaming struct {
	schemaHistory         *persistedmap.PersistedMap
	storage               *persistedmap.PersistedMap
	syncer                *replication.BinlogSyncer
	position              streaming.Position
	includedTablesAdapter map[string]*maputil.MostRecentMap[adapter.Table]
}

func (s Streaming) shouldProcessTable(tableName []byte) bool {
	_, isOk := s.includedTablesAdapter[string(tableName)]
	return isOk
}

func (s Streaming) Close() error {
	s.syncer.Close()
	return nil
}

func buildStreamingConfig(cfg config.MySQL) (Streaming, error) {
	streamer := Streaming{
		schemaHistory: persistedmap.NewPersistedMap(cfg.StreamingSettings.SchemaHistoryFile),
		storage:       persistedmap.NewPersistedMap(cfg.StreamingSettings.OffsetFile),
		syncer: replication.NewBinlogSyncer(
			replication.BinlogSyncerConfig{
				ServerID: cfg.StreamingSettings.ServerID,
				Flavor:   "mysql",
				Host:     cfg.Host,
				Port:     uint16(cfg.Port),
				User:     cfg.Username,
				Password: cfg.Password,
			},
		),
	}

	value, isOk := streamer.storage.Get(offsetKey)
	if isOk {
		pos, err := typing.AssertType[streaming.Position](value)
		if err != nil {
			return Streaming{}, err
		}

		slog.Info("Loaded offsets", slog.String("offset", pos.String()))
		streamer.position = pos
	}

	// TODO: Iterate over the schema history file and add tables to [MostRecentMap]
	includedTablesAdapter, err := streaming.BuildTablesAdapter(cfg)
	if err != nil {
		return Streaming{}, fmt.Errorf("failed to build schema history: %w", err)
	}

	streamer.includedTablesAdapter = includedTablesAdapter
	return streamer, nil
}

func (s Streaming) Run(ctx context.Context, writer writers.Writer) error {
	streamer, err := s.syncer.StartSync(s.position.ToMySQLPosition())
	if err != nil {
		return err
	}
	for {
		event, err := streamer.GetEvent(ctx)
		if err != nil {
			return fmt.Errorf("failed to get binlog event: %w", err)
		}

		switch event.Header.EventType {
		case replication.QUERY_EVENT:
			query, err := typing.AssertType[*replication.QueryEvent](event.Event)
			if err != nil {
				return err
			}

			//  TODO: Process the DDL event
			fmt.Println("query", query)
		case
			replication.WRITE_ROWS_EVENTv2,
			replication.UPDATE_ROWS_EVENTv2,
			replication.DELETE_ROWS_EVENTv2:
			rowsEvent, ok := event.Event.(*replication.RowsEvent)
			if !ok {
				return fmt.Errorf("unable to cast event to replication.RowsEvent")
			}

			if !s.shouldProcessTable(rowsEvent.Table.Table) {
				continue
			}

			// TODO: Process the event
		default:
			slog.Info("Skipping event", slog.Any("event", event.Header.EventType))
		}
	}
}
