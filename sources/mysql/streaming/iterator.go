package streaming

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"time"

	"github.com/artie-labs/transfer/lib/typing"
	"github.com/go-mysql-org/go-mysql/replication"

	"github.com/artie-labs/reader/config"
	"github.com/artie-labs/reader/lib"
	"github.com/artie-labs/reader/lib/mysql/schema"
	"github.com/artie-labs/reader/lib/storage/persistedlist"
	"github.com/artie-labs/reader/lib/storage/persistedmap"
)

func buildSchemaAdapter(db *sql.DB, cfg config.MySQL, schemaHistoryList persistedlist.PersistedList[SchemaHistory], pos Position) (SchemaAdapter, error) {
	var latestSchemaUnixTs int64
	schemaAdapter := SchemaAdapter{adapters: make(map[string]TableAdapter)}
	for _, schemaHistory := range schemaHistoryList.GetData() {
		if err := schemaAdapter.ApplyDDL(schemaHistory.Query, schemaHistory.UnixTs); err != nil {
			return SchemaAdapter{}, fmt.Errorf("failed to apply DDL: %w", err)
		}

		latestSchemaUnixTs = schemaHistory.UnixTs
	}

	// Check the position's timestamp
	if latestSchemaUnixTs > pos.UnixTs {
		return SchemaAdapter{}, fmt.Errorf("latest schema timestamp %d is greater than the current position's timestamp %d", latestSchemaUnixTs, pos.UnixTs)
	}

	// Are there any tables that are in the included list, but not in the schema adapter yet?
	for _, tbl := range cfg.Tables {
		if _, ok := schemaAdapter.adapters[tbl.Name]; !ok {
			slog.Info("Table not found in schema adapter, retrieving the table ddl and adding it to the adapter", slog.String("tableName", tbl.Name))
			ddl, err := schema.GetCreateTableDDL(db, tbl.Name)
			if err != nil {
				return SchemaAdapter{}, fmt.Errorf("failed to get create table DDL: %w", err)
			}

			if err = schemaAdapter.ApplyDDL(ddl, time.Now().Unix()); err != nil {
				return SchemaAdapter{}, fmt.Errorf("failed to apply DDL: %w", err)
			}
		}
	}

	return schemaAdapter, nil
}

const offsetKey = "offset"

func BuildStreamingIterator(db *sql.DB, cfg config.MySQL) (Iterator, error) {
	var pos Position
	offsets := persistedmap.NewPersistedMap[Position](cfg.StreamingSettings.OffsetFile)
	if _pos, isOk := offsets.Get(offsetKey); isOk {
		slog.Info("Found offsets", slog.String("offset", _pos.String()))
		pos = _pos
	}

	schemaHistoryList, err := persistedlist.NewPersistedList[SchemaHistory](cfg.StreamingSettings.SchemaHistoryFile)
	if err != nil {
		return Iterator{}, fmt.Errorf("failed to create persisted list: %w", err)
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

	schemaAdapter, err := buildSchemaAdapter(db, cfg, schemaHistoryList, pos)
	if err != nil {
		return Iterator{}, fmt.Errorf("failed to build schema adapter: %w", err)
	}

	streamer, err := syncer.StartSync(pos.ToMySQLPosition())
	if err != nil {
		return Iterator{}, fmt.Errorf("failed to start sync: %w", err)
	}

	return Iterator{
		batchSize:         cfg.GetStreamingBatchSize(),
		cfg:               cfg,
		position:          pos,
		syncer:            syncer,
		streamer:          streamer,
		offsets:           offsets,
		schemaHistoryList: &schemaHistoryList,
		schemaAdapter:     &schemaAdapter,
	}, nil
}

func (i *Iterator) HasNext() bool {
	return true
}

func (i *Iterator) CommitOffset() {
	slog.Info("Committing offset",
		slog.String("position", i.position.String()),
		slog.Int64("unixTs", i.position.UnixTs),
	)
	i.offsets.Set(offsetKey, i.position)
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
			if err = i.position.UpdatePosition(ts, event); err != nil {
				return nil, fmt.Errorf("failed to update position: %w", err)
			}

			switch event.Header.EventType {
			case replication.ROTATE_EVENT:
				// We are handling the [ROTATE_EVENT] in [UpdatePosition] method.
				continue
			case replication.QUERY_EVENT:
				query, err := typing.AssertType[*replication.QueryEvent](event.Event)
				if err != nil {
					return nil, fmt.Errorf("failed to assert a query event: %w", err)
				}

				if err = i.persistAndProcessDDL(query, ts); err != nil {
					return nil, fmt.Errorf("failed to persist DDL: %w", err)
				}
			case replication.WRITE_ROWS_EVENTv2, replication.UPDATE_ROWS_EVENTv2, replication.DELETE_ROWS_EVENTv2:
				rows, err := i.processDML(ts, event)
				if err != nil {
					return nil, fmt.Errorf("failed to process DML: %w", err)
				}

				rawMsgs = append(rawMsgs, rows...)
			default:
				slog.Info("Skipping event", slog.Any("eventType", event.Header.EventType))
			}
		}
	}

	if len(rawMsgs) == 0 {
		// If there are no messages, let's sleep a bit before we try again
		time.Sleep(2 * time.Second)
	}

	return rawMsgs, nil
}

func (i *Iterator) getTableAdapter(tableName string) (TableAdapter, bool) {
	tblAdapter, ok := i.schemaAdapter.adapters[tableName]
	if !ok {
		return TableAdapter{}, ok
	}

	idx := slices.IndexFunc(i.cfg.Tables, func(tbl *config.MySQLTable) bool { return tbl.Name == tableName })
	if idx == -1 {
		return TableAdapter{}, false
	}

	tblAdapter.tableCfg = *i.cfg.Tables[idx]
	tblAdapter.dbName = i.cfg.Database
	return tblAdapter, ok
}
