package streaming

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/artie-labs/reader/lib/kafkalib"
	"log/slog"
	"strings"
	"time"

	"github.com/artie-labs/transfer/lib/typing"
	"github.com/go-mysql-org/go-mysql/replication"

	"github.com/artie-labs/reader/config"
	"github.com/artie-labs/reader/lib/mysql"
	"github.com/artie-labs/reader/lib/mysql/schema"
	"github.com/artie-labs/reader/lib/storage/persistedlist"
	"github.com/artie-labs/reader/lib/storage/persistedmap"
	"github.com/artie-labs/reader/sources/mysql/streaming/ddl"
)

const offsetKey = "offset"

func buildSchemaAdapter(db *sql.DB, cfg config.MySQL, schemaHistoryList persistedlist.PersistedList[SchemaHistory], pos Position, sqlMode []string) (ddl.SchemaAdapter, error) {
	var latestSchemaUnixTs int64
	schemaAdapter := ddl.NewSchemaAdapter(cfg, sqlMode)
	for _, schemaHistory := range schemaHistoryList.GetData() {
		if err := schemaAdapter.ApplyDDL(schemaHistory.UnixTs, schemaHistory.Query); err != nil {
			return ddl.SchemaAdapter{}, fmt.Errorf("failed to apply DDL: %w", err)
		}

		latestSchemaUnixTs = schemaHistory.UnixTs
	}

	// If [pos.UnixTs] is set, it should be greater than the latest schema timestamp
	if latestSchemaUnixTs > pos.UnixTs && pos.UnixTs > 0 {
		return ddl.SchemaAdapter{}, fmt.Errorf("latest schema timestamp %d is greater than the current position's timestamp %d", latestSchemaUnixTs, pos.UnixTs)
	}

	// Find all the tables in the schema, check if they are already in the schema adapter
	// If not, then call [GetCreateTableDDL] to get the DDL and apply it to the schema adapter
	tables, err := schema.ListTables(db, cfg.Database)
	if err != nil {
		return ddl.SchemaAdapter{}, fmt.Errorf("failed to list tables: %w", err)
	}

	for _, tbl := range tables {
		if _, ok := schemaAdapter.GetTableAdapter(tbl); !ok {
			now := time.Now().Unix()
			ddlQuery, err := schema.GetCreateTableDDL(db, tbl)
			if err != nil {
				return ddl.SchemaAdapter{}, fmt.Errorf("failed to get columns: %w", err)
			}

			// Persist the DDL
			if err = schemaHistoryList.Push(SchemaHistory{Query: ddlQuery, UnixTs: now}); err != nil {
				return ddl.SchemaAdapter{}, fmt.Errorf("failed to push schema history: %w", err)
			}

			// Apply the DDL
			if err = schemaAdapter.ApplyDDL(now, ddlQuery); err != nil {
				return ddl.SchemaAdapter{}, fmt.Errorf("failed to apply DDL: %w", err)
			}
		}
	}

	return schemaAdapter, nil
}

func BuildStreamingIterator(db *sql.DB, cfg config.MySQL, sqlMode []string, gtidEnabled bool) (Iterator, error) {
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

	schemaAdapter, err := buildSchemaAdapter(db, cfg, schemaHistoryList, pos, sqlMode)
	if err != nil {
		return Iterator{}, fmt.Errorf("failed to build schema adapter: %w", err)
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

	var streamer *replication.BinlogStreamer
	if gtidEnabled {
		gtidSet, err := pos.ToGTIDSet()
		if err != nil {
			return Iterator{}, fmt.Errorf("failed to parse GTID: %w", err)
		}

		streamer, err = syncer.StartSyncGTID(gtidSet)
		if err != nil {
			return Iterator{}, fmt.Errorf("failed to start sync: %w", err)
		}
	} else {
		streamer, err = syncer.StartSync(pos.ToMySQLPosition())
		if err != nil {
			return Iterator{}, fmt.Errorf("failed to start sync: %w", err)
		}
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

func (i *Iterator) CommitOffset() error {
	slog.Info("Committing offset",
		slog.String("position", i.position.String()),
		slog.Int64("unixTs", i.position.UnixTs),
	)

	return i.offsets.Set(offsetKey, i.position)
}

func (i *Iterator) Close() error {
	i.syncer.Close()
	return nil
}

func (i *Iterator) Next() ([]kafkalib.Message, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var rawMsgs []kafkalib.Message
	var currentGTID *string
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

			if gtidEvent, ok := event.Event.(*replication.GTIDEvent); ok {
				next, err := gtidEvent.GTIDNext()
				if err != nil {
					return nil, fmt.Errorf("failed to retrieve next GTID set: %w", err)
				}

				currentGTID = typing.ToPtr(next.String())
				shouldProcess, err := mysql.ShouldProcessRow(i.position._gtidSet, *currentGTID)
				if err != nil {
					return nil, fmt.Errorf("failed to check if we should process row: %w", err)
				}

				if !shouldProcess {
					// We should skip this row since we have already processed it
					continue
				}
			}

			ts := getTimeFromEvent(event)
			if err = i.position.UpdatePosition(ts, event); err != nil {
				return nil, fmt.Errorf("failed to update position: %w", err)
			}

			switch event.Header.EventType {
			case
				// We don't need these events, [GTID_EVENT] will contain the offsets via GTID sets, which is handled in [UpdatePosition]
				replication.GTID_EVENT,
				replication.PREVIOUS_GTIDS_EVENT,
				replication.FORMAT_DESCRIPTION_EVENT,
				replication.ANONYMOUS_GTID_EVENT,
				replication.TABLE_MAP_EVENT,
				// We don't need TableMapEvent because we are handling it by consuming DDL queries, applying it to our schema adapter
				// RotateEvent is handled by [UpdatePosition]
				replication.ROTATE_EVENT,
				replication.XID_EVENT:
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
				rows, err := i.processDML(ts, event, currentGTID)
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

func (i *Iterator) persistAndProcessDDL(evt *replication.QueryEvent, ts time.Time) error {
	if evt.ErrorCode != 0 {
		// Don't process a non-zero error code DDL.
		return nil
	}

	if !strings.EqualFold(i.cfg.Database, string(evt.Schema)) {
		slog.Debug("Skipping this event since the database does not match the configured database",
			slog.String("config_db", i.cfg.Database),
			slog.String("event_db", string(evt.Schema)),
		)

		return nil
	}

	query := string(evt.Query)
	if shouldSkipDDL(query) {
		return nil
	}

	schemaHistory := SchemaHistory{
		Query:  query,
		UnixTs: ts.Unix(),
	}

	if err := i.schemaHistoryList.Push(schemaHistory); err != nil {
		return fmt.Errorf("failed to push schema history: %w", err)
	}

	return i.schemaAdapter.ApplyDDL(ts.Unix(), query)
}
