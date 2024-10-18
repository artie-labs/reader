package mysql

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/artie-labs/reader/config"
	"github.com/artie-labs/reader/writers"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
)

type StreamingSource struct {
	cfg    config.MySQL
	syncer *replication.BinlogSyncer
}

func LoadStreaming(cfg config.MySQL) (*StreamingSource, error) {
	syncerConfig := replication.BinlogSyncerConfig{
		ServerID: 100,
		Flavor:   "mysql",
		Host:     cfg.Host,
		Port:     uint16(cfg.Port),
		User:     cfg.Username,
		Password: cfg.Password,
	}

	return &StreamingSource{
		cfg:    cfg,
		syncer: replication.NewBinlogSyncer(syncerConfig),
	}, nil
}

func (s *StreamingSource) Close() error {
	return nil
}

func (s *StreamingSource) Run(ctx context.Context, writer writers.Writer) error {
	streamer, err := s.syncer.StartSync(mysql.Position{})
	if err != nil {
		return err
	}

	for {
		event, err := streamer.GetEvent(ctx)
		if err != nil {
			return fmt.Errorf("failed to get binlog event: %w", err)
		}

		switch typedEvent := event.Event.(type) {
		case *replication.RowsEvent:
			slog.Info("table")

			var action string
			switch event.Header.EventType {
			case replication.WRITE_ROWS_EVENTv2:
				action = "write"
			case replication.UPDATE_ROWS_EVENTv2:
				action = "update"
			case replication.DELETE_ROWS_EVENTv2:
				action = "delete"
			default:
				slog.Info("skipping rows event", "type", event.Header.EventType)
				continue
			}

			// Column names are only available if `binlog_row_metadata` is set to `FULL`.
			// They also only work on versions >= MySQL 8.0.1
			// See: https://dev.mysql.com/doc/refman/8.4/en/replication-options-binary-log.html#sysvar_binlog_row_metadata
			columnNames := make([]string, len(typedEvent.Table.ColumnName))
			for i, name := range typedEvent.Table.ColumnName {
				columnNames[i] = string(name)
			}

			slog.Info("event",
				"schema", string(typedEvent.Table.Schema),
				"name", string(typedEvent.Table.Table),
				"action", action,
				"column names", columnNames,
				"column meta", typedEvent.Table.ColumnMeta,
				"column types", typedEvent.Table.ColumnType,
			)

			for _, row := range typedEvent.Rows {
				slog.Info("rows event for", "row", row, "x", fmt.Sprintf("%T", row[0]))
			}
		default:
			slog.Info("other event", "type", event.Header.EventType)
		}
	}
}
