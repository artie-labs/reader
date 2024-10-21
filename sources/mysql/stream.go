package mysql

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/artie-labs/reader/config"
	"github.com/artie-labs/reader/lib"
	"github.com/artie-labs/reader/writers"
	"github.com/artie-labs/transfer/lib/cdc/util"
	"github.com/artie-labs/transfer/lib/debezium"
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

		switch event.Header.EventType {
		case
			replication.WRITE_ROWS_EVENTv2,
			replication.UPDATE_ROWS_EVENTv2,
			replication.DELETE_ROWS_EVENTv2:

			messages, err := convertEventToMessages(event)
			if err != nil {
				slog.Warn("failed to convert event to messages", slog.Any("err", err))
			} else {
				for i, message := range messages {
					slog.Info("messages", slog.Int("index", i), slog.Any("event", message.Event()))
				}
			}
		default:
			slog.Info("skipping event", "type", event.Header.EventType)
		}
	}
}

func convertEventToMessages(event *replication.BinlogEvent) ([]lib.RawMessage, error) {
	var operation string
	switch event.Header.EventType {
	case replication.WRITE_ROWS_EVENTv2:
		operation = "c"
	case replication.UPDATE_ROWS_EVENTv2:
		operation = "u"
	case replication.DELETE_ROWS_EVENTv2:
		operation = "d"
	default:
		return nil, fmt.Errorf("unsupported MySQL event type: %s", event.Header.EventType.String())
	}

	rowsEvent, ok := event.Event.(*replication.RowsEvent)
	if !ok {
		return nil, fmt.Errorf("unable to cast event to replication.RowsEvent")
	}

	// Column names are only available if `binlog_row_metadata` is set to `FULL`.
	// They also only work on versions >= MySQL 8.0.1
	// See: https://dev.mysql.com/doc/refman/8.4/en/replication-options-binary-log.html#sysvar_binlog_row_metadata
	columnNames := make([]string, len(rowsEvent.Table.ColumnName))
	for i, name := range rowsEvent.Table.ColumnName {
		columnNames[i] = string(name)
	}

	out := make([]lib.RawMessage, len(rowsEvent.Rows))
	for i, row := range rowsEvent.Rows {
		convertedRow, err := zipSlicesToMap(columnNames, row)
		if err != nil {
			return nil, fmt.Errorf("failed to convert row to map:%w", err)
		}

		out[i] = lib.NewRawMessage("", nil, &util.SchemaEventPayload{
			Schema: debezium.Schema{},
			Payload: util.Payload{
				After: convertedRow,
				Source: util.Source{
					TsMs: time.Unix(int64(event.Header.Timestamp), 0).UnixMilli(),
				},
				Operation: operation,
			},
		})
	}
	return out, nil
}

func zipSlicesToMap(keys []string, values []any) (map[string]any, error) {
	if len(values) != len(keys) {
		return nil, fmt.Errorf("keys length (%d) is different from values length (%d)", len(keys), len(values))
	}

	out := map[string]any{}
	for i, value := range values {
		out[keys[i]] = value
	}
	return out, nil
}
