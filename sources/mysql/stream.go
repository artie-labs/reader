package mysql

import (
	"context"
	"fmt"
	"iter"
	"log/slog"
	"slices"
	"time"

	"github.com/artie-labs/reader/config"
	"github.com/artie-labs/reader/lib"
	"github.com/artie-labs/reader/lib/debezium/converters"
	"github.com/artie-labs/reader/lib/mysql/schema"
	"github.com/artie-labs/reader/sources/mysql/adapter"
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

			rowsEvent, ok := event.Event.(*replication.RowsEvent)
			if !ok {
				return fmt.Errorf("unable to cast event to replication.RowsEvent")
			}

			if string(rowsEvent.Table.Table) != "foo" {
				continue
			}

			messages, err := convertEventToMessages(event.Header, rowsEvent)
			if err != nil {
				slog.Error("Failed to convert event to messages", slog.Any("err", err))
			} else {
				for i, message := range messages {
					slog.Info("messages", slog.Int("index", i), slog.Any("event", message.Event()))
				}
			}
		}
	}
}

func convertEventToMessages(header *replication.EventHeader, event *replication.RowsEvent) ([]lib.RawMessage, error) {
	var operation string
	switch header.EventType {
	case replication.WRITE_ROWS_EVENTv2:
		operation = "c"
	case replication.UPDATE_ROWS_EVENTv2:
		operation = "u"
	case replication.DELETE_ROWS_EVENTv2:
		operation = "d"
	default:
		return nil, fmt.Errorf("unsupported MySQL event type: %s", header.EventType.String())
	}

	// Column names are only available if `binlog_row_metadata` is set to `FULL`.
	// They also only work on versions >= MySQL 8.0.1
	// See: https://dev.mysql.com/doc/refman/8.4/en/replication-options-binary-log.html#sysvar_binlog_row_metadata
	columnNames := make([]string, len(event.Table.ColumnName))
	for i, name := range event.Table.ColumnName {
		columnNames[i] = string(name)
	}

	collationMap := event.Table.CollationMap()
	dataTypes := make([]schema.DataType, len(event.Table.ColumnType))
	for i, columnType := range event.Table.ColumnType {
		var err error
		dataTypes[i], err = parseDataType(columnType, collationMap[i])
		if err != nil {
			return nil, err
		}
	}

	slog.Info("data", "types", dataTypes, "columns", event.Table.ColumnType)

	valueConverters := make([]converters.ValueConverter, len(dataTypes))
	for i := range len(valueConverters) {
		var err error
		valueConverters[i], err = adapter.ValueConverterForType(dataTypes[i], &schema.Opts{})
		if err != nil {
			return nil, err
		}
	}

	fields := make([]debezium.Field, len(columnNames))
	for i, valueConverter := range valueConverters {
		fields[i] = valueConverter.ToField(columnNames[i])
	}

	rows := slices.Clone(event.Rows)
	for _, row := range rows {
		if err := convertRow(valueConverters, dataTypes, row); err != nil {
			return nil, err
		}
	}

	beforeAndAfters, err := splitIntoBeforeAndAfter(operation, rows)
	if err != nil {
		return nil, err
	}

	out := make([]lib.RawMessage, 0)
	for before, after := range beforeAndAfters {
		payload := util.Payload{
			Source: util.Source{
				TsMs:   time.Unix(int64(header.Timestamp), 0).UnixMilli(),
				Schema: string(event.Table.Schema),
				Table:  string(event.Table.Table),
			},
			Operation: operation,
		}

		if len(before) > 0 {
			payload.Before, err = zipSlicesToMap(columnNames, before)
			if err != nil {
				return nil, fmt.Errorf("failed to convert row to map:%w", err)
			}
		}

		if len(after) > 0 {
			payload.After, err = zipSlicesToMap(columnNames, after)
			if err != nil {
				return nil, fmt.Errorf("failed to convert row to map:%w", err)
			}
		}

		// TODO: Set partition key
		out = append(out, lib.NewRawMessage("", nil, &util.SchemaEventPayload{
			Schema: debezium.Schema{
				FieldsObject: []debezium.FieldsObject{{
					Fields:     fields,
					FieldLabel: debezium.After,
				}},
			},
			Payload: payload,
		}))
	}
	return out, nil
}

func parseDataType(columnType byte, collation uint64) (schema.DataType, error) {
	switch columnType {
	case mysql.MYSQL_TYPE_DECIMAL:
		return schema.Decimal, nil
	case mysql.MYSQL_TYPE_TINY:
		return schema.TinyInt, nil
	case mysql.MYSQL_TYPE_SHORT:
		return schema.SmallInt, nil
	case mysql.MYSQL_TYPE_LONG:
		return schema.Int, nil
	case mysql.MYSQL_TYPE_FLOAT:
		return schema.Float, nil
	case mysql.MYSQL_TYPE_DOUBLE:
		return schema.Double, nil
	case mysql.MYSQL_TYPE_NULL:
		// pass
	case mysql.MYSQL_TYPE_TIMESTAMP:
		return schema.Timestamp, nil
	case mysql.MYSQL_TYPE_LONGLONG:
		return schema.BigInt, nil
	case mysql.MYSQL_TYPE_INT24:
		return schema.MediumInt, nil
	case mysql.MYSQL_TYPE_DATE:
		return schema.Date, nil
	case mysql.MYSQL_TYPE_TIME:
		return schema.Time, nil
	case mysql.MYSQL_TYPE_DATETIME:
		return schema.DateTime, nil
	case mysql.MYSQL_TYPE_YEAR:
		return schema.Year, nil
	case mysql.MYSQL_TYPE_NEWDATE:
		return schema.Date, nil
	case mysql.MYSQL_TYPE_VARCHAR:
		return schema.Varchar, nil
	case mysql.MYSQL_TYPE_BIT:
		return schema.Bit, nil
	case mysql.MYSQL_TYPE_TIMESTAMP2:
		return schema.Timestamp, nil
	case mysql.MYSQL_TYPE_DATETIME2:
		return schema.DateTime, nil
	case mysql.MYSQL_TYPE_TIME2:
		return schema.Time, nil
	case mysql.MYSQL_TYPE_JSON:
		return schema.JSON, nil
	case mysql.MYSQL_TYPE_NEWDECIMAL:
		return schema.Decimal, nil
	case mysql.MYSQL_TYPE_ENUM:
		return schema.Enum, nil
	case mysql.MYSQL_TYPE_SET:
		return schema.Set, nil
	case mysql.MYSQL_TYPE_TINY_BLOB:
		if collation == 63 {
			return schema.Blob, nil
		} else {
			return schema.TinyText, nil
		}
	case mysql.MYSQL_TYPE_MEDIUM_BLOB:
		if collation == 63 {
			return schema.Blob, nil
		} else {
			return schema.MediumText, nil
		}
	case mysql.MYSQL_TYPE_LONG_BLOB:
		if collation == 63 {
			return schema.Blob, nil
		} else {
			return schema.LongText, nil
		}
	case mysql.MYSQL_TYPE_BLOB:
		if collation == 63 {
			return schema.Blob, nil
		} else {
			return schema.Text, nil
		}
	case mysql.MYSQL_TYPE_VAR_STRING,
		mysql.MYSQL_TYPE_STRING:
		return schema.Varchar, nil
	case mysql.MYSQL_TYPE_GEOMETRY:
		return schema.Geometry, nil
	}

	return -1, fmt.Errorf("unsupported column type: %d", columnType)
}

// splitIntoBeforeAndAfter returns a sequence of [before, after] pairs for each row that has been modified.
func splitIntoBeforeAndAfter(operation string, rows [][]any) (iter.Seq2[[]any, []any], error) {
	switch operation {
	case "c":
		return func(yield func([]any, []any) bool) {
			for _, row := range rows {
				if !yield(nil, row) {
					return
				}
			}
		}, nil
	case "u":
		// For updates, every modified row is present in the event rows, first as the row before the change and second,
		// as the row after the change.
		// We're assuming that this ordering of rows is consistent.
		if len(rows)%2 != 0 {
			return nil, fmt.Errorf("update row count is not divisible by two: %d", len(rows))
		}

		return func(yield func([]any, []any) bool) {
			for group := range slices.Chunk(rows, 2) {
				if !yield(group[0], group[1]) {
					return
				}
			}
		}, nil
	case "d":
		return func(yield func([]any, []any) bool) {
			for _, row := range rows {
				if !yield(row, nil) {
					return
				}
			}
		}, nil
	default:
		return nil, fmt.Errorf("unsupported operation: %q", operation)
	}
}

func convertRow(valueConverters []converters.ValueConverter, dataTypes []schema.DataType, row []any) error {
	if len(valueConverters) != len(row) {
		return fmt.Errorf("converters length (%d) is different from row length (%d)", len(valueConverters), len(row))
	}

	for i := range len(valueConverters) {
		value, err := schema.ConvertValue(row[i], dataTypes[i], &schema.Opts{})
		if err != nil {
			return err
		} else if value == nil {
			row[i] = nil
			continue
		}

		row[i], err = valueConverters[i].Convert(value)
		if err != nil {
			return err
		}
	}

	return nil
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
