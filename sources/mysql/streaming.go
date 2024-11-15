package mysql

import (
	"context"
	"fmt"
	"github.com/artie-labs/reader/lib"
	"github.com/artie-labs/reader/lib/debezium/converters"
	"github.com/artie-labs/reader/lib/mysql/schema"
	"github.com/artie-labs/reader/sources/mysql/adapter"
	"github.com/artie-labs/transfer/lib/cdc/util"
	"github.com/artie-labs/transfer/lib/debezium"
	"iter"
	"log/slog"
	"slices"
	"time"

	"github.com/artie-labs/transfer/lib/typing"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"

	"github.com/artie-labs/reader/config"
	"github.com/artie-labs/reader/lib/storage/persistedmap"
	"github.com/artie-labs/reader/writers"
)

const offsetKey = "offset"

type StreamingPosition struct {
	File string `yaml:"file"`
	Pos  uint32 `yaml:"pos"`
}

func (s StreamingPosition) String() string {
	return fmt.Sprintf("File: %s, Pos: %d", s.File, s.Pos)
}

func (s StreamingPosition) buildMySQLPosition() mysql.Position {
	return mysql.Position{Name: s.File, Pos: s.Pos}
}

type Streaming struct {
	syncer             *replication.BinlogSyncer
	offsets            *persistedmap.PersistedMap
	position           StreamingPosition
	tablesToIncludeMap map[string]bool

	// TODO: Support partitioned tables
	// TODO: Support column exclusion
}

func (s Streaming) Close() error {
	s.syncer.Close()
	return nil
}

func (s Streaming) shouldProcessTable(tableName string) bool {
	_, isOk := s.tablesToIncludeMap[tableName]
	return isOk
}

func buildStreamingConfig(cfg config.MySQL) (Streaming, error) {
	tablesToIncludeMap := make(map[string]bool)
	for _, table := range cfg.Tables {
		tablesToIncludeMap[table.Name] = true
	}

	streaming := Streaming{
		syncer: replication.NewBinlogSyncer(replication.BinlogSyncerConfig{
			ServerID: cfg.StreamingSettings.ServerID,
			Flavor:   "mysql",
			Host:     cfg.Host,
			Port:     uint16(cfg.Port),
			User:     cfg.Username,
			Password: cfg.Password,
		}),
		tablesToIncludeMap: tablesToIncludeMap,
	}

	storage := persistedmap.NewPersistedMap(cfg.StreamingSettings.OffsetFile)
	value, isOk := storage.Get(offsetKey)
	if isOk {
		pos, err := typing.AssertType[StreamingPosition](value)
		if err != nil {
			return Streaming{}, err
		}

		slog.Info("Loaded offsets", slog.String("offset", pos.String()))
		streaming.position = pos
	}

	return streaming, nil
}

func (s Streaming) Run(ctx context.Context, _ writers.Writer) error {
	streamer, err := s.syncer.StartSync(s.position.buildMySQLPosition())
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

			if !s.shouldProcessTable(string(rowsEvent.Table.Table)) {
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
		default:
			slog.Info("Skipping event", slog.Any("event", event.Header.EventType))
		}
	}
}

func convertHeaderToOperation(evtType replication.EventType) (string, error) {
	switch evtType {
	case replication.WRITE_ROWS_EVENTv2:
		return "c", nil
	case replication.UPDATE_ROWS_EVENTv2:
		return "u", nil
	case replication.DELETE_ROWS_EVENTv2:
		return "d", nil
	default:
		return "", fmt.Errorf("unexpected event type: %T", evtType)
	}
}

func convertEventToMessages(header *replication.EventHeader, event *replication.RowsEvent) ([]lib.RawMessage, error) {
	op, err := convertHeaderToOperation(header.EventType)
	if err != nil {
		return nil, err
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

	beforeAndAfters, err := splitIntoBeforeAndAfter(op, rows)
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
			Operation: op,
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
