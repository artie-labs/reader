package adapter

import (
	"database/sql"
	"fmt"
	"log/slog"
	"strings"

	transferDbz "github.com/artie-labs/transfer/lib/debezium"

	"github.com/artie-labs/reader/config"
	"github.com/artie-labs/reader/lib/debezium"
	"github.com/artie-labs/reader/lib/debezium/converters"
	"github.com/artie-labs/reader/lib/mysql"
	"github.com/artie-labs/reader/lib/mysql/scanner"
	"github.com/artie-labs/reader/lib/mysql/schema"
	"github.com/artie-labs/reader/lib/rdbms/scan"
)

const defaultErrorRetries = 10

type mysqlAdapter struct {
	db           *sql.DB
	table        mysql.Table
	fields       []transferDbz.Field
	scannerCfg   scan.ScannerConfig
	rowConverter converters.RowConverter
}

func NewMySQLAdapter(db *sql.DB, tableCfg config.MySQLTable) (mysqlAdapter, error) {
	slog.Info("Loading metadata for table")
	table, err := mysql.LoadTable(db, tableCfg.Name)
	if err != nil {
		return mysqlAdapter{}, fmt.Errorf("failed to load metadata for table %s: %w", tableCfg.Name, err)
	}

	return newMySQLAdapter(db, *table, tableCfg.ToScannerConfig(defaultErrorRetries))
}

func newMySQLAdapter(db *sql.DB, table mysql.Table, scannerCfg scan.ScannerConfig) (mysqlAdapter, error) {
	fields := make([]transferDbz.Field, len(table.Columns))
	valueConverters := map[string]converters.ValueConverter{}
	for i, col := range table.Columns {
		converter, err := valueConverterForType(col.Type, col.Opts)
		if err != nil {
			return mysqlAdapter{}, fmt.Errorf("failed to build field for column %s: %w", col.Name, err)
		}
		fields[i] = converter.ToField(col.Name)
		valueConverters[col.Name] = converter
	}

	return mysqlAdapter{
		db:           db,
		table:        table,
		fields:       fields,
		scannerCfg:   scannerCfg,
		rowConverter: converters.NewRowConverter(valueConverters),
	}, nil
}

func (m mysqlAdapter) TableName() string {
	return m.table.Name
}

func (m mysqlAdapter) TopicSuffix() string {
	return strings.ReplaceAll(m.table.Name, `"`, ``)
}

func (m mysqlAdapter) Fields() []transferDbz.Field {
	return m.fields
}

func (m mysqlAdapter) NewIterator() (debezium.RowsIterator, error) {
	return scanner.NewScanner(m.db, m.table, m.scannerCfg)
}

// PartitionKey returns a map of primary keys and their values for a given row.
func (m mysqlAdapter) PartitionKey(row map[string]any) map[string]any {
	result := make(map[string]any)
	for _, key := range m.table.PrimaryKeys {
		result[key] = row[key]
	}
	return result
}

func (m mysqlAdapter) ConvertRowToDebezium(row map[string]any) (map[string]any, error) {
	return m.rowConverter.Convert(row)
}

func valueConverterForType(d schema.DataType, opts *schema.Opts) (converters.ValueConverter, error) {
	switch d {
	case schema.Bit, schema.Boolean:
		return converters.BooleanPassthrough{}, nil
	case schema.TinyInt, schema.SmallInt:
		return converters.Int16Passthrough{}, nil
	case schema.MediumInt, schema.Int:
		return converters.Int32Passthrough{}, nil
	case schema.BigInt:
		return converters.Int64Passthrough{}, nil
	case schema.Float:
		return converters.FloatPassthrough{}, nil
	case schema.Double:
		return converters.DoublePassthrough{}, nil
	case schema.Decimal:
		if opts.Scale == nil {
			return nil, fmt.Errorf("scale is required for decimal type")
		}

		return converters.NewDecimalConverter(*opts.Scale, opts.Precision), nil
	case schema.Char, schema.Text, schema.Varchar, schema.TinyText, schema.MediumText, schema.LongText:
		return converters.StringPassthrough{}, nil
	case schema.Binary, schema.Varbinary, schema.Blob:
		return converters.BytesPassthrough{}, nil
	case schema.Time:
		return converters.MicroTimeConverter{}, nil
	case schema.Date:
		return converters.DateConverter{}, nil
	case schema.DateTime, schema.Timestamp:
		return converters.TimestampConverter{}, nil
	case schema.Year:
		return converters.YearConverter{}, nil
	case schema.Enum:
		return converters.EnumConverter{}, nil
	case schema.Set:
		return converters.EnumSetConverter{}, nil
	case schema.JSON:
		return converters.JSONConverter{}, nil
	}
	return nil, fmt.Errorf("unable get value converter for DataType(%d)", d)
}
