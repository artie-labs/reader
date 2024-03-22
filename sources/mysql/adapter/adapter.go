package adapter

import (
	"database/sql"
	"fmt"
	"log/slog"
	"strings"

	"github.com/artie-labs/reader/config"
	"github.com/artie-labs/reader/lib/debezium/converters"
	"github.com/artie-labs/reader/lib/debezium/transformer"
	"github.com/artie-labs/reader/lib/mysql"
	"github.com/artie-labs/reader/lib/mysql/scanner"
	"github.com/artie-labs/reader/lib/mysql/schema"
	"github.com/artie-labs/reader/lib/rdbms/column"
	"github.com/artie-labs/reader/lib/rdbms/scan"
)

const defaultErrorRetries = 10

type mysqlAdapter struct {
	db              *sql.DB
	dbName          string
	table           mysql.Table
	columns         []schema.Column
	fieldConverters []transformer.FieldConverter
	scannerCfg      scan.ScannerConfig
}

func NewMySQLAdapter(db *sql.DB, dbName string, tableCfg config.MySQLTable) (mysqlAdapter, error) {
	slog.Info("Loading metadata for table")
	table, err := mysql.LoadTable(db, tableCfg.Name)
	if err != nil {
		return mysqlAdapter{}, fmt.Errorf("failed to load metadata for table %s: %w", tableCfg.Name, err)
	}

	columns, err := column.FilterOutExcludedColumns(table.Columns, tableCfg.ExcludeColumns, table.PrimaryKeys)
	if err != nil {
		return mysqlAdapter{}, err
	}

	return newMySQLAdapter(db, dbName, *table, columns, tableCfg.ToScannerConfig(defaultErrorRetries))
}

func newMySQLAdapter(db *sql.DB, dbName string, table mysql.Table, columns []schema.Column, scannerCfg scan.ScannerConfig) (mysqlAdapter, error) {
	fieldConverters := make([]transformer.FieldConverter, len(columns))
	for i, col := range columns {
		converter, err := valueConverterForType(col.Type, col.Opts)
		if err != nil {
			return mysqlAdapter{}, fmt.Errorf("failed to build value converter for column %s: %w", col.Name, err)
		}
		fieldConverters[i] = transformer.FieldConverter{Name: col.Name, ValueConverter: converter}
	}

	return mysqlAdapter{
		db:              db,
		dbName:          dbName,
		table:           table,
		columns:         columns,
		fieldConverters: fieldConverters,
		scannerCfg:      scannerCfg,
	}, nil
}

func (m mysqlAdapter) TableName() string {
	return m.table.Name
}

func (m mysqlAdapter) TopicSuffix() string {
	return fmt.Sprintf("%s.%s", m.dbName, strings.ReplaceAll(m.table.Name, `"`, ``))
}

func (m mysqlAdapter) FieldConverters() []transformer.FieldConverter {
	return m.fieldConverters
}

func (m mysqlAdapter) NewIterator() (transformer.RowsIterator, error) {
	return scanner.NewScanner(m.db, m.table, m.columns, m.scannerCfg)
}

func (m mysqlAdapter) PartitionKeys() []string {
	return m.table.PrimaryKeys
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
