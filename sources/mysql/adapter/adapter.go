package adapter

import (
	"database/sql"
	"fmt"
	"github.com/artie-labs/reader/lib/rdbms/column"
	"log/slog"

	"github.com/artie-labs/reader/config"
	"github.com/artie-labs/reader/lib/debezium/converters"
	"github.com/artie-labs/reader/lib/debezium/transformer"
	"github.com/artie-labs/reader/lib/mysql"
	"github.com/artie-labs/reader/lib/mysql/scanner"
	"github.com/artie-labs/reader/lib/mysql/schema"
	"github.com/artie-labs/reader/lib/rdbms/scan"
)

const defaultErrorRetries = 10

type Table struct {
	table           mysql.Table     `yaml:"table"`
	columns         []schema.Column `yaml:"columns"`
	fieldConverters []transformer.FieldConverter
}

type MySQLAdapter struct {
	db           *sql.DB
	dbName       string
	scannerCfg   scan.ScannerConfig
	tableAdapter Table
}

func NewMySQLAdapter(db *sql.DB, dbName string, tableCfg config.MySQLTable) (MySQLAdapter, error) {
	slog.Info("Loading metadata for table", slog.String("name", tableCfg.Name))
	table, err := mysql.LoadTable(db, tableCfg.Name)
	if err != nil {
		return MySQLAdapter{}, fmt.Errorf("failed to load metadata for table %q: %w", tableCfg.Name, err)
	}

	tableAdapter, err := BuildTableAdapter(tableCfg, *table)
	if err != nil {
		return MySQLAdapter{}, fmt.Errorf("failed to build table: %w", err)
	}

	return newMySQLAdapter(db, dbName, tableAdapter, tableCfg.ToScannerConfig(defaultErrorRetries))
}

func BuildTableAdapter(tableCfg config.MySQLTable, table mysql.Table) (Table, error) {
	// Exclude columns (if any) from the table metadata
	columns, err := column.FilterOutExcludedColumns(table.Columns, tableCfg.ExcludeColumns, table.PrimaryKeys)
	if err != nil {
		return Table{}, err
	}

	// Include columns (if any) from the table metadata
	columns, err = column.FilterForIncludedColumns(columns, tableCfg.IncludeColumns, table.PrimaryKeys)
	if err != nil {
		return Table{}, err
	}

	fieldConverters := make([]transformer.FieldConverter, len(columns))
	for i, col := range columns {
		converter, err := valueConverterForType(col.Type, col.Opts)
		if err != nil {
			return Table{}, fmt.Errorf("failed to build value converter for column %q: %w", col.Name, err)
		}
		fieldConverters[i] = transformer.FieldConverter{Name: col.Name, ValueConverter: converter}
	}

	return Table{
		table:           table,
		columns:         columns,
		fieldConverters: fieldConverters,
	}, nil
}

func newMySQLAdapter(db *sql.DB, dbName string, table Table, scannerCfg scan.ScannerConfig) (MySQLAdapter, error) {
	return MySQLAdapter{
		db:           db,
		dbName:       dbName,
		scannerCfg:   scannerCfg,
		tableAdapter: table,
	}, nil
}

func (m MySQLAdapter) TableName() string {
	return m.tableAdapter.table.Name
}

func (m MySQLAdapter) TopicSuffix() string {
	return fmt.Sprintf("%s.%s", m.dbName, m.tableAdapter.table.Name)
}

func (m MySQLAdapter) FieldConverters() []transformer.FieldConverter {
	return m.tableAdapter.fieldConverters
}

func (m MySQLAdapter) NewIterator() (transformer.RowsIterator, error) {
	return scanner.NewScanner(m.db, m.tableAdapter.table, m.tableAdapter.columns, m.scannerCfg)
}

func (m MySQLAdapter) PartitionKeys() []string {
	return m.tableAdapter.table.PrimaryKeys
}

func valueConverterForType(d schema.DataType, opts *schema.Opts) (converters.ValueConverter, error) {
	switch d {
	case schema.Bit:
		if opts == nil || opts.Size == nil {
			return nil, fmt.Errorf("size is required for bit type")
		}

		if *opts.Size == 1 {
			return converters.BooleanPassthrough{}, nil
		}

		return converters.BytesPassthrough{}, nil
	case schema.Boolean:
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
	case schema.DateTime:
		return converters.MicroTimestampConverter{}, nil
	case schema.Timestamp:
		return converters.ZonedTimestampConverter{}, nil
	case schema.Year:
		return converters.YearConverter{}, nil
	case schema.Enum:
		return converters.EnumConverter{}, nil
	case schema.Set:
		return converters.EnumSetConverter{}, nil
	case schema.JSON:
		return converters.JSONConverter{}, nil
	case schema.Point:
		return converters.NewPointConverter(), nil
	case schema.Geometry:
		return converters.NewGeometryConverter(), nil
	}
	return nil, fmt.Errorf("unable get value converter for DataType(%d)", d)
}
