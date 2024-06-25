package adapter

import (
	"database/sql"
	"fmt"

	"github.com/artie-labs/reader/config"
	"github.com/artie-labs/reader/lib/debezium/converters"
	"github.com/artie-labs/reader/lib/debezium/transformer"
	"github.com/artie-labs/reader/lib/mssql"
	"github.com/artie-labs/reader/lib/mssql/schema"
	ptr2 "github.com/artie-labs/reader/lib/ptr"
	"github.com/artie-labs/reader/lib/rdbms/column"
	"github.com/artie-labs/reader/lib/rdbms/scan"
)

const defaultErrorRetries = 10

type MSSQLAdapter struct {
	db              *sql.DB
	dbName          string
	table           mssql.Table
	columns         []schema.Column
	fieldConverters []transformer.FieldConverter
	scannerCfg      scan.ScannerConfig
}

func NewMSSQLAdapter(db *sql.DB, dbName string, tableCfg config.MSSQLTable) (MSSQLAdapter, error) {
	table, err := mssql.LoadTable(db, tableCfg.Schema, tableCfg.Name)
	if err != nil {
		return MSSQLAdapter{}, fmt.Errorf("failed to load metadata for table %s.%s: %w", tableCfg.Schema, tableCfg.Name, err)
	}

	columns, err := column.FilterOutExcludedColumns(table.Columns(), tableCfg.ExcludeColumns, table.PrimaryKeys())
	if err != nil {
		return MSSQLAdapter{}, err
	}

	fieldConverters := make([]transformer.FieldConverter, len(columns))
	for i, col := range columns {
		converter, err := valueConverterForType(col.Type, col.Opts)
		if err != nil {
			return MSSQLAdapter{}, fmt.Errorf("failed to build value converter for column %q: %w", col.Name, err)
		}
		fieldConverters[i] = transformer.FieldConverter{Name: col.Name, ValueConverter: converter}
	}

	return MSSQLAdapter{
		db:              db,
		dbName:          dbName,
		table:           *table,
		columns:         columns,
		fieldConverters: fieldConverters,
		scannerCfg:      tableCfg.ToScannerConfig(defaultErrorRetries),
	}, nil
}

func (m MSSQLAdapter) TableName() string {
	return m.table.Name
}

func (m MSSQLAdapter) TopicSuffix() string {
	return fmt.Sprintf("%s.%s.%s", m.dbName, m.table.Schema, m.table.Name)
}

func (m MSSQLAdapter) FieldConverters() []transformer.FieldConverter {
	return m.fieldConverters
}

func (m MSSQLAdapter) NewIterator() (transformer.RowsIterator, error) {
	return mssql.NewScanner(m.db, m.table, m.columns, m.scannerCfg)
}

func (m MSSQLAdapter) PartitionKeys() []string {
	return m.table.PrimaryKeys()
}

func valueConverterForType(dataType schema.DataType, opts *schema.Opts) (converters.ValueConverter, error) {
	switch dataType {
	case schema.Bit:
		return converters.BooleanPassthrough{}, nil
	case schema.Bytes:
		return converters.BytesPassthrough{}, nil
	case schema.Int16:
		return converters.Int16Passthrough{}, nil
	case schema.Int32:
		return converters.Int32Passthrough{}, nil
	case schema.Int64:
		return converters.Int64Passthrough{}, nil
	case schema.Float:
		return converters.DoublePassthrough{}, nil
	case schema.Numeric:
		return converters.NewDecimalConverter(opts.Scale, &opts.Precision), nil
	case schema.Money:
		return converters.MoneyConverter{
			// MSSQL uses scale of 4 for money
			ScaleOverride: ptr2.ToUint16(4),
		}, nil
	case schema.String, schema.UniqueIdentifier:
		return converters.StringPassthrough{}, nil
	case schema.Time:
		return converters.TimeConverter{}, nil
	case schema.TimeMicro:
		return converters.MicroTimeConverter{}, nil
	case schema.TimeNano:
		return converters.NanoTimeConverter{}, nil
	case schema.Date:
		return converters.DateConverter{}, nil
	case schema.Datetime2:
		return converters.TimestampConverter{}, nil
	case schema.Datetime2Micro:
		return converters.MicroTimestampConverter{}, nil
	case schema.Datetime2Nano:
		return converters.NanoTimestampConverter{}, nil
	case schema.DatetimeOffset:
		return converters.ZonedTimestampConverter{}, nil
	default:
		return nil, fmt.Errorf("unsupported data type: DataType(%d)", dataType)
	}
}
