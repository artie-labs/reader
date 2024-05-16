package adapter

import (
	"database/sql"
	"fmt"
	"log/slog"
	"strings"

	"github.com/artie-labs/reader/config"
	"github.com/artie-labs/reader/lib/debezium/converters"
	"github.com/artie-labs/reader/lib/debezium/transformer"
	"github.com/artie-labs/reader/lib/mssql"
	"github.com/artie-labs/reader/lib/mssql/schema"
	"github.com/artie-labs/reader/lib/rdbms/column"
	"github.com/artie-labs/reader/lib/rdbms/scan"
)

const defaultErrorRetries = 10

type MSSQLAdapter struct {
	db              *sql.DB
	table           mssql.Table
	columns         []schema.Column
	fieldConverters []transformer.FieldConverter
	scannerCfg      scan.ScannerConfig
}

func NewMSSQLAdapter(db *sql.DB, tableCfg config.MSSQLTable) (MSSQLAdapter, error) {
	slog.Info("Loading metadata for table")
	table, err := mssql.LoadTable(db, tableCfg.Schema, tableCfg.Name)
	if err != nil {
		return MSSQLAdapter{}, fmt.Errorf("failed to load metadata for table %s.%s: %w", tableCfg.Schema, tableCfg.Name, err)
	}

	columns, err := column.FilterOutExcludedColumns(table.Columns, tableCfg.ExcludeColumns, table.PrimaryKeys)
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
	return fmt.Sprintf("%s.%s", m.table.Schema, strings.ReplaceAll(m.table.Name, `"`, ``))
}

func (m MSSQLAdapter) FieldConverters() []transformer.FieldConverter {
	return m.fieldConverters
}

func (m MSSQLAdapter) NewIterator() (transformer.RowsIterator, error) {
	return mssql.NewScanner(m.db, m.table, m.columns, m.scannerCfg)
}

func (m MSSQLAdapter) PartitionKeys() []string {
	return m.table.PrimaryKeys
}

func valueConverterForType(dataType schema.DataType, opts *schema.Opts) (converters.ValueConverter, error) {
	switch dataType {
	case schema.Bit:
		return converters.BitConverter{}, nil
	case schema.Boolean:
		return converters.BooleanPassthrough{}, nil
	case schema.Int16:
		return converters.Int16Passthrough{}, nil
	case schema.Int32:
		return converters.Int32Passthrough{}, nil
	case schema.Int64:
		return converters.Int64Passthrough{}, nil
	case schema.Real:
		return converters.FloatPassthrough{}, nil
	case schema.Double:
		return converters.DoublePassthrough{}, nil
	case schema.Numeric:
		return converters.NewDecimalConverter(opts.Scale, &opts.Precision), nil
	case schema.VariableNumeric:
		return converters.VariableNumericConverter{}, nil
	case schema.Money:
		return MoneyConverter{}, nil
	case schema.Bytea:
		return converters.BytesPassthrough{}, nil
	case schema.Text, schema.UserDefinedText:
		return converters.StringPassthrough{}, nil
	case schema.Time, schema.TimeWithTimeZone:
		return PgTimeConverter{}, nil
	case schema.Date:
		return converters.DateConverter{}, nil
	case schema.Timestamp:
		return converters.MicroTimestampConverter{}, nil
	case schema.TimestampWithTimeZone:
		return converters.ZonedTimestampConverter{}, nil
	case schema.Interval:
		return PgIntervalConverter{}, nil
	case schema.UUID:
		return converters.UUIDConverter{}, nil
	case schema.Array:
		return converters.ArrayConverter{}, nil
	case schema.JSON:
		return converters.JSONConverter{}, nil
	case schema.HStore:
		return converters.MapConverter{}, nil
	case schema.Point:
		return converters.NewPointConverter(), nil
	case schema.Geometry:
		return converters.NewGeometryConverter(), nil
	case schema.Geography:
		return converters.NewGeographyConverter(), nil
	default:
		return nil, fmt.Errorf("unsupported data type: DataType(%d)", dataType)
	}
}
