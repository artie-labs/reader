package adapter

import (
	"database/sql"
	"fmt"
	"log/slog"
	"strings"

	transferDbz "github.com/artie-labs/transfer/lib/debezium"

	"github.com/artie-labs/reader/config"
	"github.com/artie-labs/reader/lib/debezium/converters"
	"github.com/artie-labs/reader/lib/debezium/transformer"
	"github.com/artie-labs/reader/lib/postgres"
	"github.com/artie-labs/reader/lib/postgres/schema"
	"github.com/artie-labs/reader/lib/rdbms/scan"
)

const defaultErrorRetries = 10

type postgresAdapter struct {
	db              *sql.DB
	table           postgres.Table
	fieldConverters []transformer.FieldConverter
	scannerCfg      scan.ScannerConfig
}

func NewPostgresAdapter(db *sql.DB, tableCfg config.PostgreSQLTable) (postgresAdapter, error) {
	slog.Info("Loading metadata for table")
	table, err := postgres.LoadTable(db, tableCfg.Schema, tableCfg.Name)
	if err != nil {
		return postgresAdapter{}, fmt.Errorf("failed to load metadata for table %s.%s: %w", tableCfg.Schema, tableCfg.Name, err)
	}

	fieldConverters := make([]transformer.FieldConverter, len(table.Columns))
	for i, col := range table.Columns {
		converter, err := valueConverterForType(col.Type, col.Opts)
		if err != nil {
			return postgresAdapter{}, fmt.Errorf("failed to build value converter for column %s: %w", col.Name, err)
		}
		fieldConverters[i] = transformer.FieldConverter{Name: col.Name, ValueConverter: converter}
	}

	return postgresAdapter{
		db:              db,
		table:           *table,
		fieldConverters: fieldConverters,
		scannerCfg:      tableCfg.ToScannerConfig(defaultErrorRetries),
	}, nil
}

func (p postgresAdapter) TableName() string {
	return p.table.Name
}

func (p postgresAdapter) TopicSuffix() string {
	return fmt.Sprintf("%s.%s", p.table.Schema, strings.ReplaceAll(p.table.Name, `"`, ``))
}

func (p postgresAdapter) FieldConverters() []transformer.FieldConverter {
	return p.fieldConverters
}

func (p postgresAdapter) NewIterator() (transformer.RowsIterator, error) {
	return postgres.NewScanner(p.db, p.table, p.scannerCfg)
}

// PartitionKey returns a map of primary keys and their values for a given row.
func (p postgresAdapter) PartitionKey(row map[string]any) map[string]any {
	result := make(map[string]any)
	for _, key := range p.table.PrimaryKeys {
		result[key] = row[key]
	}
	return result
}

func valueConverterForType(dataType schema.DataType, opts *schema.Opts) (converters.ValueConverter, error) {
	// TODO: Replace uses of `NewPassthroughConverter` with type specific converters
	switch dataType {
	case schema.VariableNumeric:
		return converters.VariableNumericConverter{}, nil
	case schema.Numeric:
		return converters.NewDecimalConverter(opts.Scale, &opts.Precision), nil
	case schema.Money:
		return MoneyConverter{}, nil
	case schema.Boolean, schema.Bit:
		return converters.BooleanPassthrough{}, nil
	case schema.Bytea:
		return converters.BytesPassthrough{}, nil
	case schema.Text, schema.UserDefinedText, schema.Inet:
		return converters.StringPassthrough{}, nil
	case schema.Int16:
		return converters.Int16Passthrough{}, nil
	case schema.Int32:
		return converters.Int32Passthrough{}, nil
	case schema.Int64:
		return converters.Int64Passthrough{}, nil
	case schema.Date:
		return converters.DateConverter{}, nil
	case schema.Timestamp:
		return PgTimestampConverter{}, nil
	case schema.UUID:
		return converters.UUIDConverter{}, nil
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
	case schema.Interval:
		return NewPassthroughConverter("int64", "io.debezium.time.MicroDuration"), nil
	case schema.Array:
		return NewPassthroughConverter("array", ""), nil
	case schema.Float:
		return NewPassthroughConverter("float", ""), nil
	case schema.Time:
		return NewPassthroughConverter("int32", string(transferDbz.Time)), nil
	default:
		return nil, fmt.Errorf("unsupported data type: DataType(%d)", dataType)
	}
}
