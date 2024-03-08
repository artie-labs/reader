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
	"github.com/artie-labs/reader/lib/postgres"
	"github.com/artie-labs/reader/lib/postgres/schema"
	"github.com/artie-labs/reader/lib/rdbms/scan"
)

const defaultErrorRetries = 10

type postgresAdapter struct {
	db         *sql.DB
	table      postgres.Table
	fields     []transferDbz.Field
	scannerCfg scan.ScannerConfig
}

func NewPostgresAdapter(db *sql.DB, tableCfg config.PostgreSQLTable) (postgresAdapter, error) {
	slog.Info("Loading metadata for table")
	table, err := postgres.LoadTable(db, tableCfg.Schema, tableCfg.Name)
	if err != nil {
		return postgresAdapter{}, fmt.Errorf("failed to load metadata for table %s.%s: %w", tableCfg.Schema, tableCfg.Name, err)
	}

	fields := make([]transferDbz.Field, len(table.Columns))
	for i, col := range table.Columns {
		fields[i], err = ColumnToField(col)
		if err != nil {
			return postgresAdapter{}, fmt.Errorf("failed to build field for column %s: %w", col.Name, err)
		}
	}

	return postgresAdapter{
		db:         db,
		table:      *table,
		fields:     fields,
		scannerCfg: tableCfg.ToScannerConfig(defaultErrorRetries),
	}, nil
}

func (p postgresAdapter) TableName() string {
	return p.table.Name
}

func (p postgresAdapter) TopicSuffix() string {
	return fmt.Sprintf("%s.%s", p.table.Schema, strings.ReplaceAll(p.table.Name, `"`, ``))
}

func (p postgresAdapter) Fields() []transferDbz.Field {
	return p.fields
}

func (p postgresAdapter) NewIterator() (debezium.RowsIterator, error) {
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

func (p postgresAdapter) ConvertRowToDebezium(row map[string]any) (map[string]any, error) {
	result := make(map[string]any)
	for key, value := range row {
		col, err := p.table.GetColumnByName(key)
		if err != nil {
			return nil, fmt.Errorf("failed to get column %s by name: %w", key, err)
		}

		val, err := ConvertValueToDebezium(*col, value)
		if err != nil {
			return nil, fmt.Errorf("failed to convert value: %w", err)
		}

		result[key] = val
	}
	return result, nil
}

func valueConverterForType(dataType schema.DataType, _ *schema.Opts) converters.ValueConverter {
	// TODO: Implement all Postgres types
	switch dataType {
	case schema.VariableNumeric:
		return converters.VariableNumericConverter{}
	case schema.Bytea:
		return converters.BytesPassthrough{}
	case schema.Date:
		return converters.DateConverter{}
	case schema.Money:
		return MoneyConverter{}
	default:
		return nil
	}
}
