package adapter

import (
	"database/sql"
	"fmt"
	"log/slog"
	"strings"

	transferDbz "github.com/artie-labs/transfer/lib/debezium"

	"github.com/artie-labs/reader/config"
	"github.com/artie-labs/reader/lib/debezium"
	"github.com/artie-labs/reader/lib/postgres"
	"github.com/artie-labs/reader/lib/rdbms/scan"
)

const defaultErrorRetries = 10

type postgresAdapter struct {
	db         *sql.DB
	table      postgres.Table
	scannerCfg scan.ScannerConfig
}

func NewPostgresAdapter(db *sql.DB, tableCfg config.PostgreSQLTable) (postgresAdapter, error) {
	slog.Info("Loading metadata for table")
	table, err := postgres.LoadTable(db, tableCfg.Schema, tableCfg.Name)
	if err != nil {
		return postgresAdapter{}, fmt.Errorf("failed to load metadata for table %s.%s: %w", tableCfg.Schema, tableCfg.Name, err)
	}

	return postgresAdapter{
		db:         db,
		table:      *table,
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
	fields := make([]transferDbz.Field, len(p.table.Columns))
	for i, col := range p.table.Columns {
		fields[i] = ColumnToField(col)
	}
	return fields
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
