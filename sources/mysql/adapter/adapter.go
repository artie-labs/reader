package adapter

import (
	"database/sql"
	"fmt"
	"log/slog"

	"github.com/artie-labs/reader/config"
	"github.com/artie-labs/reader/lib/debezium/transformer"
	"github.com/artie-labs/reader/lib/mysql"
	"github.com/artie-labs/reader/lib/mysql/converters"
	"github.com/artie-labs/reader/lib/mysql/scanner"
	"github.com/artie-labs/reader/lib/mysql/schema"
	"github.com/artie-labs/reader/lib/rdbms/column"
	"github.com/artie-labs/reader/lib/rdbms/scan"
)

const defaultErrorRetries = 10

type MySQLAdapter struct {
	db              *sql.DB
	dbName          string
	table           mysql.Table
	columns         []schema.Column
	fieldConverters []transformer.FieldConverter
	scannerCfg      scan.ScannerConfig
}

func NewMySQLAdapter(db *sql.DB, dbName string, tableCfg config.MySQLTable) (MySQLAdapter, error) {
	slog.Info("Loading metadata for table")
	table, err := mysql.LoadTable(db, tableCfg.Name)
	if err != nil {
		return MySQLAdapter{}, fmt.Errorf("failed to load metadata for table %q: %w", tableCfg.Name, err)
	}

	// Exclude columns (if any) from the table metadata
	columns, err := column.FilterOutExcludedColumns(table.Columns, tableCfg.ExcludeColumns, table.PrimaryKeys)
	if err != nil {
		return MySQLAdapter{}, err
	}

	// Include columns (if any) from the table metadata
	columns, err = column.FilterForIncludedColumns(columns, tableCfg.IncludeColumns, table.PrimaryKeys)
	if err != nil {
		return MySQLAdapter{}, err
	}

	return newMySQLAdapter(db, dbName, *table, columns, tableCfg.ToScannerConfig(defaultErrorRetries))
}

func newMySQLAdapter(db *sql.DB, dbName string, table mysql.Table, columns []schema.Column, scannerCfg scan.ScannerConfig) (MySQLAdapter, error) {
	fieldConverters := make([]transformer.FieldConverter, len(columns))
	for i, col := range columns {
		converter, err := converters.ValueConverterForType(col.Type, col.Opts)
		if err != nil {
			return MySQLAdapter{}, fmt.Errorf("failed to build value converter for column %q: %w", col.Name, err)
		}
		fieldConverters[i] = transformer.FieldConverter{Name: col.Name, ValueConverter: converter}
	}

	return MySQLAdapter{
		db:              db,
		dbName:          dbName,
		table:           table,
		columns:         columns,
		fieldConverters: fieldConverters,
		scannerCfg:      scannerCfg,
	}, nil
}

func (m MySQLAdapter) TableName() string {
	return m.table.Name
}

func (m MySQLAdapter) TopicSuffix() string {
	return fmt.Sprintf("%s.%s", m.dbName, m.table.Name)
}

func (m MySQLAdapter) FieldConverters() []transformer.FieldConverter {
	return m.fieldConverters
}

func (m MySQLAdapter) NewIterator() (transformer.RowsIterator, error) {
	return scanner.NewScanner(m.db, m.table, m.columns, m.scannerCfg)
}

func (m MySQLAdapter) PartitionKeys() []string {
	return m.table.PrimaryKeys
}
