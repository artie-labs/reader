package scanner

import (
	"database/sql"

	"github.com/artie-labs/reader/lib/mysql"
	"github.com/artie-labs/reader/lib/mysql/schema"
	"github.com/artie-labs/reader/lib/rdbms/primary_key"
	"github.com/artie-labs/reader/lib/rdbms/scan"
)

func NewScanner(db *sql.DB, table mysql.Table, cfg scan.ScannerConfig) (*scan.Scanner, error) {
	primaryKeyBounds, err := table.GetPrimaryKeysBounds(db)
	if err != nil {
		return nil, err
	}

	adapter := scanAdapter{tableName: table.Name, columns: table.Columns}
	return scan.NewScanner(db, primaryKeyBounds, cfg, adapter)
}

type scanAdapter struct {
	tableName string
	columns   []schema.Column
}

func (s scanAdapter) BuildQuery(primaryKeys []primary_key.Key, isFirstBatch bool, batchSize uint) (string, []any, error) {
	return buildScanTableQuery(buildScanTableQueryArgs{
		TableName:           s.tableName,
		PrimaryKeys:         primaryKeys,
		Columns:             s.columns,
		InclusiveLowerBound: isFirstBatch,
		Limit:               batchSize,
	})
}

func (s scanAdapter) ParseRow(values []any) (map[string]any, error) {
	convertedValues, err := schema.ConvertValues(values, s.columns)
	if err != nil {
		return nil, err
	}

	row := make(map[string]any)
	for idx, value := range convertedValues {
		row[s.columns[idx].Name] = value
	}
	return row, nil
}
