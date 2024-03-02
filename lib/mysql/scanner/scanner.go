package scanner

import (
	"database/sql"

	"github.com/artie-labs/reader/lib/mysql"
	"github.com/artie-labs/reader/lib/mysql/schema"
	"github.com/artie-labs/reader/lib/rdbms/primary_key"
	"github.com/artie-labs/reader/lib/rdbms/scan"
)

func NewScanner(db *sql.DB, table mysql.Table, cfg scan.ScannerConfig) (scan.Scanner, error) {
	return scan.NewScanner(
		db,
		table.PrimaryKeys,
		scanAdapter{tableName: table.Name, columns: table.Columns},
		cfg,
	)
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

func (s scanAdapter) ParseRows(rows *sql.Rows) ([]map[string]any, error) {
	values := make([]any, len(s.columns))
	valuePtrs := make([]any, len(values))
	for i := range values {
		valuePtrs[i] = &values[i]
	}

	var rowsData []map[string]any
	for rows.Next() {
		err := rows.Scan(valuePtrs...)
		if err != nil {
			return nil, err
		}

		convertedValues, err := schema.ConvertValues(values, s.columns)
		if err != nil {
			return nil, err
		}

		row := make(map[string]any)
		for idx, value := range convertedValues {
			row[s.columns[idx].Name] = value
		}
		rowsData = append(rowsData, row)
	}
	return rowsData, nil
}
