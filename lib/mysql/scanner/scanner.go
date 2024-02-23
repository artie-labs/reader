package scanner

import (
	"database/sql"
	"fmt"
	"log/slog"

	"github.com/artie-labs/reader/lib/mysql"
	"github.com/artie-labs/reader/lib/mysql/schema"
	"github.com/artie-labs/reader/lib/rdbms/primary_key"
	"github.com/artie-labs/reader/lib/rdbms/scan"
	"github.com/artie-labs/transfer/lib/retry"
)

func NewScanner(db *sql.DB, table mysql.Table, cfg scan.ScannerConfig) (scan.Scanner[*mysql.Table], error) {
	return scan.NewScanner(db, &table, cfg, _scan)
}

func _scan(s *scan.Scanner[*mysql.Table], primaryKeys *primary_key.Keys, isFirstBatch bool) ([]map[string]any, error) {
	query, parameters, err := buildScanTableQuery(buildScanTableQueryArgs{
		TableName:           s.Table.Name,
		PrimaryKeys:         primaryKeys,
		Columns:             s.Table.Columns,
		InclusiveLowerBound: isFirstBatch,
		Limit:               s.BatchSize,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to generate query: %w", err)
	}

	slog.Info("Scan query", slog.String("query", query), slog.Any("parameters", parameters))

	rows, err := retry.WithRetriesAndResult(s.RetryCfg, func(_ int, _ error) (*sql.Rows, error) {
		return s.DB.Query(query, parameters...)
	})
	if err != nil {
		return nil, fmt.Errorf("failed to scan table: %w", err)
	}

	values := make([]any, len(s.Table.Columns))
	valuePtrs := make([]any, len(values))
	for i := range values {
		valuePtrs[i] = &values[i]
	}

	var rowsData []map[string]any
	for rows.Next() {
		err = rows.Scan(valuePtrs...)
		if err != nil {
			return nil, err
		}

		convertedValues, err := schema.ConvertValues(values, s.Table.Columns)
		if err != nil {
			return nil, err
		}

		row := make(map[string]any)
		for idx, value := range convertedValues {
			row[s.Table.Columns[idx].Name] = value
		}
		rowsData = append(rowsData, row)
	}

	if len(rowsData) == 0 {
		return rowsData, nil
	}

	// Update the starting key so that the next scan will pick off where we last left off.
	lastRow := rowsData[len(rowsData)-1]
	for _, pk := range primaryKeys.Keys() {
		if err := primaryKeys.UpdateStartingValue(pk.Name, lastRow[pk.Name]); err != nil {
			return nil, err
		}
	}

	return rowsData, nil
}
