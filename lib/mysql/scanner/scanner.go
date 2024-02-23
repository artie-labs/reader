package scanner

import (
	"database/sql"
	"fmt"
	"log/slog"

	"github.com/artie-labs/reader/lib/mysql"
	"github.com/artie-labs/reader/lib/mysql/schema"
	"github.com/artie-labs/reader/lib/rdbms/primary_key"
	"github.com/artie-labs/transfer/lib/retry"
)

const (
	jitterBaseMs = 300
	jitterMaxMs  = 5000
)

type scanner struct {
	// immutable
	db        *sql.DB
	table     mysql.Table
	batchSize uint
	retryCfg  retry.RetryConfig

	// mutable
	primaryKeys  *primary_key.Keys
	isFirstBatch bool
	done         bool
}

func NewScanner(db *sql.DB, table mysql.Table, primaryKeys *primary_key.Keys, batchSize uint, errorRetries int) (scanner, error) {
	retryCfg, err := retry.NewJitterRetryConfig(jitterBaseMs, jitterMaxMs, errorRetries, retry.AlwaysRetry)
	if err != nil {
		return scanner{}, fmt.Errorf("failed to build retry config: %w", err)
	}

	return scanner{
		db:           db,
		table:        table,
		batchSize:    batchSize,
		retryCfg:     retryCfg,
		primaryKeys:  primaryKeys.Clone(),
		isFirstBatch: true,
		done:         false,
	}, nil
}

func (s *scanner) HasNext() bool {
	return !s.done
}

func (s *scanner) Next() ([]map[string]interface{}, error) {
	if !s.HasNext() {
		return nil, fmt.Errorf("no more rows to scan")
	}

	rows, err := s.scan()
	if err != nil {
		s.done = true
		return nil, err
	}

	if len(rows) == 0 || s.primaryKeys.IsExhausted() {
		slog.Info("Finished scanning", slog.String("table", s.table.Name))
		s.done = true
	}

	s.isFirstBatch = false

	return rows, nil
}

func (s *scanner) scan() ([]map[string]interface{}, error) {
	query, parameters, err := buildScanTableQuery(buildScanTableQueryArgs{
		TableName:           s.table.Name,
		PrimaryKeys:         s.primaryKeys,
		Columns:             s.table.Columns,
		InclusiveLowerBound: s.isFirstBatch,
		Limit:               s.batchSize,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to generate query: %w", err)
	}

	slog.Info("Scan query", slog.String("query", query), slog.Any("parameters", parameters))

	rows, err := retry.WithRetriesAndResult(s.retryCfg, func(_ int, _ error) (*sql.Rows, error) {
		return s.db.Query(query, parameters...)
	})
	if err != nil {
		return nil, fmt.Errorf("failed to scan table: %w", err)
	}

	values := make([]interface{}, len(s.table.Columns))
	valuePtrs := make([]interface{}, len(values))
	for i := range values {
		valuePtrs[i] = &values[i]
	}

	var rowsData []map[string]interface{}
	for rows.Next() {
		err = rows.Scan(valuePtrs...)
		if err != nil {
			return nil, err
		}

		row := make(map[string]interface{})
		for idx, value := range values {
			col := s.table.Columns[idx]
			row[col.Name], err = convertValue(col.Type, value)
			if err != nil {
				return nil, fmt.Errorf("faild to convert value for column %s: %w", col.Name, err)
			}
		}
		rowsData = append(rowsData, row)
	}

	if len(rowsData) == 0 {
		return rowsData, nil
	}

	// Update the starting key so that the next scan will pick off where we last left off.
	lastRow := rowsData[len(rowsData)-1]
	for _, pk := range s.primaryKeys.Keys() {
		if err := s.primaryKeys.UpdateStartingValue(pk, lastRow[pk]); err != nil {
			return nil, err
		}
	}

	return rowsData, nil
}

func convertValue(colType schema.DataType, value interface{}) (interface{}, error) {
	// TODO: test this function with all mysql data types

	if value == nil {
		return nil, nil
	}

	switch colType {
	case schema.Varchar,
		schema.Text:
		switch castValue := value.(type) {
		case []byte:
			return string(castValue), nil
		case string:
			return castValue, nil
		default:
			return nil, fmt.Errorf("could not cast value to string: %v", value)
		}
	}

	return value, nil
}
