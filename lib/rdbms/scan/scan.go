package scan

import (
	"database/sql"
	"fmt"
	"log/slog"

	"github.com/artie-labs/reader/lib/rdbms/primary_key"
	"github.com/artie-labs/transfer/lib/retry"
)

const (
	jitterBaseMs = 300
	jitterMaxMs  = 5000
)

type ScannerConfig struct {
	BatchSize              uint
	OptionalStartingValues []string
	OptionalEndingValues   []string
	ErrorRetries           int
}

type ScanAdapter interface {
	ParsePrimaryKeyValue(columnName string, value string) (any, error)
	BuildQuery(primaryKeys []primary_key.Key, isFirstBatch bool, batchSize uint) (string, []any)
	ParseRow(row []any) error
}

type Scanner struct {
	// immutable
	db        *sql.DB
	batchSize uint
	retryCfg  retry.RetryConfig
	adapter   ScanAdapter

	// mutable
	primaryKeys  *primary_key.Keys
	isFirstBatch bool
	done         bool
}

func NewScanner(db *sql.DB, _primaryKeys []primary_key.Key, cfg ScannerConfig, adapter ScanAdapter) (*Scanner, error) {
	optionalStartingValues, err := parsePkValueOverrides(cfg.OptionalStartingValues, _primaryKeys, adapter)
	if err != nil {
		return nil, fmt.Errorf("failed to parse optional starting values: %w", err)
	}

	optionalEndingValues, err := parsePkValueOverrides(cfg.OptionalEndingValues, _primaryKeys, adapter)
	if err != nil {
		return nil, fmt.Errorf("failed to parse optional ending values: %w", err)
	}

	primaryKeys := primary_key.NewKeys(_primaryKeys)
	if err := primaryKeys.LoadValues(optionalStartingValues, optionalEndingValues); err != nil {
		return nil, fmt.Errorf("failed to override primary key values: %w", err)
	}

	retryCfg, err := retry.NewJitterRetryConfig(jitterBaseMs, jitterMaxMs, cfg.ErrorRetries, retry.AlwaysRetry)
	if err != nil {
		return nil, fmt.Errorf("failed to build retry config: %w", err)
	}

	return &Scanner{
		db:           db,
		batchSize:    cfg.BatchSize,
		retryCfg:     retryCfg,
		adapter:      adapter,
		primaryKeys:  primaryKeys,
		isFirstBatch: true,
		done:         false,
	}, nil
}

func (s *Scanner) HasNext() bool {
	return !s.done
}

func (s *Scanner) Next() ([]map[string]any, error) {
	if !s.HasNext() {
		return nil, fmt.Errorf("no more rows to scan")
	}

	rows, err := s.scan()
	if err != nil {
		s.done = true
		return nil, err
	}

	wasFirstBatch := s.isFirstBatch
	s.isFirstBatch = false

	if len(rows) == 0 || s.primaryKeys.IsExhausted() {
		s.done = true
	} else {
		// Update the starting keys so that the next scan will pick off where we last left off.
		lastRow := rows[len(rows)-1]
		var startingValuesChanged bool
		for _, pk := range s.primaryKeys.Keys() {
			changed, err := s.primaryKeys.UpdateStartingValue(pk.Name, lastRow[pk.Name])
			if err != nil {
				s.done = true
				return nil, err
			}
			startingValuesChanged = startingValuesChanged || changed
		}

		if !wasFirstBatch && !startingValuesChanged {
			// Detect situations where the scanner is stuck in a loop.
			// The second batch will use a > comparision instead of a > comparision for the lower bound.
			return nil, fmt.Errorf("primarky key start values did not change, scanner is stuck in a loop")
		}
	}

	return rows, nil
}

func (s *Scanner) scan() ([]map[string]any, error) {
	query, parameters := s.adapter.BuildQuery(s.primaryKeys.Keys(), s.isFirstBatch, s.batchSize)
	slog.Info("Scan query", slog.String("query", query), slog.Any("parameters", parameters))

	rows, err := retry.WithRetriesAndResult(s.retryCfg, func(_ int, _ error) (*sql.Rows, error) {
		return s.db.Query(query, parameters...)
	})
	if err != nil {
		return nil, fmt.Errorf("scan failed: %w", err)
	}

	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("failed to get columns: %w", err)
	}

	values := make([]any, len(columns))
	valuePtrs := make([]any, len(values))
	for i := range values {
		valuePtrs[i] = &values[i]
	}

	var rowsData []map[string]any
	for rows.Next() {
		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, err
		}

		if err := s.adapter.ParseRow(values); err != nil {
			return nil, err
		}

		row := make(map[string]any)
		for i, column := range columns {
			row[column] = values[i]
		}
		rowsData = append(rowsData, row)
	}
	return rowsData, nil
}

// parsePkValueOverrides converts primary key starting/ending string values coming from db config files into values
// usable by the db driver.
func parsePkValueOverrides(values []string, primaryKeys []primary_key.Key, adapter ScanAdapter) ([]any, error) {
	if len(values) == 0 {
		return make([]any, 0), nil
	}

	if len(values) != len(primaryKeys) {
		return nil, fmt.Errorf("keys (%d), and override values (%d) length does not match, keys: %v, values: %v",
			len(primaryKeys), len(values), primaryKeys, values)
	}

	result := make([]any, len(values))
	for i, value := range values {
		parsedValue, err := adapter.ParsePrimaryKeyValue(primaryKeys[i].Name, value)
		if err != nil {
			return nil, fmt.Errorf("failed to parse value '%v': %w", value, err)
		}
		result[i] = parsedValue
	}
	return result, nil
}
