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
	BatchSize uint
	// TODO: These two should be []any
	OptionalStartingValues []string
	OptionalEndingValues   []string
	ErrorRetries           int
}

type ScanAdapter interface {
	BuildQuery(primaryKeys []primary_key.Key, isFirstBatch bool, batchSize uint) (string, []any, error)
	ParseRow(row []any) (map[string]any, error)
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

func NewScanner(db *sql.DB, _primaryKeys []primary_key.Key, cfg ScannerConfig, adapter ScanAdapter) (Scanner, error) {
	primaryKeys := primary_key.NewKeys(_primaryKeys)
	if err := primaryKeys.LoadValues(cfg.OptionalStartingValues, cfg.OptionalEndingValues); err != nil {
		return Scanner{}, fmt.Errorf("failed to override primary key values: %w", err)
	}

	retryCfg, err := retry.NewJitterRetryConfig(jitterBaseMs, jitterMaxMs, cfg.ErrorRetries, retry.AlwaysRetry)
	if err != nil {
		return Scanner{}, fmt.Errorf("failed to build retry config: %w", err)
	}

	return Scanner{
		db:           db,
		batchSize:    cfg.BatchSize,
		retryCfg:     retryCfg,
		adapter:      adapter,
		primaryKeys:  primaryKeys.Clone(),
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

	s.isFirstBatch = false

	if len(rows) == 0 || s.primaryKeys.IsExhausted() {
		s.done = true
	} else {
		// Update the starting keys so that the next scan will pick off where we last left off.
		lastRow := rows[len(rows)-1]
		for _, pk := range s.primaryKeys.Keys() {
			if err := s.primaryKeys.UpdateStartingValue(pk.Name, lastRow[pk.Name]); err != nil {
				return nil, err
			}
		}
	}

	return rows, nil
}

func (s *Scanner) scan() ([]map[string]any, error) {
	query, parameters, err := s.adapter.BuildQuery(s.primaryKeys.Keys(), s.isFirstBatch, s.batchSize)
	if err != nil {
		return nil, fmt.Errorf("failed to build scan query: %w", err)
	}

	logger := slog.With(slog.String("query", query))
	if len(parameters) > 0 {
		logger = logger.With(slog.Any("parameters", parameters))
	}
	logger.Info("Scan query")

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

		row, err := s.adapter.ParseRow(values)
		if err != nil {
			return nil, err
		}

		rowsData = append(rowsData, row)
	}
	return rowsData, nil
}
