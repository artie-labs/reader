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

type Table interface {
	GetName() string
	GetPrimaryKeys() []primary_key.Key
}

type ScannerConfig struct {
	BatchSize uint
	// TODO: These two should be []any
	OptionalStartingValues []string
	OptionalEndingValues   []string
	ErrorRetries           int
}

type Scanner[T Table] struct {
	// immutable
	DB        *sql.DB
	Table     T
	BatchSize uint
	RetryCfg  retry.RetryConfig
	scan      func(scanner *Scanner[T], primaryKeys []primary_key.Key, isFirstBatch bool) ([]map[string]any, error)

	// mutable
	primaryKeys  *primary_key.Keys
	isFirstBatch bool
	done         bool
}

func NewScanner[T Table](
	db *sql.DB,
	table T,
	cfg ScannerConfig,
	scan func(scanner *Scanner[T], primaryKeys []primary_key.Key, isFirstBatch bool) ([]map[string]any, error),
) (Scanner[T], error) {
	primaryKeys := primary_key.NewKeys(table.GetPrimaryKeys())
	if err := primaryKeys.LoadValues(cfg.OptionalStartingValues, cfg.OptionalEndingValues); err != nil {
		return Scanner[T]{}, fmt.Errorf("failed to override primary key values: %w", err)
	}

	retryCfg, err := retry.NewJitterRetryConfig(jitterBaseMs, jitterMaxMs, cfg.ErrorRetries, retry.AlwaysRetry)
	if err != nil {
		return Scanner[T]{}, fmt.Errorf("failed to build retry config: %w", err)
	}

	return Scanner[T]{
		DB:           db,
		Table:        table,
		BatchSize:    cfg.BatchSize,
		RetryCfg:     retryCfg,
		scan:         scan,
		primaryKeys:  primaryKeys.Clone(),
		isFirstBatch: true,
		done:         false,
	}, nil
}

func (s *Scanner[T]) HasNext() bool {
	return !s.done
}

func (s *Scanner[T]) Next() ([]map[string]any, error) {
	if !s.HasNext() {
		return nil, fmt.Errorf("no more rows to scan")
	}

	rows, err := s.scan(s, s.primaryKeys.Keys(), s.isFirstBatch)
	if err != nil {
		s.done = true
		return nil, err
	}

	s.isFirstBatch = false

	if len(rows) == 0 || s.primaryKeys.IsExhausted() {
		slog.Info("Finished scanning", slog.String("table", s.Table.GetName()))
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
