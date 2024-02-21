package scanner

import (
	"database/sql"
	"fmt"
	"log/slog"

	"github.com/artie-labs/reader/lib/mysql"
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
	isLastBatch  bool
	done         bool
}

func NewScanner(db *sql.DB, table mysql.Table, batchSize uint, errorRetries int) (scanner, error) {
	retryCfg, err := retry.NewJitterRetryConfig(jitterBaseMs, jitterMaxMs, errorRetries, retry.AlwaysRetry)
	if err != nil {
		return scanner{}, fmt.Errorf("failed to build retry config: %w", err)
	}

	return scanner{
		db:           db,
		table:        table,
		batchSize:    batchSize,
		retryCfg:     retryCfg,
		primaryKeys:  table.PrimaryKeys.Clone(),
		isFirstBatch: true,
		isLastBatch:  false,
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

	if len(rows) == 0 {
		slog.Info("Finished scanning", slog.String("table", s.table.Name))
		s.done = true
		return nil, nil
	}

	s.isFirstBatch = false
	// The reason why lastRow exists is because in the past, we had queries only return partial results but it wasn't fully done
	s.isLastBatch = s.batchSize > uint(len(rows))

	return rows, nil
}

func (s *scanner) scan() ([]map[string]interface{}, error) {
	query, parameters, err := buildScanTableQuery(buildScanTableQueryArgs{
		TableName:   s.table.Name,
		PrimaryKeys: s.primaryKeys,
		Columns:     s.table.Columns,

		InclusiveLowerBound: s.isFirstBatch,
		InclusiveUpperBound: !s.isLastBatch,

		Limit: s.batchSize,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to generate query: %w", err)
	}

	slog.Info("Scan query", slog.String("query", query), slog.Any("parameters", parameters))

	panic("not implemented")
}
