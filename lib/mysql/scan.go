package mysql

import (
	"database/sql"
	"fmt"

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
	table     *Table
	batchSize uint
	retryCfg  retry.RetryConfig

	// mutable
	primaryKeys *primary_key.Keys
}

func (t *Table) NewScanner(db *sql.DB, batchSize uint, errorRetries int) (scanner, error) {
	retryCfg, err := retry.NewJitterRetryConfig(jitterBaseMs, jitterMaxMs, errorRetries, retry.AlwaysRetry)
	if err != nil {
		return scanner{}, fmt.Errorf("failed to build retry config: %w", err)
	}

	return scanner{
		db:          db,
		table:       t,
		batchSize:   batchSize,
		retryCfg:    retryCfg,
		primaryKeys: t.PrimaryKeys.Clone(),
	}, nil
}

func (s *scanner) HasNext() bool {
	panic("not implemented")
}

func (s *scanner) Next() ([]map[string]interface{}, error) {
	panic("not implemented")
}
