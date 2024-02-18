package mysql

import (
	"database/sql"

	"github.com/artie-labs/reader/lib/rdbms/primary_key"
)

type scanner struct {
	// immutable
	db           *sql.DB
	table        *Table
	batchSize    uint
	errorRetries int

	// mutable
	primaryKeys *primary_key.Keys
}

func (t *Table) NewScanner(db *sql.DB, batchSize uint, errorRetries int) scanner {
	return scanner{
		db:           db,
		table:        t,
		batchSize:    batchSize,
		errorRetries: errorRetries,
		primaryKeys:  t.PrimaryKeys.Clone(),
	}
}

func (s *scanner) HasNext() bool {
	panic("not implemented")
}

func (s *scanner) Next() ([]map[string]interface{}, error) {
	panic("not implemented")
}
