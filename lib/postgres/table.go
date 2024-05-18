package postgres

import (
	"database/sql"
	"fmt"

	"github.com/artie-labs/reader/lib/postgres/parse"
	"github.com/artie-labs/reader/lib/postgres/schema"
	"github.com/artie-labs/reader/lib/rdbms/column"
	"github.com/artie-labs/reader/lib/rdbms/primary_key"
)

type Table struct {
	Name   string
	Schema string

	Columns     []schema.Column
	PrimaryKeys []string
}

func LoadTable(db *sql.DB, _schema string, name string) (*Table, error) {
	tbl := &Table{
		Name:   name,
		Schema: _schema,
	}

	var err error
	if tbl.Columns, err = schema.DescribeTable(db, tbl.Schema, tbl.Name); err != nil {
		return nil, fmt.Errorf("failed to describe table %s.%s: %w", tbl.Schema, tbl.Name, err)
	}

	if tbl.PrimaryKeys, err = schema.FetchPrimaryKeys(db, tbl.Schema, tbl.Name); err != nil {
		return nil, fmt.Errorf("failed to retrieve primary keys: %w", err)
	}

	return tbl, nil
}

func (t *Table) FetchPrimaryKeysBounds(db *sql.DB) ([]primary_key.Key, error) {
	keyColumns, err := column.GetColumnsByName(t.Columns, t.PrimaryKeys)
	if err != nil {
		return nil, fmt.Errorf("missing primary key columns: %w", err)
	}

	primaryKeysBounds, err := schema.FetchPrimaryKeysBounds(db, t.Schema, t.Name, keyColumns)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve bounds for primary keys: %w", err)
	}

	result := make([]primary_key.Key, len(primaryKeysBounds))
	for idx, bounds := range primaryKeysBounds {
		col := keyColumns[idx]

		minVal, err := parse.ParseValue(col.Type, bounds.Min)
		if err != nil {
			return nil, err
		}

		maxVal, err := parse.ParseValue(col.Type, bounds.Max)
		if err != nil {
			return nil, err
		}

		result[idx] = primary_key.Key{
			Name:          col.Name,
			StartingValue: minVal,
			EndingValue:   maxVal,
		}
	}
	return result, nil
}
