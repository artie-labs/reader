package mysql

import (
	"database/sql"
	"fmt"

	"github.com/artie-labs/reader/lib/mysql/schema"
	"github.com/artie-labs/reader/lib/rdbms/column"
	"github.com/artie-labs/reader/lib/rdbms/primary_key"
)

type Table struct {
	Name string

	Columns     []schema.Column
	PrimaryKeys []string
}

func LoadTable(db *sql.DB, name string) (*Table, error) {
	tbl := &Table{
		Name: name,
	}

	var err error
	if tbl.Columns, err = schema.DescribeTable(db, tbl.Name); err != nil {
		return nil, fmt.Errorf("failed to describe table %s: %w", tbl.Name, err)
	}

	if tbl.PrimaryKeys, err = schema.GetPrimaryKeys(db, tbl.Name); err != nil {
		return nil, fmt.Errorf("failed to retrieve primary keys: %w", err)
	}

	return tbl, nil
}

func (t *Table) GetPrimaryKeysBounds(db *sql.DB) ([]primary_key.Key, error) {
	keyColumns, err := column.GetColumnsByName(t.Columns, t.PrimaryKeys)
	if err != nil {
		return nil, fmt.Errorf("missing primary key columns: %w", err)
	}

	primaryKeysBounds, err := schema.GetPrimaryKeysBounds(db, t.Name, keyColumns)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve bounds for primary keys: %w", err)
	}

	result := make([]primary_key.Key, len(primaryKeysBounds))
	for idx, bounds := range primaryKeysBounds {
		result[idx] = primary_key.Key{
			Name:          keyColumns[idx].Name,
			StartingValue: bounds.Min,
			EndingValue:   bounds.Max,
		}
	}
	return result, nil
}
