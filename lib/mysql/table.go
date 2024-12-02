package mysql

import (
	"database/sql"
	"fmt"

	"github.com/artie-labs/reader/lib/mysql/schema"
	"github.com/artie-labs/reader/lib/rdbms/column"
	"github.com/artie-labs/reader/lib/rdbms/primary_key"
)

type Table struct {
	Name        string
	Columns     []schema.Column
	PrimaryKeys []string
}

func LoadTable(db *sql.DB, name string) (Table, error) {
	cols, err := schema.DescribeTable(db, name)
	if err != nil {
		return Table{}, fmt.Errorf("failed to describe table %q: %w", name, err)
	}

	pks, err := schema.FetchPrimaryKeys(db, name)
	if err != nil {
		return Table{}, fmt.Errorf("failed to retrieve primary keys: %w", err)
	}

	return Table{Name: name, Columns: cols, PrimaryKeys: pks}, nil
}

func (t *Table) FetchPrimaryKeysBounds(db *sql.DB) ([]primary_key.Key, error) {
	keyColumns, err := column.ByNames(t.Columns, t.PrimaryKeys)
	if err != nil {
		return nil, fmt.Errorf("missing primary key columns: %w", err)
	}

	primaryKeysBounds, err := schema.FetchPrimaryKeysBounds(db, t.Name, keyColumns)
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
