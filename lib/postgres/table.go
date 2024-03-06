package postgres

import (
	"database/sql"
	"fmt"
	"slices"

	"github.com/artie-labs/reader/lib/postgres/parse"
	"github.com/artie-labs/reader/lib/postgres/schema"
	"github.com/artie-labs/reader/lib/rdbms/primary_key"
)

type Table struct {
	Name   string
	Schema string

	Columns     []schema.Column
	PrimaryKeys []string
}

func NewTable(schema string, name string) *Table {
	return &Table{
		Name:   name,
		Schema: schema,
	}
}

func (t *Table) GetColumnByName(colName string) (*schema.Column, error) {
	index := slices.IndexFunc(t.Columns, func(c schema.Column) bool { return c.Name == colName })
	if index < 0 {
		return nil, fmt.Errorf("failed to find column with name %s", colName)
	}
	return &t.Columns[index], nil
}

func (t *Table) GetColumnsByName(colNames []string) ([]schema.Column, error) {
	var result []schema.Column
	for _, colName := range colNames {
		col, err := t.GetColumnByName(colName)
		if err != nil {
			return nil, err
		}
		result = append(result, *col)
	}
	return result, nil
}

func (t *Table) PopulateColumns(db *sql.DB) error {
	cols, err := schema.DescribeTable(db, t.Schema, t.Name)
	if err != nil {
		return fmt.Errorf("failed to describe table %s.%s: %w", t.Schema, t.Name, err)
	}
	t.Columns = cols

	t.PrimaryKeys, err = schema.GetPrimaryKeys(db, t.Schema, t.Name)
	if err != nil {
		return fmt.Errorf("failed to retrieve primary keys: %w", err)
	}
	return nil
}

func (t *Table) GetPrimaryKeysBounds(db *sql.DB) ([]primary_key.Key, error) {
	keyColumns, err := t.GetColumnsByName(t.PrimaryKeys)
	if err != nil {
		return nil, fmt.Errorf("missing primary key columns: %w", err)
	}

	primaryKeysBounds, err := schema.GetPrimaryKeysBounds(db, t.Schema, t.Name, keyColumns, castColumn)
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
