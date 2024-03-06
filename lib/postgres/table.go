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

func LoadTable(db *sql.DB, _schema string, name string) (*Table, error) {
	tbl := &Table{
		Name:   name,
		Schema: _schema,
	}

	var err error
	tbl.Columns, err = schema.DescribeTable(db, tbl.Schema, tbl.Name)
	if err != nil {
		return nil, fmt.Errorf("failed to describe table %s.%s: %w", tbl.Schema, tbl.Name, err)
	}

	tbl.PrimaryKeys, err = schema.GetPrimaryKeys(db, tbl.Schema, tbl.Name)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve primary keys: %w", err)
	}

	return tbl, nil
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
