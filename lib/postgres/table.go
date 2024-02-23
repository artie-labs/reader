package postgres

import (
	"database/sql"
	"fmt"
	"slices"

	"github.com/artie-labs/reader/lib/postgres/schema"
	"github.com/artie-labs/reader/lib/rdbms/primary_key"
)

type Table struct {
	Name   string
	Schema string

	Columns     []schema.Column
	PrimaryKeys []primary_key.Key
}

func NewTable(schema string, name string) *Table {
	return &Table{
		Name:   name,
		Schema: schema,
	}
}

func (t *Table) GetName() string {
	return t.Name
}

func (t *Table) GetPrimaryKeys() []primary_key.Key {
	return t.PrimaryKeys
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

	return t.findStartAndEndPrimaryKeys(db)
}

func (t *Table) findStartAndEndPrimaryKeys(db *sql.DB) error {
	keys, err := schema.GetPrimaryKeys(db, t.Schema, t.Name)
	if err != nil {
		return fmt.Errorf("failed to retrieve primary keys: %w", err)
	}

	keyColumns, err := t.GetColumnsByName(keys)
	if err != nil {
		return fmt.Errorf("missing primary key columns: %w", err)
	}

	primaryKeysBounds, err := schema.GetPrimaryKeysBounds(db, t.Schema, t.Name, keyColumns, castColumn)
	if err != nil {
		return fmt.Errorf("failed to retrieve bounds for primary keys: %w", err)
	}

	t.PrimaryKeys = make([]primary_key.Key, len(primaryKeysBounds))
	for idx, bounds := range primaryKeysBounds {
		col := keyColumns[idx]

		minVal, err := ParseValue(col.Type, ParseValueArgs{
			ValueWrapper: ValueWrapper{Value: bounds.Min},
			ParseTime:    true,
		})
		if err != nil {
			return err
		}

		maxVal, err := ParseValue(col.Type, ParseValueArgs{
			ValueWrapper: ValueWrapper{Value: bounds.Max},
			ParseTime:    true,
		})
		if err != nil {
			return err
		}

		t.PrimaryKeys[idx] = primary_key.Key{
			Name:          col.Name,
			StartingValue: minVal.Value,
			EndingValue:   maxVal.Value,
		}
	}

	return nil
}
