package mysql

import (
	"database/sql"
	"fmt"
	"slices"
	"strings"

	"github.com/artie-labs/reader/config"
	"github.com/artie-labs/reader/lib/mysql/schema"
)

type Table struct {
	Name string

	Columns []schema.Column

	OptionalPrimaryKeyValStart string
	OptionalPrimaryKeyValEnd   string
}

func NewTable(cfgTable config.MySQLTable) *Table {
	return &Table{
		Name:                       cfgTable.Name,
		OptionalPrimaryKeyValStart: cfgTable.OptionalPrimaryKeyValStart,
		OptionalPrimaryKeyValEnd:   cfgTable.OptionalPrimaryKeyValEnd,
	}
}

func (t *Table) TopicSuffix() string {
	return strings.ReplaceAll(t.Name, `"`, ``)
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
	cols, err := schema.DescribeTable(db, t.Name)
	if err != nil {
		return fmt.Errorf("failed to describe table %s: %w", t.Name, err)
	}
	t.Columns = cols

	// TODO: Fetch primary keys
	return nil
}
