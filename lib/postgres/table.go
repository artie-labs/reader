package postgres

import (
	"database/sql"
	"fmt"
	"slices"
	"strings"

	"github.com/artie-labs/transfer/lib/ptr"

	"github.com/artie-labs/reader/config"
	"github.com/artie-labs/reader/lib/postgres/debezium"
	"github.com/artie-labs/reader/lib/postgres/primary_key"
	"github.com/artie-labs/reader/lib/postgres/schema"
)

type Table struct {
	Name   string
	Schema string

	PrimaryKeys *primary_key.Keys

	Columns []schema.Column
	Fields  *debezium.Fields

	OptionalPrimaryKeyValStart string
	OptionalPrimaryKeyValEnd   string
}

func NewTable(cfgTable *config.PostgreSQLTable) *Table {
	return &Table{
		Name:                       cfgTable.Name,
		Schema:                     cfgTable.Schema,
		PrimaryKeys:                primary_key.NewKeys(),
		Fields:                     debezium.NewFields(),
		OptionalPrimaryKeyValStart: cfgTable.OptionalPrimaryKeyValStart,
		OptionalPrimaryKeyValEnd:   cfgTable.OptionalPrimaryKeyValEnd,
	}
}

func (t *Table) TopicSuffix() string {
	return fmt.Sprintf("%s.%s", t.Schema, strings.ReplaceAll(t.Name, `"`, ``))
}

func (t *Table) PopulateColumns(db *sql.DB) error {
	cols, err := schema.DescribeTable(db, t.Schema, t.Name)
	if err != nil {
		return fmt.Errorf("failed to describe table %s.%s: %w", t.Schema, t.Name, err)
	}

	for _, col := range cols {
		t.Fields.AddField(col.Name, col.Type, col.Opts)
		t.Columns = append(t.Columns, col)
	}

	return t.findStartAndEndPrimaryKeys(db)
}

func (t *Table) findStartAndEndPrimaryKeys(db *sql.DB) error {
	keys, err := schema.GetPrimaryKeys(db, t.Schema, t.Name)
	if err != nil {
		return fmt.Errorf("failed to retrieve primary keys: %w", err)
	}

	var castedPrimaryKeys []string
	for _, primaryKey := range keys {
		index := slices.IndexFunc(t.Columns, func(c schema.Column) bool { return c.Name == primaryKey })
		if index < 0 {
			return fmt.Errorf("failed to find primary key from original columns, key: %s, columns: %v", primaryKey, t.Columns)
		}
		col := t.Columns[index]
		castedPrimaryKeys = append(castedPrimaryKeys, castColumn(col))
	}

	primaryKeysBounds, err := schema.GetPrimaryKeysBounds(db, t.Schema, t.Name, keys, castedPrimaryKeys)
	if err != nil {
		return fmt.Errorf("failed to retrieve bounds for primary keys: %w", err)
	}

	for idx, bound := range primaryKeysBounds {
		colName := keys[idx]

		minVal, err := ParseValue(t.Fields, ParseValueArgs{
			ColName:      colName,
			ValueWrapper: ValueWrapper{Value: bound.Min},
			ParseTime:    true,
		})
		if err != nil {
			return err
		}

		maxVal, err := ParseValue(t.Fields, ParseValueArgs{
			ColName:      colName,
			ValueWrapper: ValueWrapper{Value: bound.Max},
			ParseTime:    true,
		})
		if err != nil {
			return err
		}

		t.PrimaryKeys.Upsert(colName, ptr.ToString(minVal.String()), ptr.ToString(maxVal.String()))
	}

	return t.PrimaryKeys.LoadValues(t.OptionalPrimaryKeyValStart, t.OptionalPrimaryKeyValEnd)
}

// PartitionKeyMap returns a map of primary keys and their values for a given row.
func (t *Table) PartitionKey(row map[string]interface{}) map[string]interface{} {
	result := make(map[string]interface{})
	for _, key := range t.PrimaryKeys.Keys() {
		result[key] = row[key]
	}
	return result
}
