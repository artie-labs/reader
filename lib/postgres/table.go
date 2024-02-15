package postgres

import (
	"database/sql"
	"fmt"
	"log/slog"
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

	// TODO: `OriginalColumns` and `ColumnsCastedForScanning` can be merged later to be more concise.
	OriginalColumns          []string
	ColumnsCastedForScanning []string
	Fields                   *debezium.Fields

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

func (t *Table) FindStartAndEndPrimaryKeys(db *sql.DB) error {
	primaryKeys, err := schema.GetPrimaryKeys(db, t.Schema, t.Name)
	if err != nil {
		return fmt.Errorf("failed to retrieve primary keys: %w", err)
	}

	for _, primaryKey := range primaryKeys {
		// Just fill the name in first, values will be loaded later.
		t.PrimaryKeys.Upsert(primaryKey, nil, nil)
	}

	keys := t.PrimaryKeys.Keys()
	var castedPrimaryKeys []string
	for _, primaryKey := range keys {
		index := slices.Index(t.OriginalColumns, primaryKey)
		if index < 0 {
			return fmt.Errorf("failed to find primary key from original columns, key: %s, originalColumns: %v, index: %d", primaryKey, t.OriginalColumns, index)
		}

		castedPrimaryKeys = append(castedPrimaryKeys, t.ColumnsCastedForScanning[index])
	}

	values, err := schema.GetPrimaryKeysUpperBounds(db, t.Schema, t.Name, keys, castedPrimaryKeys)
	if err != nil {
		return fmt.Errorf("failed to retrieve upper bounds for primary keys: %w", err)
	}

	for idx, maxValue := range values {
		val, err := ParseValue(t.Fields, ParseValueArgs{
			ColName: keys[idx],
			ValueWrapper: ValueWrapper{
				Value: maxValue,
			},
			ParseTime: true,
		})

		if err != nil {
			return err
		}

		t.PrimaryKeys.Upsert(keys[idx], nil, ptr.ToString(val.String()))
	}

	minValues, err := schema.GetPrimaryKeysLowerBounds(db, t.Schema, t.Name, keys, castedPrimaryKeys)
	if err != nil {
		return fmt.Errorf("failed to retrieve lower bounds for primary keys: %w", err)
	}

	slog.Info("Primary keys bounds", slog.Any("min", minValues), slog.Any("max", values))

	for idx, minValue := range minValues {
		val, err := ParseValue(t.Fields, ParseValueArgs{
			ColName: keys[idx],
			ValueWrapper: ValueWrapper{
				Value: minValue,
			},
			ParseTime: true,
		})

		if err != nil {
			return err
		}

		t.PrimaryKeys.Upsert(keys[idx], ptr.ToString(val.String()), nil)
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
