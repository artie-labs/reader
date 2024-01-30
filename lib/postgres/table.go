package postgres

import (
	"database/sql"
	"fmt"
	"log/slog"
	"slices"
	"strings"

	"github.com/artie-labs/transfer/lib/ptr"

	"github.com/artie-labs/reader/config"
	"github.com/artie-labs/reader/lib/postgres/primary_key"
	"github.com/artie-labs/reader/lib/postgres/queries"
)

type Table struct {
	Name   string
	Schema string

	PrimaryKeys *primary_key.Keys

	// TODO: `OriginalColumns` and `ColumnsCastedForScanning` can be merged later to be more concise.
	OriginalColumns          []string
	ColumnsCastedForScanning []string
	Config                   *Config

	OptionalPrimaryKeyValStart string
	OptionalPrimaryKeyValEnd   string
}

func NewTable(cfgTable *config.PostgreSQLTable) *Table {
	return &Table{
		Name:                       cfgTable.Name,
		Schema:                     cfgTable.Schema,
		PrimaryKeys:                primary_key.NewKeys(),
		Config:                     NewPostgresConfig(),
		OptionalPrimaryKeyValStart: cfgTable.OptionalPrimaryKeyValStart,
		OptionalPrimaryKeyValEnd:   cfgTable.OptionalPrimaryKeyValEnd,
	}
}

func (t *Table) TopicSuffix() string {
	return fmt.Sprintf("%s.%s", t.Schema, strings.ReplaceAll(t.Name, `"`, ``))
}

func (t *Table) findPrimaryKeys(db *sql.DB) error {
	sqlQuery, sqlArgs := queries.RetrievePrimaryKeys(
		queries.RetrievePrimaryKeysArgs{
			Schema:    t.Schema,
			TableName: t.Name,
		},
	)

	rows, err := db.Query(sqlQuery, sqlArgs...)
	if err != nil {
		return fmt.Errorf("failed to query, err: %w, sqlQuery: %s, sqlArgs: %v", err, sqlQuery, sqlArgs)
	}

	for rows.Next() {
		var primaryKey string
		err = rows.Scan(&primaryKey)
		if err != nil {
			return err
		}

		// Just fill the name in first, values will be loaded later.
		t.PrimaryKeys.Upsert(primaryKey, nil, nil)
	}

	return nil
}

func (t *Table) FindStartAndEndPrimaryKeys(db *sql.DB) error {
	if err := t.findPrimaryKeys(db); err != nil {
		return fmt.Errorf("failed looking up primary keys, error: %v", err)
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

	values := make([]interface{}, t.PrimaryKeys.Length())
	scannedMaxPkValues := make([]interface{}, t.PrimaryKeys.Length())
	for i := range values {
		scannedMaxPkValues[i] = &values[i]
	}

	maxQuery := queries.SelectTableQuery(queries.SelectTableQueryArgs{
		Keys:       castedPrimaryKeys,
		Schema:     t.Schema,
		TableName:  t.Name,
		OrderBy:    t.PrimaryKeys.Keys(),
		Descending: true,
	})

	slog.Info("Find max pk query", slog.String("query", maxQuery))
	err := db.QueryRow(maxQuery).Scan(scannedMaxPkValues...)
	if err != nil {
		return err
	}

	for idx, maxValue := range values {
		val, err := t.Config.ParseValue(ParseValueArgs{
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

	minValues := make([]interface{}, t.PrimaryKeys.Length())
	scannedMinPkValues := make([]interface{}, t.PrimaryKeys.Length())
	for i := range minValues {
		scannedMinPkValues[i] = &minValues[i]
	}

	minQuery := queries.SelectTableQuery(queries.SelectTableQueryArgs{
		Keys:      castedPrimaryKeys,
		Schema:    t.Schema,
		TableName: t.Name,
		OrderBy:   t.PrimaryKeys.Keys(),
	})

	slog.Info("Find min pk query", slog.String("query", minQuery))
	err = db.QueryRow(minQuery).Scan(scannedMinPkValues...)
	if err != nil {
		return err
	}

	for idx, minValue := range minValues {
		val, err := t.Config.ParseValue(ParseValueArgs{
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
