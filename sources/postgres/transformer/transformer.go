package transformer

import (
	"fmt"
	"strings"

	"github.com/artie-labs/transfer/lib/debezium"

	"github.com/artie-labs/reader/lib/postgres"
)

type postgresAdapter struct {
	table postgres.Table
}

func NewPostgresAdapter(table postgres.Table) postgresAdapter {
	return postgresAdapter{table: table}
}

func (d postgresAdapter) TopicSuffix() string {
	return fmt.Sprintf("%s.%s", d.table.Schema, strings.ReplaceAll(d.table.Name, `"`, ``))
}

// PartitionKey returns a map of primary keys and their values for a given row.
func (d postgresAdapter) PartitionKey(row map[string]interface{}) map[string]interface{} {
	result := make(map[string]interface{})
	for _, key := range d.table.PrimaryKeys.Keys() {
		result[key] = row[key]
	}
	return result
}

func (d postgresAdapter) ConvertRowToDebezium(row map[string]interface{}) (map[string]interface{}, error) {
	result := make(map[string]interface{})
	for key, value := range row {
		col, err := d.table.GetColumnByName(key)
		if err != nil {
			return nil, fmt.Errorf("failed to get column %s by name: %w", key, err)
		}

		val, err := ParseValue(*col, value)
		if err != nil {
			return nil, fmt.Errorf("failed to convert value: %w", err)
		}

		result[key] = val
	}
	return result, nil
}

func (p postgresAdapter) Fields() []debezium.Field {
	return ColumnsToFields(p.table.Columns)
}

func (p postgresAdapter) TableName() string {
	return p.table.Name
}
