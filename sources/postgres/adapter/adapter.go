package adapter

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

func (p postgresAdapter) TableName() string {
	return p.table.Name
}

func (p postgresAdapter) TopicSuffix() string {
	return fmt.Sprintf("%s.%s", p.table.Schema, strings.ReplaceAll(p.table.Name, `"`, ``))
}

func (p postgresAdapter) Fields() []debezium.Field {
	fields := make([]debezium.Field, len(p.table.Columns))
	for i, col := range p.table.Columns {
		fields[i] = ColumnToField(col)
	}
	return fields
}

// PartitionKey returns a map of primary keys and their values for a given row.
func (p postgresAdapter) PartitionKey(row map[string]interface{}) map[string]interface{} {
	result := make(map[string]interface{})
	for _, key := range p.table.PrimaryKeys {
		result[key.Name] = row[key.Name]
	}
	return result
}

func (p postgresAdapter) ConvertRowToDebezium(row map[string]interface{}) (map[string]interface{}, error) {
	result := make(map[string]interface{})
	for key, value := range row {
		col, err := p.table.GetColumnByName(key)
		if err != nil {
			return nil, fmt.Errorf("failed to get column %s by name: %w", key, err)
		}

		val, err := ConvertValueToDebezium(*col, value)
		if err != nil {
			return nil, fmt.Errorf("failed to convert value: %w", err)
		}

		result[key] = val
	}
	return result, nil
}
