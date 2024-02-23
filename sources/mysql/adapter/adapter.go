package adapter

import (
	"fmt"
	"strings"

	"github.com/artie-labs/transfer/lib/debezium"

	"github.com/artie-labs/reader/lib/mysql"
)

type mysqlAdapter struct {
	table mysql.Table
}

func NewMySQLAdapter(table mysql.Table) mysqlAdapter {
	return mysqlAdapter{table: table}
}

func (m mysqlAdapter) TableName() string {
	return m.table.Name
}

func (m mysqlAdapter) TopicSuffix() string {
	return strings.ReplaceAll(m.table.Name, `"`, ``)
}

func (m mysqlAdapter) Fields() []debezium.Field {
	panic("not implemented")
}

// PartitionKey returns a map of primary keys and their values for a given row.
func (m mysqlAdapter) PartitionKey(row map[string]any) map[string]any {
	result := make(map[string]any)
	for _, key := range m.table.PrimaryKeys {
		result[key.Name] = row[key.Name]
	}
	return result
}

func (m mysqlAdapter) ConvertRowToDebezium(row map[string]any) (map[string]any, error) {
	result := make(map[string]any)
	for key, value := range row {
		col, err := m.table.GetColumnByName(key)
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
