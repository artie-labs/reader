package adapter

import (
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

// PartitionKey returns a map of primary keys and their values for a given row.
func (m mysqlAdapter) PartitionKey(row map[string]interface{}) map[string]interface{} {
	result := make(map[string]interface{})
	for _, key := range m.table.PrimaryKeys.Keys() {
		result[key] = row[key]
	}
	return result
}

func (m mysqlAdapter) Fields() []debezium.Field {
	panic("not implemented")
}

func (m mysqlAdapter) ConvertRowToDebezium(row map[string]interface{}) (map[string]interface{}, error) {
	panic("not implemented")
}
