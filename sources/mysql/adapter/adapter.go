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

func (d mysqlAdapter) TopicSuffix() string {
	return strings.ReplaceAll(d.table.Name, `"`, ``)
}

// PartitionKey returns a map of primary keys and their values for a given row.
func (d mysqlAdapter) PartitionKey(row map[string]interface{}) map[string]interface{} {
	result := make(map[string]interface{})
	for _, key := range d.table.PrimaryKeys.Keys() {
		result[key] = row[key]
	}
	return result
}

func (d mysqlAdapter) ConvertRowToDebezium(row map[string]interface{}) (map[string]interface{}, error) {
	panic("not implemented")
}

func (p mysqlAdapter) Fields() []debezium.Field {
	panic("not implemented")
}

func (p mysqlAdapter) TableName() string {
	return p.table.Name
}
