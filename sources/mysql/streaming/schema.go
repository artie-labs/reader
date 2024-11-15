package streaming

import "time"

type Columns struct {
	Name       string `yaml:"name"`
	ColumnType string `yaml:"columnType"`
}

type TableSchemaCache struct {
	UpdatedAt  time.Time `yaml:"updatedAt"`
	SchemaName string    `yaml:"schemaName"`
	TableName  string    `yaml:"tableName"`
	Columns    []Columns `yaml:"columns"`
}
