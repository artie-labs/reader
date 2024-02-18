package transformer

import (
	"fmt"
	"strings"
	"time"

	"github.com/artie-labs/transfer/lib/cdc"
	"github.com/artie-labs/transfer/lib/cdc/util"
	"github.com/artie-labs/transfer/lib/debezium"

	"github.com/artie-labs/reader/lib"
	"github.com/artie-labs/reader/lib/mtr"
	"github.com/artie-labs/reader/lib/postgres"
)

type Adapter interface {
	TableName() string
	TopicSuffix() string
	PartitionKey(row map[string]interface{}) map[string]interface{}
	Fields() []debezium.Field
	ConvertRowToDebezium(row map[string]interface{}) (map[string]interface{}, error)
}

type postgresAdapter struct {
	table postgres.Table
}

func NewPostgresAdapter(table postgres.Table) Adapter {
	return postgresAdapter{table: table}
}

type DebeziumTransformer struct {
	statsD  mtr.Client
	adapter Adapter
	iter    batchRowIterator
}

func NewDebeziumTransformer(adapter Adapter, iter batchRowIterator) *DebeziumTransformer {
	return &DebeziumTransformer{
		adapter: adapter,
		iter:    iter,
	}
}

type batchRowIterator interface {
	HasNext() bool
	Next() ([]map[string]interface{}, error)
}

func (d *DebeziumTransformer) HasNext() bool {
	return d != nil && d.iter.HasNext()
}

func (d *DebeziumTransformer) Next() ([]lib.RawMessage, error) {
	if !d.HasNext() {
		return make([]lib.RawMessage, 0), nil
	}

	rows, err := d.iter.Next()
	if err != nil {
		return nil, fmt.Errorf("failed to scan postgres: %w", err)
	}

	var result []lib.RawMessage
	for _, row := range rows {
		payload, err := d.createPayload(row)
		if err != nil {
			return nil, fmt.Errorf("failed to create debezium payload: %w", err)
		}

		result = append(result, lib.NewRawMessage(d.adapter.TopicSuffix(), d.adapter.PartitionKey(row), payload))
	}
	return result, nil
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

func (d *DebeziumTransformer) createPayload(row map[string]interface{}) (util.SchemaEventPayload, error) {
	dbzRow, err := d.adapter.ConvertRowToDebezium(row)
	if err != nil {
		return util.SchemaEventPayload{}, fmt.Errorf("failed to convert row to debezium: %w", err)
	}

	schema := debezium.Schema{
		FieldsObject: []debezium.FieldsObject{{
			Fields:     d.adapter.Fields(),
			Optional:   false,
			FieldLabel: cdc.After,
		}},
	}

	payload := util.Payload{
		After: dbzRow,
		Source: util.Source{
			Table: d.adapter.TableName(),
			TsMs:  time.Now().UnixMilli(),
		},
		Operation: "r",
	}

	return util.SchemaEventPayload{
		Schema:  schema,
		Payload: payload,
	}, nil
}
