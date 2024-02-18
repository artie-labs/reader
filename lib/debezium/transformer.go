package debezium

import (
	"fmt"
	"time"

	"github.com/artie-labs/transfer/lib/cdc"
	"github.com/artie-labs/transfer/lib/cdc/util"
	"github.com/artie-labs/transfer/lib/debezium"

	"github.com/artie-labs/reader/lib"
)

type Adapter interface {
	TableName() string
	TopicSuffix() string
	PartitionKey(row map[string]interface{}) map[string]interface{}
	Fields() []debezium.Field
	ConvertRowToDebezium(row map[string]interface{}) (map[string]interface{}, error)
}

type DebeziumTransformer struct {
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
