package debezium

import (
	"fmt"
	"time"

	"github.com/artie-labs/transfer/lib/cdc"
	"github.com/artie-labs/transfer/lib/cdc/util"
	"github.com/artie-labs/transfer/lib/debezium"

	"github.com/artie-labs/reader/lib"
)

type Row = map[string]any

type RowsIterator interface {
	HasNext() bool
	Next() ([]Row, error)
}

type Adapter interface {
	TableName() string
	TopicSuffix() string
	PartitionKey(row Row) map[string]any
	Fields() []debezium.Field
	NewIterator() (RowsIterator, error)
	ConvertRowToDebezium(row Row) (Row, error)
}

type DebeziumTransformer struct {
	adapter Adapter
	schema  debezium.Schema
	iter    RowsIterator
}

func NewDebeziumTransformer(adapter Adapter) (*DebeziumTransformer, error) {
	iter, err := adapter.NewIterator()
	if err != nil {
		return nil, fmt.Errorf("failed to create iterator :%w", err)
	}
	return NewDebeziumTransformerWithIterator(adapter, iter), nil
}

// NewDebeziumTransformerWithIterator is used for tests.
func NewDebeziumTransformerWithIterator(adapter Adapter, iter RowsIterator) *DebeziumTransformer {
	schema := debezium.Schema{
		FieldsObject: []debezium.FieldsObject{{
			Fields:     adapter.Fields(),
			Optional:   false,
			FieldLabel: cdc.After,
		}},
	}

	return &DebeziumTransformer{
		adapter: adapter,
		schema:  schema,
		iter:    iter,
	}
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
		return nil, fmt.Errorf("failed to scan: %w", err)
	}

	var result []lib.RawMessage
	for _, row := range rows {
		payload, err := d.createPayload(row)
		if err != nil {
			return nil, fmt.Errorf("failed to create Debezium payload: %w", err)
		}

		result = append(result, lib.NewRawMessage(d.adapter.TopicSuffix(), d.adapter.PartitionKey(row), payload))
	}
	return result, nil
}

func (d *DebeziumTransformer) createPayload(row Row) (util.SchemaEventPayload, error) {
	dbzRow, err := d.adapter.ConvertRowToDebezium(row)
	if err != nil {
		return util.SchemaEventPayload{}, fmt.Errorf("failed to convert row to Debezium: %w", err)
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
		Schema:  d.schema,
		Payload: payload,
	}, nil
}
