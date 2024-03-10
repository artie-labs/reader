package transformer

import (
	"fmt"
	"time"

	"github.com/artie-labs/transfer/lib/cdc"
	"github.com/artie-labs/transfer/lib/cdc/util"
	"github.com/artie-labs/transfer/lib/debezium"

	"github.com/artie-labs/reader/lib"
	"github.com/artie-labs/reader/lib/debezium/converters"
)

type Row = map[string]any

type RowsIterator interface {
	HasNext() bool
	Next() ([]Row, error)
}

type FieldConverter struct {
	Name           string
	ValueConverter converters.ValueConverter
}

type Adapter interface {
	TableName() string
	TopicSuffix() string
	PartitionKeys() []string
	FieldConverters() []FieldConverter
	NewIterator() (RowsIterator, error)
}

type DebeziumTransformer struct {
	adapter         Adapter
	schema          debezium.Schema
	iter            RowsIterator
	valueConverters map[string]converters.ValueConverter
}

func NewDebeziumTransformer(adapter Adapter) (*DebeziumTransformer, error) {
	iter, err := adapter.NewIterator()
	if err != nil {
		return nil, fmt.Errorf("failed to create iterator :%w", err)
	}
	return NewDebeziumTransformerWithIterator(adapter, iter), nil
}

func NewDebeziumTransformerWithIterator(adapter Adapter, iter RowsIterator) *DebeziumTransformer {
	fieldConverters := adapter.FieldConverters()
	fields := make([]debezium.Field, len(fieldConverters))
	valueConverters := map[string]converters.ValueConverter{}
	for i, fieldConverter := range fieldConverters {
		fields[i] = fieldConverter.ValueConverter.ToField(fieldConverter.Name)
		valueConverters[fieldConverter.Name] = fieldConverter.ValueConverter
	}

	schema := debezium.Schema{
		FieldsObject: []debezium.FieldsObject{{
			Fields:     fields,
			Optional:   false,
			FieldLabel: cdc.After,
		}},
	}

	return &DebeziumTransformer{
		adapter:         adapter,
		schema:          schema,
		iter:            iter,
		valueConverters: valueConverters,
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

		result = append(result, lib.NewRawMessage(d.adapter.TopicSuffix(), d.partitionKey(row), payload))
	}
	return result, nil
}

func (d *DebeziumTransformer) partitionKey(row map[string]any) map[string]any {
	result := make(map[string]any)
	for _, key := range d.adapter.PartitionKeys() {
		result[key] = row[key]
	}
	return result
}

func (d *DebeziumTransformer) createPayload(row Row) (util.SchemaEventPayload, error) {
	dbzRow, err := convertRow(d.valueConverters, row)
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

// TODO: Add tests for this function
func convertRow(valueConverters map[string]converters.ValueConverter, row map[string]any) (map[string]any, error) {
	result := make(map[string]any)
	for key, value := range row {
		valueConverter, isOk := valueConverters[key]
		if !isOk {
			return nil, fmt.Errorf("failed to get ValueConverter for key %s", key)
		}

		if value != nil {
			var err error
			value, err = valueConverter.Convert(value)
			if err != nil {
				return nil, fmt.Errorf("failed to convert value: %w", err)
			}
		}

		result[key] = value
	}
	return result, nil
}
