package transformer

import (
	"fmt"
	"time"

	"github.com/artie-labs/transfer/lib/cdc/util"
	"github.com/artie-labs/transfer/lib/debezium"

	"github.com/artie-labs/reader/lib"
	"github.com/artie-labs/reader/lib/debezium/converters"
	"github.com/artie-labs/reader/lib/iterator"
)

type Row = map[string]any

type RowsIterator = iterator.Iterator[[]Row]

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
			FieldLabel: debezium.After,
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

		result = append(result, lib.NewRawMessage(d.adapter.TopicSuffix(), d.partitionKey(row), &payload))
	}

	return result, nil
}

func (d *DebeziumTransformer) partitionKey(row Row) map[string]any {
	result := make(map[string]any)
	for _, key := range d.adapter.PartitionKeys() {
		result[key] = row[key]
	}
	return result
}

func (d *DebeziumTransformer) createPayload(row Row) (util.SchemaEventPayload, error) {
	dbzRow, err := convertRow(d.valueConverters, row)
	if err != nil {
		return util.SchemaEventPayload{}, err
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

func convertPartitionKey(valueConverters map[string]converters.ValueConverter, partitionKeys []string, row Row) (map[string]any, error) {
	result := make(map[string]any, len(partitionKeys))
	for _, key := range partitionKeys {
		valueConverter, isOk := valueConverters[key]
		if !isOk {
			return nil, fmt.Errorf("failed to get ValueConverter for key %q", key)
		}

		// Key must exist in row
		value, isOk := row[key]
		if !isOk {
			return nil, fmt.Errorf("failed to get partition key value for key %q", key)
		}

		convertedValue, err := valueConverter.Convert(value)
		if err != nil {
			return nil, fmt.Errorf("failed to convert partition key value for key %q: %w", key, err)
		}

		result[key] = convertedValue
	}

	return result, nil
}

func convertRow(valueConverters map[string]converters.ValueConverter, row Row) (Row, error) {
	result := make(map[string]any)
	for key, value := range row {
		valueConverter, isOk := valueConverters[key]
		if !isOk {
			return nil, fmt.Errorf("failed to get ValueConverter for key %q", key)
		}

		if value != nil {
			var err error
			value, err = valueConverter.Convert(value)
			if err != nil {
				return nil, fmt.Errorf("failed to convert row value for key %q: %w", key, err)
			}
		}

		result[key] = value
	}
	return result, nil
}
