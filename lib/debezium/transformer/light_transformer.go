package transformer

import (
	"fmt"
	"time"

	"github.com/artie-labs/transfer/lib/cdc/util"
	"github.com/artie-labs/transfer/lib/debezium"

	"github.com/artie-labs/reader/lib/debezium/converters"
)

type LightDebeziumTransformer struct {
	fields          []debezium.Field
	partitionKeys   []string
	tableName       string
	valueConverters map[string]converters.ValueConverter
}

func NewLightDebeziumTransformer(tableName string, partitionKeys []string, fieldConverters []FieldConverter) LightDebeziumTransformer {
	fields := make([]debezium.Field, len(fieldConverters))
	valueConverters := make(map[string]converters.ValueConverter)
	for i, fieldConverter := range fieldConverters {
		fields[i] = fieldConverter.ValueConverter.ToField(fieldConverter.Name)
		valueConverters[fieldConverter.Name] = fieldConverter.ValueConverter
	}

	return LightDebeziumTransformer{
		fields:          fields,
		partitionKeys:   partitionKeys,
		tableName:       tableName,
		valueConverters: valueConverters,
	}
}

func (l LightDebeziumTransformer) BuildPartitionKey(beforeRow, afterRow Row) (debezium.PrimaryKeyPayload, error) {
	if beforeRow == nil && afterRow == nil {
		return debezium.PrimaryKeyPayload{}, fmt.Errorf("both before and after rows are nil")
	}

	row := afterRow
	if len(afterRow) == 0 {
		// After row may not exist for a delete event.
		row = beforeRow
	}

	return convertPartitionKey(l.valueConverters, l.partitionKeys, row)
}

func (l LightDebeziumTransformer) BuildEventPayload(beforeRow Row, afterRow Row, op string, ts time.Time) (util.SchemaEventPayload, error) {
	schema := debezium.Schema{FieldsObject: []debezium.FieldsObject{}}
	payload := util.Payload{
		Source: util.Source{
			Table: l.tableName,
			TsMs:  ts.UnixMilli(),
		},
		Operation: op,
	}

	if beforeRow != nil {
		before, err := convertRow(l.valueConverters, beforeRow)
		if err != nil {
			return util.SchemaEventPayload{}, fmt.Errorf("failed to convert before row: %w", err)
		}

		schema.FieldsObject = append(schema.FieldsObject,
			debezium.FieldsObject{
				Fields:     l.fields,
				Optional:   false,
				FieldLabel: debezium.Before,
			},
		)

		payload.Before = before
	}

	if afterRow != nil {
		after, err := convertRow(l.valueConverters, afterRow)
		if err != nil {
			return util.SchemaEventPayload{}, fmt.Errorf("failed to convert after row: %w", err)
		}

		schema.FieldsObject = append(schema.FieldsObject,
			debezium.FieldsObject{
				Fields:     l.fields,
				Optional:   false,
				FieldLabel: debezium.After,
			},
		)

		payload.After = after
	}

	return util.SchemaEventPayload{Schema: schema, Payload: payload}, nil
}
