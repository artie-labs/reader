package transformer

import (
	"fmt"
	"github.com/artie-labs/reader/lib/debezium/converters"
	"github.com/artie-labs/transfer/lib/cdc/util"
	"github.com/artie-labs/transfer/lib/debezium"
	"time"
)

type LightDebeziumTransformer struct {
	tableName       string
	partitionKeys   []string
	valueConverters map[string]converters.ValueConverter
	// Generated
	fields []debezium.Field
}

func NewLightDebeziumTransformer(tableName string, partitionKeys []string, fieldConverters []FieldConverter) LightDebeziumTransformer {
	fields := make([]debezium.Field, len(fieldConverters))
	valueConverters := make(map[string]converters.ValueConverter)
	for i, fieldConverter := range fieldConverters {
		fields[i] = fieldConverter.ValueConverter.ToField(fieldConverter.Name)
		valueConverters[fieldConverter.Name] = fieldConverter.ValueConverter
	}

	return LightDebeziumTransformer{
		tableName:       tableName,
		partitionKeys:   partitionKeys,
		valueConverters: valueConverters,
		fields:          fields,
	}
}

func (l LightDebeziumTransformer) BuildPartitionKey(row Row) (map[string]any, error) {
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
