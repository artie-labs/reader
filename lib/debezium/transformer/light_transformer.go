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
	fields          []debezium.Field
	valueConverters map[string]converters.ValueConverter
}

func NewLightDebeziumTransformer(tableName string, fieldConverters []FieldConverter) *LightDebeziumTransformer {
	fields := make([]debezium.Field, len(fieldConverters))
	valueConverters := make(map[string]converters.ValueConverter)
	for i, fieldConverter := range fieldConverters {
		fields[i] = fieldConverter.ValueConverter.ToField(fieldConverter.Name)
		valueConverters[fieldConverter.Name] = fieldConverter.ValueConverter
	}

	return &LightDebeziumTransformer{
		tableName:       tableName,
		fields:          fields,
		valueConverters: valueConverters,
	}
}

func (l LightDebeziumTransformer) BuildEventPayload(beforeRow Row, afterRow Row, op string, ts time.Time) (util.SchemaEventPayload, error) {
	before, err := convertRow(l.valueConverters, beforeRow)
	if err != nil {
		return util.SchemaEventPayload{}, fmt.Errorf("failed to convert before row: %w", err)
	}

	after, err := convertRow(l.valueConverters, afterRow)
	if err != nil {
		return util.SchemaEventPayload{}, fmt.Errorf("failed to convert after row: %w", err)
	}

	return util.SchemaEventPayload{
		Schema: debezium.Schema{
			FieldsObject: []debezium.FieldsObject{
				{
					Fields:     l.fields,
					Optional:   false,
					FieldLabel: debezium.Before,
				},
				{
					Fields:     l.fields,
					Optional:   false,
					FieldLabel: debezium.After,
				},
			},
		},
		Payload: util.Payload{
			Before: before,
			After:  after,
			Source: util.Source{
				Table: l.tableName,
				TsMs:  ts.UnixMilli(),
			},
			Operation: op,
		},
	}, nil
}
