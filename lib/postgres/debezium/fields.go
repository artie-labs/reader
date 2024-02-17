package debezium

import (
	"github.com/artie-labs/reader/lib/postgres/schema"
	"github.com/artie-labs/transfer/lib/cdc"
	"github.com/artie-labs/transfer/lib/cdc/util"
	"github.com/artie-labs/transfer/lib/debezium"
	"github.com/artie-labs/transfer/lib/typing"
)

type Fields struct {
	fields []debezium.Field
}

func NewFields(columns []schema.Column) *Fields {
	fields := &Fields{}
	for _, col := range columns {
		fields.AddField(col.Name, col.Type, col.Opts)
	}
	return fields
}

func (f *Fields) GetDebeziumFields() []debezium.Field {
	return f.fields
}

func (f *Fields) GetField(fieldName string) (debezium.Field, bool) {
	for _, field := range f.fields {
		if field.FieldName == fieldName {
			return field, true
		}
	}
	return debezium.Field{}, false
}

func (f *Fields) AddField(colName string, dataType schema.DataType, opts *schema.Opts) {
	res := ToDebeziumType(dataType)
	field := debezium.Field{
		FieldName:    colName,
		Type:         res.Type,
		DebeziumType: res.DebeziumType,
	}

	if opts != nil {
		field.Parameters = make(map[string]interface{})

		if opts.Scale != nil {
			field.Parameters["scale"] = *opts.Scale
		}

		if opts.Precision != nil {
			field.Parameters[debezium.KafkaDecimalPrecisionKey] = *opts.Precision
		}
	}

	f.fields = append(f.fields, field)
}

func (f *Fields) GetOptionalSchema() map[string]typing.KindDetails {
	schemaEvtPayload := &util.SchemaEventPayload{
		Schema: debezium.Schema{
			FieldsObject: []debezium.FieldsObject{{
				Fields:     f.GetDebeziumFields(),
				Optional:   false,
				FieldLabel: cdc.After,
			}},
		},
	}

	return schemaEvtPayload.GetOptionalSchema()
}
