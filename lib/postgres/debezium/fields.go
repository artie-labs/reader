package debezium

import (
	"github.com/artie-labs/transfer/lib/cdc"
	"github.com/artie-labs/transfer/lib/cdc/util"
	"github.com/artie-labs/transfer/lib/debezium"
	"github.com/artie-labs/transfer/lib/typing"
)

type Fields struct {
	fields              []debezium.Field
	fieldKeyToDataTypes map[string]DataType
}

func NewFields() *Fields {
	return &Fields{
		fieldKeyToDataTypes: make(map[string]DataType),
	}
}

func (f *Fields) GetDebeziumFields() []debezium.Field {
	return f.fields
}

func (f *Fields) GetField(fieldName string) (debezium.Field, bool) {
	// Let's not waste time iterating over an array if we have a faster lookup field.
	_, isOk := f.fieldKeyToDataTypes[fieldName]
	if !isOk {
		return debezium.Field{}, false
	}

	for _, field := range f.fields {
		if field.FieldName == fieldName {
			return field, true
		}
	}

	return debezium.Field{}, false
}

func (f *Fields) GetDataType(fieldName string) DataType {
	dataType, isOk := f.fieldKeyToDataTypes[fieldName]
	if !isOk {
		return InvalidDataType
	}

	return dataType
}

type Opts struct {
	Scale     *string
	Precision *string
}

func (f *Fields) AddField(colName string, dataType DataType, opts *Opts) {
	res := dataType.ToDebeziumType()
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
	f.fieldKeyToDataTypes[colName] = dataType
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
