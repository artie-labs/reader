package debezium

import (
	"github.com/artie-labs/reader/lib/postgres/schema"
	"github.com/artie-labs/transfer/lib/debezium"
)

type Fields struct {
	fields []debezium.Field
}

func NewFields(columns []schema.Column) *Fields {
	fields := &Fields{}
	for _, col := range columns {
		fields.AddField(col)
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

func ColumnToField(col schema.Column) debezium.Field {
	res := ToDebeziumType(col.Type)
	field := debezium.Field{
		FieldName:    col.Name,
		Type:         res.Type,
		DebeziumType: res.DebeziumType,
	}

	if col.Opts != nil {
		field.Parameters = make(map[string]interface{})

		if col.Opts.Scale != nil {
			field.Parameters["scale"] = *col.Opts.Scale
		}

		if col.Opts.Precision != nil {
			field.Parameters[debezium.KafkaDecimalPrecisionKey] = *col.Opts.Precision
		}
	}
	return field
}

func (f *Fields) AddField(col schema.Column) {
	f.fields = append(f.fields, ColumnToField(col))
}
