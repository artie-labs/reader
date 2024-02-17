package debezium

import (
	"github.com/artie-labs/reader/lib/postgres/schema"
	"github.com/artie-labs/transfer/lib/debezium"
)

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

func ColumnsToFields(columns []schema.Column) []debezium.Field {
	fields := make([]debezium.Field, len(columns))
	for i, col := range columns {
		fields[i] = ColumnToField(col)
	}
	return fields
}
