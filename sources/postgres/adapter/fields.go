package adapter

import (
	"github.com/artie-labs/transfer/lib/debezium"

	"github.com/artie-labs/reader/lib/postgres/schema"
)

type Result struct {
	DebeziumType string
	Type         string
}

func toDebeziumType(d schema.DataType) Result {
	switch d {
	case schema.Geography:
		return Result{
			DebeziumType: string(debezium.GeographyType),
			Type:         "struct",
		}
	case schema.Geometry:
		return Result{
			DebeziumType: string(debezium.GeometryType),
			Type:         "struct",
		}
	case schema.Point:
		return Result{
			DebeziumType: string(debezium.GeometryPointType),
			Type:         "struct",
		}
	case schema.VariableNumeric:
		return Result{
			DebeziumType: string(debezium.KafkaVariableNumericType),
			Type:         "struct",
		}
	case schema.Money, schema.Numeric:
		return Result{
			DebeziumType: string(debezium.KafkaDecimalType),
		}
	case schema.Boolean, schema.Bit:
		return Result{
			Type: "boolean",
		}
	case schema.Text, schema.UserDefinedText, schema.Inet:
		return Result{
			Type: "string",
		}
	case schema.Interval:
		return Result{
			DebeziumType: "io.debezium.time.MicroDuration",
			Type:         "int64",
		}
	case schema.Array:
		return Result{
			Type: "array",
		}
	case schema.Float:
		return Result{
			Type: "float",
		}
	case schema.Int16:
		return Result{
			Type: "int16",
		}
	case schema.Int32:
		return Result{
			Type: "int32",
		}
	case schema.Int64:
		return Result{
			Type: "int64",
		}
	case schema.UUID:
		return Result{
			DebeziumType: "io.debezium.data.Uuid",
			Type:         "string",
		}
	case schema.JSON:
		return Result{
			DebeziumType: "io.debezium.data.Json",
			Type:         "string",
		}
	case schema.Time:
		return Result{
			DebeziumType: string(debezium.Time),
			Type:         "int32",
		}
	case schema.Date:
		return Result{
			DebeziumType: string(debezium.Date),
			Type:         "int32",
		}
	case schema.HStore:
		return Result{
			DebeziumType: "",
			Type:         "map",
		}
	case schema.Timestamp:
		return Result{
			DebeziumType: string(debezium.Timestamp),
			// NOTE: We are returning string here because we want the right layout to be used by our Typing library
			Type: "string",
		}
	}

	return Result{}
}

func ColumnToField(col schema.Column) debezium.Field {
	res := toDebeziumType(col.Type)
	field := debezium.Field{
		FieldName:    col.Name,
		Type:         res.Type,
		DebeziumType: res.DebeziumType,
	}

	if col.Opts != nil {
		field.Parameters = make(map[string]any)

		if col.Opts.Scale != nil {
			field.Parameters["scale"] = *col.Opts.Scale
		}

		if col.Opts.Precision != nil {
			field.Parameters[debezium.KafkaDecimalPrecisionKey] = *col.Opts.Precision
		}
	}
	return field
}
