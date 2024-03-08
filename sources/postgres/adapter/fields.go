package adapter

import (
	"fmt"

	"github.com/artie-labs/transfer/lib/debezium"

	"github.com/artie-labs/reader/lib/postgres/schema"
)

type Result struct {
	DebeziumType string
	Type         string
}

func toDebeziumType(d schema.DataType) (Result, error) {
	switch d {
	case schema.Geography:
		return Result{
			DebeziumType: string(debezium.GeographyType),
			Type:         "struct",
		}, nil
	case schema.Geometry:
		return Result{
			DebeziumType: string(debezium.GeometryType),
			Type:         "struct",
		}, nil
	case schema.Point:
		return Result{
			DebeziumType: string(debezium.GeometryPointType),
			Type:         "struct",
		}, nil
	case schema.Boolean, schema.Bit:
		return Result{
			Type: "boolean",
		}, nil
	case schema.Text, schema.UserDefinedText, schema.Inet:
		return Result{
			Type: "string",
		}, nil
	case schema.Interval:
		return Result{
			DebeziumType: "io.debezium.time.MicroDuration",
			Type:         "int64",
		}, nil
	case schema.Array:
		return Result{
			Type: "array",
		}, nil
	case schema.Float:
		return Result{
			Type: "float",
		}, nil
	case schema.Int16:
		return Result{
			Type: "int16",
		}, nil
	case schema.Int32:
		return Result{
			Type: "int32",
		}, nil
	case schema.Int64:
		return Result{
			Type: "int64",
		}, nil
	case schema.Time:
		return Result{
			DebeziumType: string(debezium.Time),
			Type:         "int32",
		}, nil
	}

	return Result{}, fmt.Errorf("unsupported data type: DataType(%d)", d)
}

func ColumnToField(col schema.Column) (debezium.Field, error) {
	if converter := valueConverterForType(col.Type, col.Opts); converter != nil {
		return converter.ToField(col.Name), nil
	}

	res, err := toDebeziumType(col.Type)
	if err != nil {
		return debezium.Field{}, err
	}
	field := debezium.Field{
		FieldName:    col.Name,
		Type:         res.Type,
		DebeziumType: res.DebeziumType,
	}

	if col.Opts != nil {
		return debezium.Field{}, fmt.Errorf("opts should be nil")
	}
	return field, nil
}
