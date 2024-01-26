package debezium

import (
	"github.com/artie-labs/transfer/lib/debezium"
)

type DataType int

var (
	TextBasedColumns = []string{
		"xml",
		"cidr",
		"macaddr",
		"macaddr8",
		"inet",
		"int4range",
		"int8range",
		"numrange",
		"daterange",
		"tsrange",
		"tstzrange",
	}
)

const (
	InvalidDataType DataType = iota
	VariableNumeric
	Money
	Numeric
	Bit
	Boolean
	TextThatRequiresEscaping
	Text
	Interval
	Array
	HStore
	Float
	Int16
	Int32
	Int64
	UUID
	UserDefinedText
	JSON
	Timestamp
	Time
	Date
	// PostGIS
	Point
	Geometry
)

type Result struct {
	DebeziumType string
	Type         string
}

func (d DataType) ToDebeziumType() Result {
	switch d {
	case Geometry:
		return Result{
			DebeziumType: string(debezium.GeometryType),
			Type:         "struct",
		}
	case Point:
		return Result{
			DebeziumType: string(debezium.GeometryPointType),
			Type:         "struct",
		}
	case VariableNumeric:
		return Result{
			DebeziumType: string(debezium.KafkaVariableNumericType),
			Type:         "struct",
		}
	case Money, Numeric:
		return Result{
			DebeziumType: string(debezium.KafkaDecimalType),
		}
	case Boolean, Bit:
		return Result{
			Type: "boolean",
		}
	case Text, UserDefinedText, TextThatRequiresEscaping:
		return Result{
			Type: "string",
		}
	case Interval:
		return Result{
			DebeziumType: "io.debezium.time.MicroDuration",
			Type:         "int64",
		}
	case Array:
		return Result{
			Type: "array",
		}
	case Float:
		return Result{
			Type: "float",
		}
	case Int16:
		return Result{
			Type: "int16",
		}
	case Int32:
		return Result{
			Type: "int32",
		}
	case Int64:
		return Result{
			Type: "int64",
		}
	case UUID:
		return Result{
			DebeziumType: "io.debezium.data.Uuid",
			Type:         "string",
		}
	case JSON:
		return Result{
			DebeziumType: "io.debezium.data.Json",
			Type:         "string",
		}
	case Time:
		return Result{
			DebeziumType: string(debezium.Time),
			Type:         "int32",
		}
	case Date:
		return Result{
			DebeziumType: string(debezium.Date),
			Type:         "int32",
		}
	case HStore:
		return Result{
			DebeziumType: "",
			Type:         "map",
		}
	case Timestamp:
		return Result{
			DebeziumType: string(debezium.Timestamp),
			// NOTE: We are returning string here because we want the right layout to be used by our Typing library
			Type: "string",
		}
	}

	return Result{}
}
