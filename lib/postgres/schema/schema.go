package schema

import (
	"strings"

	"github.com/artie-labs/transfer/lib/ptr"
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
	Geography
)

type Opts struct {
	Scale     *string
	Precision *string
}

type DescribeTableArgs struct {
	Name   string
	Schema string
}

const describeTableQuery = `
SELECT column_name, data_type, numeric_precision, numeric_scale, udt_name
FROM information_schema.columns
WHERE table_name = $1 AND table_schema = $2`

func DescribeTableQuery(args DescribeTableArgs) (string, []any) {
	return strings.TrimSpace(describeTableQuery), []any{args.Name, args.Schema}
}

func ColKindToDataType(colKind string, precision, scale, udtName *string) (DataType, *Opts) {
	colKind = strings.ToLower(colKind)
	switch colKind {
	case "point":
		return Point, nil
	case "real", "double precision":
		return Float, nil
	case "smallint":
		return Int16, nil
	case "integer":
		return Int32, nil
	case "bigint", "oid":
		return Int64, nil
	case "array":
		return Array, nil
	case "bit":
		return Bit, nil
	case "boolean":
		return Boolean, nil
	case "date":
		return Date, nil
	case "uuid":
		return UUID, nil
	case "user-defined":
		if udtName != nil && *udtName == "hstore" {
			return HStore, nil
		} else if udtName != nil && *udtName == "geometry" {
			return Geometry, nil
		} else if udtName != nil && *udtName == "geography" {
			return Geography, nil
		} else {
			return UserDefinedText, nil
		}
	case "interval":
		return Interval, nil
	case "time with time zone", "time without time zone":
		return Time, nil
	case "money":
		return Money, &Opts{
			Scale: ptr.ToString("2"),
		}
	case "character varying", "text":
		return Text, nil
	case "character":
		return TextThatRequiresEscaping, nil
	case "json", "jsonb":
		return JSON, nil
	case "timestamp without time zone", "timestamp with time zone":
		return Timestamp, nil
	default:
		if strings.Contains(colKind, "numeric") {
			if precision == nil && scale == nil {
				return VariableNumeric, nil
			} else {
				return Numeric, &Opts{
					Scale:     scale,
					Precision: precision,
				}
			}
		}

		for _, textBasedCol := range TextBasedColumns {
			// char (m) or character
			if strings.Contains(colKind, textBasedCol) {
				return TextThatRequiresEscaping, nil
			}
		}
	}

	return InvalidDataType, nil
}
