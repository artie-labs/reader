package schema

import (
	"database/sql"
	"fmt"
	"log/slog"
	"strings"

	"github.com/artie-labs/transfer/lib/ptr"
	"github.com/jackc/pgx/v5"
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

type Column struct {
	Name string
	Type DataType
	Opts *Opts
}

const describeTableQuery = `
SELECT column_name, data_type, numeric_precision, numeric_scale, udt_name
FROM information_schema.columns
WHERE table_schema = $1 AND table_name = $2`

func DescribeTable(db *sql.DB, _schema, table string) ([]Column, error) {
	query := strings.TrimSpace(describeTableQuery)
	rows, err := db.Query(query, _schema, table)
	if err != nil {
		return nil, fmt.Errorf("failed to run query: %s: %w", query, err)
	}
	defer rows.Close()

	var cols []Column
	for rows.Next() {
		var colName string
		var colType string
		var numericPrecision *string
		var numericScale *string
		var udtName *string
		err = rows.Scan(&colName, &colType, &numericPrecision, &numericScale, &udtName)
		if err != nil {
			return nil, err
		}

		dataType, opts := ParseColumnDataType(colType, numericPrecision, numericScale, udtName)
		if dataType == InvalidDataType {
			slog.Warn("Unable to identify column type", slog.String("colName", colName), slog.String("colType", colType))
		}

		cols = append(cols, Column{
			Name: colName,
			Type: dataType,
			Opts: opts,
		})
	}
	return cols, nil
}

func ParseColumnDataType(colKind string, precision, scale, udtName *string) (DataType, *Opts) {
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

// This is a fork of: https://wiki.postgresql.org/wiki/Retrieve_primary_key_columns
const primaryKeysQuery = `
SELECT a.attname::text as id
FROM   pg_index i
JOIN   pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
WHERE  i.indrelid = $1::regclass
AND    i.indisprimary;`

func GetPrimaryKeys(db *sql.DB, schema, table string) ([]string, error) {
	query := strings.TrimSpace(primaryKeysQuery)
	rows, err := db.Query(query, pgx.Identifier{schema, table}.Sanitize())
	if err != nil {
		return nil, fmt.Errorf("failed to run query: %s: %w", query, err)
	}
	defer rows.Close()

	var primaryKeys []string
	for rows.Next() {
		var primaryKey string
		err = rows.Scan(&primaryKey)
		if err != nil {
			return nil, err
		}
		primaryKeys = append(primaryKeys, primaryKey)
	}
	return primaryKeys, nil
}
