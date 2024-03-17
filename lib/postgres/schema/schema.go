package schema

import (
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"github.com/artie-labs/reader/lib/rdbms"
	"github.com/artie-labs/reader/lib/rdbms/column"
	"github.com/jackc/pgx/v5"
)

type DataType int

const (
	Bit DataType = iota + 1
	Boolean
	Int16
	Int32
	Int64
	Real
	Double
	Numeric
	VariableNumeric
	Money
	Bytea
	Text
	UserDefinedText
	Time
	TimeWithTimeZone
	Date
	Timestamp
	Interval
	UUID
	Array
	JSON
	HStore
	// PostGIS
	Point
	Geometry
	Geography
)

type Opts struct {
	Scale     int
	Precision int
}

type Column = column.Column[DataType, Opts]

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
		var numericPrecision *int
		var numericScale *int
		var udtName *string
		err = rows.Scan(&colName, &colType, &numericPrecision, &numericScale, &udtName)
		if err != nil {
			return nil, err
		}

		dataType, opts, err := ParseColumnDataType(colType, numericPrecision, numericScale, udtName)
		if err != nil {
			return nil, fmt.Errorf("unable to identify type for column %s: %s", colName, colType)
		}

		cols = append(cols, Column{
			Name: colName,
			Type: dataType,
			Opts: opts,
		})
	}
	return cols, nil
}

func ParseColumnDataType(colKind string, precision, scale *int, udtName *string) (DataType, *Opts, error) {
	colKind = strings.ToLower(colKind)
	switch colKind {
	case "bit":
		return Bit, nil, nil
	case "boolean":
		return Boolean, nil, nil
	case "smallint":
		return Int16, nil, nil
	case "integer":
		return Int32, nil, nil
	case "bigint", "oid":
		return Int64, nil, nil
	case "real":
		return Real, nil, nil
	case "double precision":
		return Double, nil, nil
	case "money":
		return Money, nil, nil
	case "bytea":
		return Bytea, nil, nil
	case "character varying", "text", "character", "xml", "cidr", "macaddr", "macaddr8",
		"int4range", "int8range", "numrange", "daterange", "tsrange", "tstzrange":
		return Text, nil, nil
	case "time without time zone":
		return Time, nil, nil
	case "time with time zone":
		return TimeWithTimeZone, nil, nil
	case "date":
		return Date, nil, nil
	case "timestamp without time zone", "timestamp with time zone":
		return Timestamp, nil, nil
	case "interval":
		return Interval, nil, nil
	case "uuid":
		return UUID, nil, nil
	case "inet":
		return Text, nil, nil
	case "array":
		return Array, nil, nil
	case "json", "jsonb":
		return JSON, nil, nil
	case "point":
		return Point, nil, nil
	case "user-defined":
		if udtName != nil && *udtName == "hstore" {
			return HStore, nil, nil
		} else if udtName != nil && *udtName == "geometry" {
			return Geometry, nil, nil
		} else if udtName != nil && *udtName == "geography" {
			return Geography, nil, nil
		} else {
			return UserDefinedText, nil, nil
		}
	default:
		if strings.Contains(colKind, "numeric") {
			if precision == nil && scale == nil {
				return VariableNumeric, nil, nil
			} else if precision != nil && scale != nil {
				return Numeric, &Opts{
					Scale:     *scale,
					Precision: *precision,
				}, nil
			} else {
				return -1, nil, fmt.Errorf(
					"expected precision (nil: %v) and scale (nil: %v) to both be nil or not-nil",
					precision == nil,
					scale == nil,
				)
			}
		}
	}

	return -1, nil, fmt.Errorf("unknown data type: %s", colKind)
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
		if err = rows.Scan(&primaryKey); err != nil {
			return nil, err
		}
		primaryKeys = append(primaryKeys, primaryKey)
	}
	return primaryKeys, nil
}

type buildPkValuesQueryArgs struct {
	Keys       []Column
	Schema     string
	TableName  string
	Descending bool
	CastFunc   func(c Column) (string, error)
}

func buildPkValuesQuery(args buildPkValuesQueryArgs) (string, error) {
	castedColumns := make([]string, len(args.Keys))
	for i, col := range args.Keys {
		var err error
		castedColumns[i], err = args.CastFunc(col)
		if err != nil {
			return "", err
		}
	}

	var fragments []string
	for _, key := range args.Keys {
		fragment := pgx.Identifier{key.Name}.Sanitize()
		if args.Descending {
			fragment += " DESC"
		}
		fragments = append(fragments, fragment)
	}
	return fmt.Sprintf(`SELECT %s FROM %s ORDER BY %s LIMIT 1`, strings.Join(castedColumns, ","),
		pgx.Identifier{args.Schema, args.TableName}.Sanitize(), strings.Join(fragments, ",")), nil
}

func getPrimaryKeyValues(db *sql.DB, schema, table string, primaryKeys []Column, cast func(c Column) (string, error), descending bool) ([]any, error) {
	result := make([]any, len(primaryKeys))
	resultPtrs := make([]any, len(primaryKeys))
	for i := range result {
		resultPtrs[i] = &result[i]
	}

	query, err := buildPkValuesQuery(buildPkValuesQueryArgs{
		Keys:       primaryKeys,
		Schema:     schema,
		TableName:  table,
		CastFunc:   cast,
		Descending: descending,
	})
	if err != nil {
		return nil, err
	}
	if descending {
		slog.Info("Find max pk query", slog.String("query", query))
	} else {
		slog.Info("Find min pk query", slog.String("query", query))
	}

	if err := db.QueryRow(query).Scan(resultPtrs...); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, rdbms.ErrNoPkValuesForEmptyTable
		}
		return nil, err
	}
	return result, nil
}

type Bounds struct {
	Min any
	Max any
}

func GetPrimaryKeysBounds(db *sql.DB, schema, table string, primaryKeys []Column, cast func(c Column) (string, error)) ([]Bounds, error) {
	minValues, err := getPrimaryKeyValues(db, schema, table, primaryKeys, cast, false)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve lower bounds for primary keys: %w", err)
	}

	maxValues, err := getPrimaryKeyValues(db, schema, table, primaryKeys, cast, true)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve upper bounds for primary keys: %w", err)
	}

	var bounds []Bounds
	for idx, minValue := range minValues {
		bounds = append(bounds, Bounds{
			Min: minValue,
			Max: maxValues[idx],
		})
		slog.Info("Primary key bounds", slog.String("key", primaryKeys[idx].Name), slog.Any("min", minValue), slog.Any("max", maxValues[idx]))
	}
	return bounds, nil
}
