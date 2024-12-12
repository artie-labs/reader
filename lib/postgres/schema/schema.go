package schema

import (
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/artie-labs/reader/lib/rdbms"
	"github.com/artie-labs/reader/lib/rdbms/column"
	"github.com/artie-labs/reader/lib/rdbms/primary_key"
)

type DataType int

const (
	Bit DataType = iota + 1
	BitVarying
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
	TimestampWithTimeZone
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
	Scale         uint16
	Precision     int
	CharMaxLength int
}

type Column = column.Column[DataType, Opts]

const describeTableQuery = `
SELECT 
    a.attname AS column_name,
    pg_catalog.format_type(a.atttypid, a.atttypmod) AS data_type,
    t.typname AS udt_name
FROM 
    pg_catalog.pg_attribute a
JOIN 
    pg_catalog.pg_class cl ON a.attrelid = cl.oid
JOIN 
    pg_catalog.pg_namespace n ON cl.relnamespace = n.oid
JOIN 
    information_schema.columns c 
    ON c.column_name = a.attname
    AND c.table_name = cl.relname
    AND c.table_schema = n.nspname
JOIN 
    pg_catalog.pg_type t ON a.atttypid = t.oid
WHERE 
    c.table_schema = $1
    AND c.table_name = $2
    AND a.attnum > 0
    AND NOT a.attisdropped
ORDER BY 
    a.attnum;
`

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
		var udtName string
		if err = rows.Scan(&colName, &colType, &udtName); err != nil {
			return nil, err
		}

		if colType == "tsvector" {
			// We should skip tsvector data types for now because these are created to support Postgres internal full text search.
			// Debezium returns a binary blob of this as it's an unrecognized data type
			// When we fully support Postgres WAL through Reader and there's a use case, we can then revisit the decision to skip this.
			continue
		}

		dataType, opts, err := parseColumnDataType(colType, udtName)
		if err != nil {
			return nil, fmt.Errorf("unable to identify type %q for column %q", colType, colName)
		}

		cols = append(cols, Column{
			Name: colName,
			Type: dataType,
			Opts: opts,
		})
	}
	return cols, nil
}

func parseColumnDataType(originalS string, udtName string) (DataType, *Opts, error) {
	s := strings.ToLower(originalS)
	var metadata string

	parenIndex := strings.Index(s, "(")
	if parenIndex != -1 {
		if s[len(s)-1] != ')' {
			// Make sure the format looks like int (n) unsigned
			return -1, nil, fmt.Errorf("malformed data type: %q", originalS)
		}
		metadata = originalS[parenIndex+1 : len(s)-1]
		s = s[:parenIndex]
	}

	switch s {
	case "bit":
		size, err := strconv.Atoi(metadata)
		if err != nil {
			return -1, nil, fmt.Errorf("failed to parse metadata value %q: %w", s, err)
		}

		return Bit, &Opts{CharMaxLength: size}, nil
	case "bit varying":
		opts := &Opts{}
		if metadata != "" {
			size, err := strconv.Atoi(metadata)
			if err != nil {
				return -1, nil, fmt.Errorf("failed to parse metadata value %q: %w", s, err)
			}

			opts.CharMaxLength = size
		}

		return BitVarying, opts, nil
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
	case
		"character varying",
		"text",
		"citext",
		"character",
		"xml",
		"cidr",
		"inet",
		"macaddr",
		"macaddr8",
		"int4range",
		"int8range",
		"numrange",
		"daterange",
		"tsrange",
		"tstzrange":
		return Text, nil, nil
	case "time without time zone":
		return Time, nil, nil
	case "time with time zone":
		return TimeWithTimeZone, nil, nil
	case "date":
		return Date, nil, nil
	case "timestamp without time zone":
		return Timestamp, nil, nil
	case "timestamp with time zone":
		return TimestampWithTimeZone, nil, nil
	case "interval":
		return Interval, nil, nil
	case "uuid":
		return UUID, nil, nil
	case "array":
		return Array, nil, nil
	case "json", "jsonb":
		return JSON, nil, nil
	case "point":
		return Point, nil, nil
	case "hstore":
		return HStore, nil, nil
	case "geometry":
		return Geometry, nil, nil
	case "geography":
		return Geography, nil, nil
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
	case "numeric":
		if metadata == "" {
			return VariableNumeric, nil, nil
		}

		parts := strings.Split(metadata, ",")
		if len(parts) != 2 {
			return -1, nil, fmt.Errorf("expected precision and scale to both be set or not set, got %q", originalS)
		}

		precision, err := strconv.Atoi(parts[0])
		if err != nil {
			return -1, nil, fmt.Errorf("failed to parse precision value %q: %w", metadata, err)
		}

		scale, err := strconv.ParseUint(parts[1], 10, 16)
		if err != nil {
			return -1, nil, fmt.Errorf("failed to parse scale value %q: %w", metadata, err)
		}

		return Numeric, &Opts{Precision: precision, Scale: uint16(scale)}, nil
	}

	return -1, nil, fmt.Errorf("unknown data type: %q", originalS)
}

// This is a fork of: https://wiki.postgresql.org/wiki/Retrieve_primary_key_columns
const primaryKeysQuery = `
SELECT a.attname::text as id
FROM   pg_index i
JOIN   pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
WHERE  i.indrelid = $1::regclass
AND    i.indisprimary;`

func FetchPrimaryKeys(db *sql.DB, schema, table string) ([]string, error) {
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
}

func buildPkValuesQuery(args buildPkValuesQueryArgs) string {
	escapedColumns := make([]string, len(args.Keys))
	for i, col := range args.Keys {
		escapedColumns[i] = pgx.Identifier{col.Name}.Sanitize()
	}

	var fragments []string
	for _, key := range args.Keys {
		fragment := pgx.Identifier{key.Name}.Sanitize()
		if args.Descending {
			fragment += " DESC"
		}
		fragments = append(fragments, fragment)
	}
	return fmt.Sprintf(`SELECT %s FROM %s ORDER BY %s LIMIT 1`, strings.Join(escapedColumns, ","),
		pgx.Identifier{args.Schema, args.TableName}.Sanitize(), strings.Join(fragments, ","))
}

func fetchPrimaryKeyValues(db *sql.DB, schema, table string, primaryKeys []Column, descending bool) ([]any, error) {
	result := make([]any, len(primaryKeys))
	resultPtrs := make([]any, len(primaryKeys))
	for i := range result {
		resultPtrs[i] = &result[i]
	}

	query := buildPkValuesQuery(buildPkValuesQueryArgs{
		Keys:       primaryKeys,
		Schema:     schema,
		TableName:  table,
		Descending: descending,
	})
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

func FetchPrimaryKeysBounds(db *sql.DB, schema, table string, primaryKeys []Column) ([]primary_key.Bounds, error) {
	minValues, err := fetchPrimaryKeyValues(db, schema, table, primaryKeys, false)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve lower bounds for primary keys: %w", err)
	}

	maxValues, err := fetchPrimaryKeyValues(db, schema, table, primaryKeys, true)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve upper bounds for primary keys: %w", err)
	}

	var bounds []primary_key.Bounds
	for idx, minValue := range minValues {
		bounds = append(bounds, primary_key.Bounds{
			Min: minValue,
			Max: maxValues[idx],
		})
		slog.Info("Primary key bounds", slog.String("key", primaryKeys[idx].Name), slog.Any("min", minValue), slog.Any("max", maxValues[idx]))
	}
	return bounds, nil
}
