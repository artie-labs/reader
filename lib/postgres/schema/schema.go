package schema

import (
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
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
	ElementType   *string
}

type Column = column.Column[DataType, Opts]

const describeTableQuery = `
SELECT 
    c.column_name, 
    c.data_type, 
    c.numeric_precision, 
    c.numeric_scale, 
    c.udt_name, 
    c.character_maximum_length,
    CASE 
        WHEN c.data_type = 'ARRAY' THEN t.typname
        ELSE NULL
    END AS element_type
FROM information_schema.columns c
LEFT JOIN pg_type t ON c.udt_name = t.typname
WHERE c.table_schema = $1 AND c.table_name = $2;
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
		var numericPrecision *int
		var numericScale *uint16
		var udtName *string
		var charMaxLength *int
		var elementType *string
		if err = rows.Scan(&colName, &colType, &numericPrecision, &numericScale, &udtName, &charMaxLength, &elementType); err != nil {
			return nil, err
		}

		if colType == "tsvector" {
			// We should skip tsvector data types for now because these are created to support Postgres internal full text search.
			// Debezium returns a binary blob of this as it's an unrecognized data type
			// When we fully support Postgres WAL through Reader and there's a use case, we can then revisit the decision to skip this.
			continue
		}

		dataType, opts, err := parseColumnDataType(colType, numericPrecision, numericScale, charMaxLength, udtName, elementType)
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

func parseColumnDataType(colKind string, precision *int, scale *uint16, charMaxLength *int, udtName *string, elementType *string) (DataType, *Opts, error) {
	colKind = strings.ToLower(colKind)
	switch colKind {
	case "bit":
		if charMaxLength == nil {
			return -1, nil, fmt.Errorf("invalid bit column: missing character maximum length")
		}

		return Bit, &Opts{CharMaxLength: *charMaxLength}, nil
	case "bit varying":
		opts := &Opts{}
		if charMaxLength != nil {
			opts.CharMaxLength = *charMaxLength
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
	case "character varying", "text", "character", "xml", "cidr", "inet", "macaddr", "macaddr8",
		"int4range", "int8range", "numrange", "daterange", "tsrange", "tstzrange":
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
		if elementType == nil {
			return -1, nil, fmt.Errorf("missing element type for array column")
		}

		// Element type should have _ prefix, so we need to remove it
		if !strings.HasPrefix(*elementType, "_") {
			return -1, nil, fmt.Errorf("expected element type to have _ prefix: %q", *elementType)
		}

		*elementType = (*elementType)[1:]
		return Array, &Opts{ElementType: elementType}, nil
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
					"expected precision (nil: %t) and scale (nil: %t) to both be nil or not-nil",
					precision == nil,
					scale == nil,
				)
			}
		}
	}

	return -1, nil, fmt.Errorf("unknown data type: %q", colKind)
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
