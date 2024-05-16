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
	Int16 DataType = iota + 1
	Int32
	Int64
	Numeric
	Float
	Real

	Bit
	Money
	Date
	String
	Bytes

	Time
	TimeMicro
	TimeNano

	Datetime2
	Datetime2Micro
	Datetime2Nano

	DatetimeOffset
	DatetimeOffsetMicro
	DatetimeOffsetNano
)

type Opts struct {
	Scale     int
	Precision int
}

type Column = column.Column[DataType, Opts]

const describeTableQuery = `
SELECT 
    COLUMN_NAME,
    DATA_TYPE,
    NUMERIC_PRECISION,
    NUMERIC_SCALE,
    DATETIME_PRECISION,
FROM 
    INFORMATION_SCHEMA.COLUMNS
WHERE 
    TABLE_SCHEMA = @p1 AND 
    TABLE_NAME = @p2;
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
		var numericScale *int
		var datetimePrecision *int
		if err = rows.Scan(&colName, &colType, &numericPrecision, &numericScale, &datetimePrecision); err != nil {
			return nil, err
		}

		dataType, opts, err := ParseColumnDataType(colType, numericPrecision, numericScale, datetimePrecision)
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

func ParseColumnDataType(colKind string, precision, scale, datetimePrecision *int) (DataType, *Opts, error) {
	colKind = strings.ToLower(colKind)
	switch colKind {
	case "bit":
		return Bit, nil, nil
	case "smallint", "tinyint":
		return Int16, nil, nil
	case "int":
		return Int32, nil, nil
	case "bigint":
		return Int64, nil, nil
	case "float", "real":
		return Float, nil, nil
	case "smallmoney", "money":
		return Money, nil, nil
	case "numeric":
		if precision == nil && scale == nil {
			return -1, nil, fmt.Errorf("expected precision and scale to be not-nil")
		}

		return Numeric, &Opts{
			Scale:     *scale,
			Precision: *precision,
		}, nil
	case "time":
		if datetimePrecision == nil {
			return -1, nil, fmt.Errorf("expected datetime precision to be not-nil")
		}

		switch *datetimePrecision {
		case 0, 1, 2, 3:
			return Time, nil, nil
		case 4, 5, 6:
			return TimeMicro, nil, nil
		case 7:
			return TimeNano, nil, nil
		default:
			return -1, nil, fmt.Errorf("invalid datetime precision: %d", *datetimePrecision)
		}
	case "date":
		return Date, nil, nil
	case "smalldatetime", "datetime":
		return Datetime2, nil, nil
	case "datetime2":
		if datetimePrecision == nil {
			return -1, nil, fmt.Errorf("expected datetime precision to be not-nil")
		}

		switch *datetimePrecision {
		case 0, 1, 2, 3:
			return Datetime2, nil, nil
		case 4, 5, 6:
			return Datetime2Micro, nil, nil
		case 7:
			return Datetime2Nano, nil, nil
		default:
			return -1, nil, fmt.Errorf("invalid datetime precision: %d", *datetimePrecision)
		}
	case "datetimeoffset":
		if datetimePrecision == nil {
			return -1, nil, fmt.Errorf("expected datetime precision to be not-nil")
		}

		switch *datetimePrecision {
		case 0, 1, 2, 3:
			return DatetimeOffset, nil, nil
		case 4, 5, 6:
			return DatetimeOffsetMicro, nil, nil
		case 7:
			return DatetimeOffsetNano, nil, nil
		default:
			return -1, nil, fmt.Errorf("invalid datetime precision: %d", *datetimePrecision)
		}
	case "char", "nchar", "varchar", "nvarchar", "text", "ntext", "xml", "uniqueidentifier":
		return String, nil, nil
	case "image", "binary", "varbinary":
		return Bytes, nil, nil
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

func getPrimaryKeyValues(db *sql.DB, schema, table string, primaryKeys []Column, descending bool) ([]any, error) {
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

type Bounds struct {
	Min any
	Max any
}

func GetPrimaryKeysBounds(db *sql.DB, schema, table string, primaryKeys []Column) ([]Bounds, error) {
	minValues, err := getPrimaryKeyValues(db, schema, table, primaryKeys, false)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve lower bounds for primary keys: %w", err)
	}

	maxValues, err := getPrimaryKeyValues(db, schema, table, primaryKeys, true)
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
