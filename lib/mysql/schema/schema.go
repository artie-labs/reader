package schema

import (
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"

	"github.com/artie-labs/reader/lib/rdbms"
	"github.com/artie-labs/reader/lib/rdbms/column"
	"github.com/artie-labs/reader/lib/rdbms/primary_key"
	"github.com/artie-labs/transfer/lib/typing"
)

type DataType int

const (
	// Integer Types (Exact Value)
	TinyInt DataType = iota + 1
	SmallInt
	MediumInt
	Int
	BigInt
	// Fixed-Point Types (Exact Value)
	Decimal
	// Floating-Point Types (Approximate Value)
	Float
	Double
	// Bit-Value Type
	Bit
	Boolean
	// Date and Time Data Types
	Date
	DateTime
	Timestamp
	Time
	Year
	// String Types
	Char
	Varchar
	Binary
	Varbinary
	Blob
	Text
	TinyText
	MediumText
	LongText
	Enum
	Set
	// JSON
	JSON
	// Spatial Data Types
	Point
	Geometry
)

type Opts struct {
	Scale     *uint16
	Precision *int
	Size      *int
}

type Column = column.Column[DataType, Opts]

func QuoteIdentifier(s string) string {
	return fmt.Sprintf("`%s`", strings.ReplaceAll(s, "`", "``"))
}

func GetCreateTableDDL(db *sql.DB, table string) (string, error) {
	row := db.QueryRow("SHOW CREATE TABLE " + QuoteIdentifier(table))
	var unused string
	var createTableDDL string
	if err := row.Scan(&unused, &createTableDDL); err != nil {
		return "", fmt.Errorf("failed to get create table DDL: %w", err)
	}

	return createTableDDL, nil
}

func DescribeTable(db *sql.DB, table string) ([]Column, error) {
	r, err := db.Query("DESCRIBE " + QuoteIdentifier(table))
	if err != nil {
		return nil, fmt.Errorf("failed to describe table %q: %w", table, err)
	}
	defer r.Close()

	var result []Column
	for r.Next() {
		var colName string
		var colType string
		var nullable string
		var key string
		var defaultValue sql.NullString
		var extra string
		err = r.Scan(&colName, &colType, &nullable, &key, &defaultValue, &extra)
		if err != nil {
			return nil, fmt.Errorf("failed to scan: %w", err)
		}

		dataType, opts, err := ParseColumnDataType(colType)
		if err != nil {
			return nil, fmt.Errorf("failed to parse data type: %w", err)
		}

		result = append(result, Column{
			Name: colName,
			Type: dataType,
			Opts: opts,
		})
	}
	return result, nil
}

func ParseColumnDataType(originalS string) (DataType, *Opts, error) {
	// Preserve the original value, so we can return the error message without the actual value being mutated.
	s := originalS
	var metadata string
	var unsigned bool
	if strings.HasSuffix(s, " unsigned") {
		// If a number is unsigned, we'll bump them up by one (e.g. int32 -> int64)
		unsigned = true
		s = strings.TrimSuffix(s, " unsigned")
	}

	parenIndex := strings.Index(s, "(")
	if parenIndex != -1 {
		if s[len(s)-1] != ')' {
			// Make sure the format looks like int (n) unsigned
			return -1, nil, fmt.Errorf("malformed data type: %q", originalS)
		}
		metadata = s[parenIndex+1 : len(s)-1]
		s = s[:parenIndex]
	}

	switch s {
	case "tinyint":
		if unsigned {
			return SmallInt, nil, nil
		}

		return TinyInt, nil, nil
	case "smallint":
		if unsigned {
			return Int, nil, nil
		}

		return SmallInt, nil, nil
	case "mediumint":
		if unsigned {
			return Int, nil, nil
		}

		return MediumInt, nil, nil
	case "int":
		if unsigned {
			return BigInt, nil, nil
		}

		return Int, nil, nil
	case "bigint":
		return BigInt, nil, nil
	case "decimal", "numeric":
		parts := strings.Split(metadata, ",")
		if len(parts) != 2 {
			return -1, nil, fmt.Errorf("invalid decimal metadata: %q", metadata)
		}

		precision, err := strconv.Atoi(parts[0])
		if err != nil {
			return -1, nil, fmt.Errorf("failed to parse precision value %q: %w", s, err)
		}

		scale, err := strconv.ParseUint(parts[1], 10, 16)
		if err != nil {
			return -1, nil, fmt.Errorf("failed to parse scale value %q: %w", s, err)
		}
		return Decimal, &Opts{Precision: typing.ToPtr(precision), Scale: typing.ToPtr(uint16(scale))}, nil
	case "float":
		return Float, nil, nil
	case "double":
		return Double, nil, nil
	case "bit":
		size, err := strconv.Atoi(metadata)
		if err != nil {
			return -1, nil, fmt.Errorf("failed to parse metadata value %q: %w", s, err)
		}

		return Bit, &Opts{Size: typing.ToPtr(size)}, nil
	case "date":
		return Date, nil, nil
	case "datetime":
		return DateTime, nil, nil
	case "timestamp":
		return Timestamp, nil, nil
	case "time":
		return Time, nil, nil
	case "year":
		return Year, nil, nil
	case "char":
		return Char, nil, nil
	case "varchar":
		size, err := strconv.Atoi(metadata)
		if err != nil {
			return -1, nil, fmt.Errorf("failed to parse varchar size: %w", err)
		}
		return Varchar, &Opts{Size: typing.ToPtr(size)}, nil
	case "binary":
		return Binary, nil, nil
	case "varbinary":
		return Varbinary, nil, nil
	case "blob", "tinyblob", "mediumblob", "longblob":
		return Blob, nil, nil
	case "text":
		return Text, nil, nil
	case "tinytext":
		return TinyText, nil, nil
	case "mediumtext":
		return MediumText, nil, nil
	case "longtext":
		return LongText, nil, nil
	case "enum":
		return Enum, nil, nil
	case "set":
		return Set, nil, nil
	case "json":
		return JSON, nil, nil
	case "point":
		return Point, nil, nil
	case
		"geomcollection",
		"geometry",
		"linestring",
		"multilinestring",
		"multipoint",
		"multipolygon",
		"polygon":
		return Geometry, nil, nil
	default:
		return -1, nil, fmt.Errorf("unknown data type: %q", originalS)
	}
}

const primaryKeysQuery = `
SELECT key_column_usage.column_name
FROM information_schema.table_constraints
JOIN information_schema.key_column_usage
USING (constraint_name, table_schema, table_name)
WHERE table_constraints.constraint_type='PRIMARY KEY'
  AND table_constraints.table_schema=DATABASE()
  AND table_constraints.table_name=?
`

func FetchPrimaryKeys(db *sql.DB, table string) ([]string, error) {
	query := strings.TrimSpace(primaryKeysQuery)
	rows, err := db.Query(query, table)
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

func buildPkValuesQuery(keys []Column, tableName string, descending bool) string {
	quotedColumns := make([]string, len(keys))
	for i, col := range keys {
		quotedColumns[i] = QuoteIdentifier(col.Name)
	}

	var orderByFragments []string
	for _, key := range keys {
		fragment := QuoteIdentifier(key.Name)
		if descending {
			fragment += " DESC"
		}
		orderByFragments = append(orderByFragments, fragment)
	}
	return fmt.Sprintf(`SELECT %s FROM %s ORDER BY %s LIMIT 1`,
		// SELECT
		strings.Join(quotedColumns, ","),
		// FROM
		QuoteIdentifier(tableName),
		// ORDER BY
		strings.Join(orderByFragments, ","),
		// LIMIT 1
	)
}

func fetchPrimaryKeyValues(db *sql.DB, table string, primaryKeys []Column, descending bool) ([]any, error) {
	result := make([]any, len(primaryKeys))
	resultPtrs := make([]any, len(primaryKeys))
	for i := range result {
		resultPtrs[i] = &result[i]
	}

	query := buildPkValuesQuery(primaryKeys, table, descending)
	if descending {
		slog.Info("Find max pk query", slog.String("query", query))
	} else {
		slog.Info("Find min pk query", slog.String("query", query))
	}

	// We're using a prepared statement to force the driver to return native types.
	// This is necessary because otherwise the values returned will be []uint8.
	// See https://github.com/go-sql-driver/mysql/issues/861
	stmt, err := db.Prepare(query)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()

	if err = stmt.QueryRow().Scan(resultPtrs...); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, rdbms.ErrNoPkValuesForEmptyTable
		}
		return nil, err
	}

	if err = ConvertValues(result, primaryKeys); err != nil {
		return nil, err
	}

	return result, nil
}

func FetchPrimaryKeysBounds(db *sql.DB, table string, primaryKeys []Column) ([]primary_key.Bounds, error) {
	minValues, err := fetchPrimaryKeyValues(db, table, primaryKeys, false)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve lower bounds for primary keys: %w", err)
	}

	maxValues, err := fetchPrimaryKeyValues(db, table, primaryKeys, true)
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
