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
	"github.com/artie-labs/transfer/lib/ptr"
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
)

type Opts struct {
	Scale     *int
	Precision *int
	Size      *int
}

type Column = column.Column[DataType, Opts]

func QuoteIdentifier(s string) string {
	return fmt.Sprintf("`%s`", strings.ReplaceAll(s, "`", "``"))
}

func QuotedIdentifiers(values []string) []string {
	result := make([]string, len(values))
	for i, value := range values {
		result[i] = QuoteIdentifier(value)
	}
	return result
}

func DescribeTable(db *sql.DB, table string) ([]Column, error) {
	r, err := db.Query("DESCRIBE " + QuoteIdentifier(table))
	if err != nil {
		return nil, fmt.Errorf("failed to describe table %s: %w", table, err)
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

		dataType, opts, err := parseColumnDataType(colType)
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

func parseColumnDataType(s string) (DataType, *Opts, error) {
	var metadata string
	parenIndex := strings.Index(s, "(")
	if parenIndex != -1 {
		if s[len(s)-1] != ')' {
			return -1, nil, fmt.Errorf("malformed data type: %s", s)
		}
		metadata = s[parenIndex+1 : len(s)-1]
		s = s[:parenIndex]
	}

	switch s {
	case "tinyint":
		// Boolean, bool are aliases for tinyint(1)
		if metadata == "1" {
			return Boolean, nil, nil
		}

		return TinyInt, nil, nil
	case "smallint":
		return SmallInt, nil, nil
	case "mediumint":
		return MediumInt, nil, nil
	case "int":
		return Int, nil, nil
	case "bigint":
		return BigInt, nil, nil
	case "decimal", "numeric":
		parts := strings.Split(metadata, ",")
		if len(parts) != 2 {
			return -1, nil, fmt.Errorf("invalid decimal metadata: %s", metadata)
		}

		precision, err := strconv.Atoi(parts[0])
		if err != nil {
			return -1, nil, fmt.Errorf("failed to parse precision value %s: %w", s, err)
		}

		scale, err := strconv.Atoi(parts[1])
		if err != nil {
			return -1, nil, fmt.Errorf("failed to parse scale value %s: %w", s, err)
		}
		return Decimal, &Opts{Precision: ptr.ToInt(precision), Scale: ptr.ToInt(scale)}, nil
	case "float":
		return Float, nil, nil
	case "double":
		return Double, nil, nil
	case "bit":
		return Bit, nil, nil
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
		return Varchar, &Opts{Size: ptr.ToInt(size)}, nil
	case "binary":
		return Binary, nil, nil
	case "varbinary":
		return Varbinary, nil, nil
	case "blob":
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
	default:
		return -1, nil, fmt.Errorf("unknown data type: %s", s)
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

func GetPrimaryKeys(db *sql.DB, table string) ([]string, error) {
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

func getPrimaryKeyValues(db *sql.DB, table string, primaryKeys []Column, descending bool) ([]any, error) {
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

	if err := stmt.QueryRow().Scan(resultPtrs...); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, rdbms.ErrNoPkValuesForEmptyTable
		}
		return nil, err
	}

	if err := ConvertValues(result, primaryKeys); err != nil {
		return nil, err
	}

	return result, nil
}

type Bounds struct {
	Min any
	Max any
}

func GetPrimaryKeysBounds(db *sql.DB, table string, primaryKeys []Column) ([]Bounds, error) {
	minValues, err := getPrimaryKeyValues(db, table, primaryKeys, false)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve lower bounds for primary keys: %w", err)
	}

	maxValues, err := getPrimaryKeyValues(db, table, primaryKeys, true)
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
