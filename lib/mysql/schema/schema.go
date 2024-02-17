package schema

import (
	"database/sql"
	"fmt"
	"log/slog"
	"strconv"
	"strings"

	"github.com/artie-labs/transfer/lib/ptr"
)

type DataType int

const (
	InvalidDataType DataType = iota
	// Integer Types (Exact Value)
	TinyInt
	SmallInt
	MediumInt
	Int
	BigInt
	// Fixed-Point Types (Exact Value)
	Decimal
	Numeric
	// Floating-Point Types (Approximate Value)
	Float
	Double
	// Bit-Value Type
	Bit
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
	Enum
	Set
)

func QuoteIdentifier(s string) string {
	return fmt.Sprintf("`%s`", strings.ReplaceAll(s, "`", "``"))
}

type Opts struct {
	Scale     *int
	Precision *int
	Size      *int
}

type Column struct {
	Name string
	Type DataType
	Opts *Opts
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
		if dataType == InvalidDataType {
			return nil, fmt.Errorf("unable to identify type for column %s: %s", colName, colType)
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
			return InvalidDataType, nil, fmt.Errorf("invalid data type: %s", s)
		}
		metadata = s[parenIndex+1 : len(s)-1]
		s = s[:parenIndex]
	}

	switch s {
	case "tinyint":
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
			return InvalidDataType, nil, fmt.Errorf("invalid decimal metadata: %s", metadata)
		}

		precision, err := strconv.Atoi(parts[0])
		if err != nil {
			return InvalidDataType, nil, fmt.Errorf("failed to parse %s precision: %w", s, err)
		}

		scale, err := strconv.Atoi(parts[1])
		if err != nil {
			return InvalidDataType, nil, fmt.Errorf("failed to parse %s scale: %w", s, err)
		}

		if s == "decimal" {
			return Decimal, &Opts{Precision: ptr.ToInt(precision), Scale: ptr.ToInt(scale)}, nil
		} else {
			return Numeric, &Opts{Precision: ptr.ToInt(precision), Scale: ptr.ToInt(scale)}, nil
		}
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
			return InvalidDataType, nil, fmt.Errorf("failed to parse varchar size: %w", err)
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
	case "enum":
		return Enum, nil, nil
	case "set":
		return Set, nil, nil
	default:
		return InvalidDataType, nil, nil
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

func selectTableQuery(keys []Column, tableName string, descending bool) string {
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
	// The LIMIT ? at the end is a hack to force the MySQL driver to used a prepared statement.
	// This is necessary because otherwise the values returned will be []uint8.
	// See https://github.com/go-sql-driver/mysql/issues/861
	return fmt.Sprintf(`SELECT %s FROM %s ORDER BY %s LIMIT ?`, strings.Join(quotedColumns, ","),
		QuoteIdentifier(tableName), strings.Join(orderByFragments, ","))
}

func getTableRow(db *sql.DB, table string, primaryKeys []Column, descending bool) ([]interface{}, error) {
	result := make([]interface{}, len(primaryKeys))
	resultPtrs := make([]interface{}, len(primaryKeys))
	for i := range result {
		resultPtrs[i] = &result[i]
	}

	query := selectTableQuery(primaryKeys, table, descending)
	slog.Info("Running query", slog.String("query", query))

	if err := db.QueryRow(query, 1).Scan(resultPtrs...); err != nil {
		return nil, err
	}
	return result, nil
}

func getPrimaryKeysLowerBounds(db *sql.DB, table string, primaryKeys []Column) ([]interface{}, error) {
	return getTableRow(db, table, primaryKeys, false)
}

func getPrimaryKeysUpperBounds(db *sql.DB, table string, primaryKeys []Column) ([]interface{}, error) {
	return getTableRow(db, table, primaryKeys, true)
}

type Bounds struct {
	Min interface{}
	Max interface{}
}

func GetPrimaryKeysBounds(db *sql.DB, table string, primaryKeys []Column) ([]Bounds, error) {
	minValues, err := getPrimaryKeysLowerBounds(db, table, primaryKeys)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve lower bounds for primary keys: %w", err)
	}

	maxValues, err := getPrimaryKeysUpperBounds(db, table, primaryKeys)
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
