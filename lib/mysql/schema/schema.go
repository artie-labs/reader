package schema

import (
	"database/sql"
	"fmt"
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
