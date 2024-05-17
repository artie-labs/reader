package scanner

import (
	"database/sql"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/artie-labs/reader/lib/mysql"
	"github.com/artie-labs/reader/lib/mysql/schema"
	"github.com/artie-labs/reader/lib/rdbms"
	"github.com/artie-labs/reader/lib/rdbms/primary_key"
	"github.com/artie-labs/reader/lib/rdbms/scan"
)

func NewScanner(db *sql.DB, table mysql.Table, columns []schema.Column, cfg scan.ScannerConfig) (*scan.Scanner, error) {
	primaryKeyBounds, err := table.GetPrimaryKeysBounds(db)
	if err != nil {
		return nil, err
	}

	adapter := scanAdapter{tableName: table.Name, columns: columns}
	return scan.NewScanner(db, primaryKeyBounds, cfg, adapter)
}

type scanAdapter struct {
	tableName string
	columns   []schema.Column
}

func (s scanAdapter) ParsePrimaryKeyValueForOverrides(columnName string, value string) (any, error) {
	columnIdx := slices.IndexFunc(s.columns, func(x schema.Column) bool { return x.Name == columnName })
	if columnIdx < 0 {
		return nil, fmt.Errorf("primary key column %q does not exist", columnName)
	}
	column := s.columns[columnIdx]
	switch column.Type {
	case schema.TinyInt:
		intValue, err := strconv.ParseInt(value, 10, 8)
		if err != nil {
			return nil, fmt.Errorf("unable to convert %q to a tinyint: %w", value, err)
		}
		return int8(intValue), nil
	case schema.SmallInt:
		intValue, err := strconv.ParseInt(value, 10, 16)
		if err != nil {
			return nil, fmt.Errorf("unable to convert %q to a smallint: %w", value, err)
		}
		return int16(intValue), nil
	case schema.MediumInt:
		intValue, err := strconv.ParseInt(value, 10, 24)
		if err != nil {
			return nil, fmt.Errorf("unable to convert %q to a mediumint: %w", value, err)
		}
		return int32(intValue), nil
	case schema.Int:
		intValue, err := strconv.ParseInt(value, 10, 32)
		if err != nil {
			return nil, fmt.Errorf("unable to convert %q to an int: %w", value, err)
		}
		return int32(intValue), nil
	case schema.BigInt:
		intValue, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("unable to convert %q to a bigint: %w", value, err)
		}
		return intValue, nil
	case schema.Bit, schema.Boolean:
		boolValue, err := strconv.ParseBool(value)
		if err != nil {
			return nil, fmt.Errorf("unable to convert %q to a bool: %w", value, err)
		}
		return boolValue, nil
	case schema.Float:
		floatValue, err := strconv.ParseFloat(value, 32)
		if err != nil {
			return nil, fmt.Errorf("unable to convert %q to a float: %w", value, err)
		}
		return float32(floatValue), nil
	case schema.Double:
		floatValue, err := strconv.ParseFloat(value, 64)
		if err != nil {
			return nil, fmt.Errorf("unable to convert %q to a double: %w", value, err)
		}
		return floatValue, nil
	case schema.DateTime, schema.Timestamp:
		timeValue, err := time.Parse(schema.DateTimeFormat, value)
		if err != nil {
			return nil, err
		}
		return timeValue, nil
	case schema.Year:
		intValue, err := strconv.ParseInt(value, 10, 32)
		if err != nil {
			return nil, fmt.Errorf("unable to convert %q to a year: %w", value, err)
		}
		// MySQL only supports years from 1901 to 2155
		// https://dev.mysql.com/doc/refman/8.3/en/year.html
		if intValue < 1901 {
			return nil, fmt.Errorf("unable to convert %q to a year: value must be >= 1901", value)
		} else if intValue > 2155 {
			return nil, fmt.Errorf("unable to convert %q to a year: value must be <= 2155", value)
		}
		return int16(intValue), nil
	case
		schema.Decimal,
		schema.Time,
		schema.Date,
		schema.Char,
		schema.Varchar,
		schema.Text,
		schema.TinyText,
		schema.MediumText,
		schema.LongText,
		schema.Enum,
		schema.Set,
		schema.JSON:
		return value, nil
	case schema.Binary, schema.Varbinary, schema.Blob:
		return nil, fmt.Errorf("primary key value parsing not implemented for binary types")
	default:
		return nil, fmt.Errorf("primary key value parsing not implemented for DataType(%d)", column.Type)
	}
}

func (s scanAdapter) BuildQuery(primaryKeys []primary_key.Key, isFirstBatch bool, batchSize uint) (string, []any) {
	colNames := make([]string, len(s.columns))
	for idx, col := range s.columns {
		colNames[idx] = schema.QuoteIdentifier(col.Name)
	}

	var startingValues = make([]any, len(primaryKeys))
	var endingValues = make([]any, len(startingValues))
	for i, pk := range primaryKeys {
		startingValues[i] = pk.StartingValue
		endingValues[i] = pk.EndingValue
	}

	quotedKeyNames := make([]string, len(primaryKeys))
	for i, key := range primaryKeys {
		quotedKeyNames[i] = schema.QuoteIdentifier(key.Name)
	}

	lowerBoundComparison := ">"
	if isFirstBatch {
		lowerBoundComparison = ">="
	}

	return fmt.Sprintf(`SELECT %s FROM %s WHERE (%s) %s (%s) AND (%s) <= (%s) ORDER BY %s LIMIT %d`,
		// SELECT
		strings.Join(colNames, ","),
		// FROM
		schema.QuoteIdentifier(s.tableName),
		// WHERE (pk) > (123)
		strings.Join(quotedKeyNames, ","), lowerBoundComparison, strings.Join(rdbms.QueryPlaceholders("?", len(startingValues)), ","),
		// AND NOT (pk) <= (123)
		strings.Join(quotedKeyNames, ","), strings.Join(rdbms.QueryPlaceholders("?", len(endingValues)), ","),
		// ORDER BY
		strings.Join(quotedKeyNames, ","),
		// LIMIT
		batchSize,
	), slices.Concat(startingValues, endingValues)
}

func (s scanAdapter) ParseRow(values []any) error {
	return schema.ConvertValues(values, s.columns)
}
