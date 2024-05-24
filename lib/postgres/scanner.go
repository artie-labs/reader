package postgres

import (
	"database/sql"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/artie-labs/reader/lib/postgres/parse"
	"github.com/artie-labs/reader/lib/postgres/schema"
	"github.com/artie-labs/reader/lib/rdbms/column"
	"github.com/artie-labs/reader/lib/rdbms/primary_key"
	"github.com/artie-labs/reader/lib/rdbms/scan"
)

var supportedPrimaryKeyDataType = []schema.DataType{
	schema.Bit,
	schema.Boolean,
	schema.Int16,
	schema.Int32,
	schema.Int64,
	schema.Real,
	schema.Double,
	schema.Numeric,
	schema.VariableNumeric,
	schema.Money,
	schema.Bytea,
	schema.Text,
	schema.UserDefinedText,
	schema.Time,
	schema.Date,
	schema.Timestamp,
	schema.TimestampWithTimeZone,
	schema.Interval,
	schema.UUID,
	schema.JSON,
	// schema.TimeWithTimeZone - fails: without the original timezone offset the query doesn't match any rows
	// schema.Array - fails: this doesn't work: need to serialize to Postgres array format "{1,2,3}"
	// schema.HStore - fails: operator does not exist: hstore >= unknown (SQLSTATE 42883)
	// schema.Point - can't be used as a primary key
	// schema.Geometry - fails: parse error - invalid geometry (SQLSTATE XX000)
	// schema.Geography - fails: parse error - invalid geometry (SQLSTATE XX000)
}

func NewScanner(db *sql.DB, table Table, columns []schema.Column, cfg scan.ScannerConfig) (*scan.Scanner, error) {
	for _, key := range table.PrimaryKeys {
		column, err := column.ByName(columns, key)
		if err != nil {
			return nil, fmt.Errorf("missing column with name: %q", key)
		}
		if !slices.Contains(supportedPrimaryKeyDataType, column.Type) {
			return nil, fmt.Errorf("DataType(%d) for column %q is not supported for use as a primary key", column.Type, column.Name)
		}
	}

	primaryKeyBounds, err := table.FetchPrimaryKeysBounds(db)
	if err != nil {
		return nil, err
	}

	adapter := scanAdapter{schema: table.Schema, tableName: table.Name, columns: columns}
	return scan.NewScanner(db, primaryKeyBounds, cfg, adapter)
}

type scanAdapter struct {
	schema    string
	tableName string
	columns   []schema.Column
}

func (s scanAdapter) ParsePrimaryKeyValueForOverrides(columnName string, value string) (any, error) {
	columnIdx := slices.IndexFunc(s.columns, func(x schema.Column) bool { return x.Name == columnName })
	if columnIdx < 0 {
		return nil, fmt.Errorf("primary key column does not exist: %q", columnName)
	}
	column := s.columns[columnIdx]

	if !slices.Contains(supportedPrimaryKeyDataType, column.Type) {
		return nil, fmt.Errorf("DataType(%d) for column %q is not supported for use as a primary key", column.Type, column.Name)
	}

	switch column.Type {
	case schema.Boolean:
		boolValue, err := strconv.ParseBool(value)
		if err != nil {
			return nil, fmt.Errorf("unable to convert %q to a bool: %w", value, err)
		}
		return boolValue, nil
	case schema.Int16:
		intValue, err := strconv.ParseInt(value, 10, 16)
		if err != nil {
			return nil, fmt.Errorf("unable to convert %q to an int16: %w", value, err)
		}
		return int16(intValue), nil
	case schema.Int32:
		intValue, err := strconv.ParseInt(value, 10, 32)
		if err != nil {
			return nil, fmt.Errorf("unable to convert %q to an int32: %w", value, err)
		}
		return int32(intValue), nil
	case schema.Int64:
		intValue, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("unable to convert %q to an int64: %w", value, err)
		}
		return intValue, nil
	case schema.Real:
		floatValue, err := strconv.ParseFloat(value, 32)
		if err != nil {
			return nil, fmt.Errorf("unable to convert %q to a float32: %w", value, err)
		}
		return float32(floatValue), nil
	case schema.Double:
		floatValue, err := strconv.ParseFloat(value, 64)
		if err != nil {
			return nil, fmt.Errorf("unable to convert %q to a float64: %w", value, err)
		}
		return floatValue, nil
	case
		schema.Numeric,
		schema.VariableNumeric,
		schema.Text,
		schema.Money,
		schema.UserDefinedText,
		schema.Time,
		schema.Date,
		schema.Timestamp,
		schema.TimestampWithTimeZone,
		schema.Interval,
		schema.UUID,
		schema.JSON:
		return value, nil
	default:
		return nil, fmt.Errorf("primary key value parsing not implemented for DataType(%d)", column.Type)
	}
}

// castColumn will take a colName and return the escaped version of what we should be using to call Postgres.
func castColumn(col schema.Column) string {
	colName := pgx.Identifier{col.Name}.Sanitize()
	switch col.Type {
	case schema.TimeWithTimeZone:
		// If we don't convert `time with time zone` to UTC we end up with strings like `10:23:54-02`
		// And pgtype.Time doesn't parse the offset propertly.
		// See https://github.com/jackc/pgx/issues/1940
		return fmt.Sprintf(`%s AT TIME ZONE 'UTC' AS %q`, colName, col.Name)
	case schema.Array:
		return fmt.Sprintf(`ARRAY_TO_JSON(%s)::TEXT as %q`, colName, col.Name)
	default:
		return colName
	}
}

func queryPlaceholders(offset, count int) []string {
	result := make([]string, count)
	for i := range count {
		result[i] = fmt.Sprintf("$%d", 1+offset+i)
	}
	return result
}

func (s scanAdapter) BuildQuery(primaryKeys []primary_key.Key, isFirstBatch bool, batchSize uint) (string, []any, error) {
	castedColumns := make([]string, len(s.columns))
	for i, col := range s.columns {
		castedColumns[i] = castColumn(col)
	}

	startingValues := make([]any, len(primaryKeys))
	endingValues := make([]any, len(primaryKeys))
	for i, pk := range primaryKeys {
		startingValues[i] = pk.StartingValue
		endingValues[i] = pk.EndingValue
	}

	quotedKeyNames := make([]string, len(primaryKeys))
	for i, key := range primaryKeys {
		quotedKeyNames[i] = pgx.Identifier{key.Name}.Sanitize()
	}

	lowerBoundComparison := ">"
	if isFirstBatch {
		lowerBoundComparison = ">="
	}

	return fmt.Sprintf(`SELECT %s FROM %s WHERE row(%s) %s row(%s) AND row(%s) <= row(%s) ORDER BY %s LIMIT %d`,
		// SELECT
		strings.Join(castedColumns, ","),
		// FROM
		pgx.Identifier{s.schema, s.tableName}.Sanitize(),
		// WHERE row(pk) > row($1)
		strings.Join(quotedKeyNames, ","), lowerBoundComparison, strings.Join(queryPlaceholders(0, len(startingValues)), ","),
		// AND row(pk) <= row($2)
		strings.Join(quotedKeyNames, ","), strings.Join(queryPlaceholders(len(startingValues), len(endingValues)), ","),
		// ORDER BY
		strings.Join(quotedKeyNames, ","),
		// LIMIT
		batchSize,
	), slices.Concat(startingValues, endingValues), nil
}

func (s scanAdapter) ParseRow(values []any) error {
	for i, value := range values {
		var err error
		if values[i], err = parse.ParseValue(s.columns[i].Type, value); err != nil {
			return fmt.Errorf("failed to parse column: %q: %w", s.columns[i].Name, err)
		}
	}
	return nil
}
