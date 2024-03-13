package postgres

import (
	"database/sql"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/artie-labs/reader/lib/postgres/parse"
	"github.com/artie-labs/reader/lib/postgres/schema"
	"github.com/artie-labs/reader/lib/rdbms/column"
	"github.com/artie-labs/reader/lib/rdbms/primary_key"
	"github.com/artie-labs/reader/lib/rdbms/scan"
)

var supportedPrimaryKeyDataType []schema.DataType = []schema.DataType{
	schema.Boolean,
	schema.Int16,
	schema.Int32,
	schema.Int64,
	schema.Real,
	schema.Double,
	schema.Numeric,
	schema.VariableNumeric,
	schema.Money,
	schema.Text,
	schema.UserDefinedText,
	schema.Time,
	schema.Date,
	schema.Timestamp,
	schema.Interval,
	schema.UUID,
	schema.Inet,
	schema.JSON,
	// schema.Bit - fails: operator does not exist: bit >= boolean (SQLSTATE 42883)
	// schema.Bytea - fails: invalid byte sequence for encoding
	// schema.TimeWithTimeZone - fails: without the original timezone offset the query doesn't match any rows
	// schema.Array - fails: this doesn't work: need to serialize to Postgres array format "{1,2,3}"
	// schema.HStore - fails: operator does not exist: hstore >= unknown (SQLSTATE 42883)
	// schema.Point - can't be used as a primary key
	// schema.Geometry - fails: parse error - invalid geometry (SQLSTATE XX000)
	// schema.Geography - fails: parse error - invalid geometry (SQLSTATE XX000)
}

type scanTableQueryArgs struct {
	Schema              string
	TableName           string
	PrimaryKeys         []primary_key.Key
	Columns             []schema.Column
	InclusiveLowerBound bool
	Limit               uint
}

func scanTableQuery(args scanTableQueryArgs) (string, error) {
	castedColumns := make([]string, len(args.Columns))
	for idx, col := range args.Columns {
		var err error
		castedColumns[idx], err = castColumn(col)
		if err != nil {
			return "", err
		}
	}

	startingValues := make([]string, len(args.PrimaryKeys))
	endingValues := make([]string, len(args.PrimaryKeys))
	for i, pk := range args.PrimaryKeys {
		colIndex := slices.IndexFunc(args.Columns, func(col schema.Column) bool { return col.Name == pk.Name })
		if colIndex == -1 {
			return "", fmt.Errorf("primary key %v not found in columns", pk.Name)
		}

		var err error
		if startingValues[i], err = convertToStringForQuery(pk.StartingValue); err != nil {
			return "", err
		}
		if endingValues[i], err = convertToStringForQuery(pk.EndingValue); err != nil {
			return "", err
		}
	}

	quotedKeyNames := make([]string, len(args.PrimaryKeys))
	for i, key := range args.PrimaryKeys {
		quotedKeyNames[i] = pgx.Identifier{key.Name}.Sanitize()
	}

	lowerBoundComparison := ">"
	if args.InclusiveLowerBound {
		lowerBoundComparison = ">="
	}

	return fmt.Sprintf(`SELECT %s FROM %s WHERE row(%s) %s row(%s) AND row(%s) <= row(%s) ORDER BY %s LIMIT %d`,
		// SELECT
		strings.Join(castedColumns, ","),
		// FROM
		pgx.Identifier{args.Schema, args.TableName}.Sanitize(),
		// WHERE row(pk) > row(123)
		strings.Join(quotedKeyNames, ","), lowerBoundComparison, strings.Join(startingValues, ","),
		// AND row(pk) <= row(123)
		strings.Join(quotedKeyNames, ","), strings.Join(endingValues, ","),
		// ORDER BY
		strings.Join(quotedKeyNames, ","),
		// LIMIT
		args.Limit,
	), nil
}

// convertToStringForQuery returns a string value suitable for use directly in a query.
func convertToStringForQuery(value any) (string, error) {
	// TODO: Switch to using a parameterized query
	switch castValue := value.(type) {
	case bool, int, int8, int16, int32, int64, float32, float64:
		return fmt.Sprint(value), nil
	case string:
		return QuoteLiteral(castValue), nil
	case time.Time:
		return QuoteLiteral(castValue.Format(time.RFC3339)), nil
	case pgtype.Time:
		if !castValue.Valid {
			return "null", nil
		}
		dbValue, err := castValue.Value()
		if err != nil {
			return "", err
		}
		stringValue, ok := dbValue.(string)
		if !ok {
			return "", fmt.Errorf("expected string got %T with value %v", value, value)
		}
		return QuoteLiteral(stringValue), nil
	case pgtype.Interval:
		if !castValue.Valid {
			return "null", nil
		}
		value, err := castValue.Value()
		if err != nil {
			return "", err
		}
		stringValue, ok := value.(string)
		if !ok {
			return "", fmt.Errorf("expected string got %T with value %v", value, value)
		}
		return QuoteLiteral(stringValue), nil
	default:
		return "", fmt.Errorf("unexpected type %T for primary key with value %v", value, value)
	}
}

func NewScanner(db *sql.DB, table Table, cfg scan.ScannerConfig) (*scan.Scanner, error) {
	for _, key := range table.PrimaryKeys {
		column, err := column.GetColumnByName(table.Columns, key)
		if err != nil {
			return nil, fmt.Errorf("missing column with name: %s", key)
		}
		if !slices.Contains(supportedPrimaryKeyDataType, column.Type) {
			return nil, fmt.Errorf("DataType(%d) for column '%s' is not supported for use as a primary key", column.Type, column.Name)
		}
	}

	primaryKeyBounds, err := table.GetPrimaryKeysBounds(db)
	if err != nil {
		return nil, err
	}

	adapter := scanAdapter{schema: table.Schema, tableName: table.Name, columns: table.Columns}
	return scan.NewScanner(db, primaryKeyBounds, cfg, adapter)
}

type scanAdapter struct {
	schema    string
	tableName string
	columns   []schema.Column
}

func (s scanAdapter) ParsePrimaryKeyValue(columnName string, value string) (any, error) {
	columnIdx := slices.IndexFunc(s.columns, func(x schema.Column) bool { return x.Name == columnName })
	if columnIdx < 0 {
		return nil, fmt.Errorf("primary key column does not exist: %s", columnName)
	}
	column := s.columns[columnIdx]

	if !slices.Contains(supportedPrimaryKeyDataType, column.Type) {
		return nil, fmt.Errorf("DataType(%d) for column '%s' is not supported for use as a primary key", column.Type, column.Name)
	}

	switch column.Type {
	case schema.Boolean:
		boolValue, err := strconv.ParseBool(value)
		if err != nil {
			return nil, fmt.Errorf(`unable to convert "%s" to a bool: %w`, value, err)
		}
		return boolValue, nil
	case schema.Int16:
		intValue, err := strconv.ParseInt(value, 10, 16)
		if err != nil {
			return nil, fmt.Errorf(`unable to convert "%s" to an int16: %w`, value, err)
		}
		return int16(intValue), nil
	case schema.Int32:
		intValue, err := strconv.ParseInt(value, 10, 32)
		if err != nil {
			return nil, fmt.Errorf(`unable to convert "%s" to an int32: %w`, value, err)
		}
		return int32(intValue), nil
	case schema.Int64:
		intValue, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return nil, fmt.Errorf(`unable to convert "%s" to an int64: %w`, value, err)
		}
		return intValue, nil
	case schema.Real:
		floatValue, err := strconv.ParseFloat(value, 32)
		if err != nil {
			return nil, fmt.Errorf(`unable to convert "%s" to a float32: %w`, value, err)
		}
		return float32(floatValue), nil
	case schema.Double:
		floatValue, err := strconv.ParseFloat(value, 64)
		if err != nil {
			return nil, fmt.Errorf(`unable to convert "%s" to a float64: %w`, value, err)
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
		schema.Interval,
		schema.UUID,
		schema.Inet,
		schema.JSON:
		return value, nil
	default:
		return nil, fmt.Errorf("primary key value parsing not implemented for DataType(%d)", column.Type)
	}
}

func (s scanAdapter) BuildQuery(primaryKeys []primary_key.Key, isFirstBatch bool, batchSize uint) (string, []any, error) {
	query, err := scanTableQuery(scanTableQueryArgs{
		Schema:              s.schema,
		TableName:           s.tableName,
		PrimaryKeys:         primaryKeys,
		Columns:             s.columns,
		InclusiveLowerBound: isFirstBatch,
		Limit:               batchSize,
	})
	return query, nil, err
}

func (s scanAdapter) ParseRow(values []any) error {
	for i, value := range values {
		var err error
		if values[i], err = parse.ParseValue(s.columns[i].Type, value); err != nil {
			return err
		}
	}
	return nil
}
