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

var supportedPrimaryKeyDataType []schema.DataType = []schema.DataType{
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
	schema.Interval,
	schema.UUID,
	schema.Inet,
	schema.JSON,
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

func scanTableQuery(args scanTableQueryArgs) (string, []any, error) {
	castedColumns := make([]string, len(args.Columns))
	for idx, col := range args.Columns {
		var err error
		castedColumns[idx], err = castColumn(col)
		if err != nil {
			return "", nil, err
		}
	}

	startingValues := make([]any, len(args.PrimaryKeys))
	endingValues := make([]any, len(args.PrimaryKeys))
	for i, pk := range args.PrimaryKeys {
		startingValues[i] = pk.StartingValue
		endingValues[i] = pk.EndingValue
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
		// WHERE row(pk) > row($1)
		strings.Join(quotedKeyNames, ","), lowerBoundComparison, strings.Join(QueryPlaceholders(0, len(startingValues)), ","),
		// AND row(pk) <= row($2)
		strings.Join(quotedKeyNames, ","), strings.Join(QueryPlaceholders(len(startingValues), len(endingValues)), ","),
		// ORDER BY
		strings.Join(quotedKeyNames, ","),
		// LIMIT
		args.Limit,
	), slices.Concat(startingValues, endingValues), nil
}

func NewScanner(db *sql.DB, table Table, columns []schema.Column, cfg scan.ScannerConfig) (*scan.Scanner, error) {
	for _, key := range table.PrimaryKeys {
		column, err := column.GetColumnByName(columns, key)
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

	adapter := scanAdapter{schema: table.Schema, tableName: table.Name, columns: columns}
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
	return scanTableQuery(scanTableQueryArgs{
		Schema:              s.schema,
		TableName:           s.tableName,
		PrimaryKeys:         primaryKeys,
		Columns:             s.columns,
		InclusiveLowerBound: isFirstBatch,
		Limit:               batchSize,
	})
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
