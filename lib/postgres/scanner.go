package postgres

import (
	"database/sql"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/artie-labs/reader/lib/postgres/parse"
	"github.com/artie-labs/reader/lib/postgres/schema"
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
		colType := args.Columns[colIndex].Type

		var err error
		if startingValues[i], err = convertToStringForQuery(pk.StartingValue, colType); err != nil {
			return "", err
		}
		if endingValues[i], err = convertToStringForQuery(pk.EndingValue, colType); err != nil {
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

func shouldQuoteValue(dataType schema.DataType) (bool, error) {
	if !slices.Contains(supportedPrimaryKeyDataType, dataType) {
		return false, fmt.Errorf("unsupported primary key type: DataType(%d)", dataType)
	}

	switch dataType {
	case
		// Natively supported types in convertToStringForQuery
		schema.Time,
		schema.Interval:
		return false, fmt.Errorf("unexpected primary key type: DataType(%d)", dataType)
	case
		schema.Real,
		schema.Double,
		schema.Int16,
		schema.Int32,
		schema.Int64,
		schema.Boolean:
		return false, nil
	case schema.VariableNumeric,
		schema.Money,
		schema.Numeric,
		schema.Inet,
		schema.Text,
		schema.UUID,
		schema.UserDefinedText,
		schema.JSON,
		schema.Timestamp,
		schema.Date:
		return true, nil
	default:
		return false, fmt.Errorf("unsupported data type: DataType(%d)", dataType)
	}
}

// convertToStringForQuery returns a string value suitable for use directly in a query.
func convertToStringForQuery(value any, dataType schema.DataType) (string, error) {
	switch castValue := value.(type) {
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
	case bool, int, int8, int16, int32, int64, float32, float64:
		return fmt.Sprint(value), nil
	case string:
		switch dataType {
		case schema.Text, schema.UserDefinedText, schema.Inet, schema.UUID, schema.JSON, schema.VariableNumeric,
			schema.Numeric, schema.Money:
			return QuoteLiteral(castValue), nil
		default:
			slog.Error("string value with non-string column type",
				slog.String("value", castValue),
				slog.Any("dataType", dataType),
			)
			// legacy behavior - used when optionalPrimaryKeyValStart/End is configured
			// TODO: parse optionalPrimaryKeyValStart/End based on DataType to Go type
			shouldQuote, err := shouldQuoteValue(dataType)
			if err != nil {
				return "", err
			}
			if shouldQuote {
				return QuoteLiteral(fmt.Sprint(value)), nil
			} else {
				return fmt.Sprint(value), nil
			}
		}
	default:
		return "", fmt.Errorf("unexpected type %T for primary key with value %v", value, value)
	}
}

func NewScanner(db *sql.DB, table Table, cfg scan.ScannerConfig) (*scan.Scanner, error) {
	for _, key := range table.PrimaryKeys {
		column, err := table.GetColumnByName(key)
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

func (s scanAdapter) ParseRow(values []any) (map[string]any, error) {
	row := make(map[string]any)
	for idx, v := range values {
		col := s.columns[idx]

		value, err := parse.ParseValue(col.Type, v)
		if err != nil {
			return nil, err
		}

		row[col.Name] = value
	}
	return row, nil
}
