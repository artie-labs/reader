package postgres

import (
	"database/sql"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/artie-labs/reader/lib/postgres/parse"
	"github.com/artie-labs/reader/lib/postgres/schema"
	"github.com/artie-labs/reader/lib/rdbms/primary_key"
	"github.com/artie-labs/reader/lib/rdbms/scan"
)

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
	switch dataType {
	case
		schema.Bit,       // Fails: operator does not exist: bit >= boolean (SQLSTATE 42883)
		schema.Time,      // Fails: invalid input syntax for type time: "45296000" (SQLSTATE 22007)
		schema.Interval,  // Fails: operator does not exist: interval >= bigint (SQLSTATE 42883)
		schema.Array,     // Fails: This doesn't work: need to serialize to Postgres array format "{1,2,3}"
		schema.Bytea,     // Fails: ERROR: invalid byte sequence for encoding
		schema.HStore,    // Fails: operator does not exist: hstore >= unknown (SQLSTATE 42883)
		schema.Point,     // Can't be used as a primary key
		schema.Geometry,  // Fails: parse error - invalid geometry (SQLSTATE XX000)
		schema.Geography: // Fails: parse error - invalid geometry (SQLSTATE XX000)
		return false, fmt.Errorf("unsupported primary key type: DataType(%d)", dataType)
	case schema.Float,
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
	// TODO: Change logs to actual errors then remove legacy behavior
	switch castValue := value.(type) {
	case time.Time:
		return QuoteLiteral(castValue.Format(time.RFC3339)), nil
	case int, int8, int16, int32, int64:
		switch dataType {
		case schema.Int16, schema.Int32, schema.Int64:
			return fmt.Sprint(value), nil
		default:
			slog.Error("int8/16/32/64 value with non-int column type",
				slog.Any("value", value),
				slog.Any("dataType", dataType),
			)
		}
	case float32, float64:
		switch dataType {
		case schema.Float:
			return fmt.Sprint(value), nil
		default:
			slog.Error("float32/64 value with non-float column type",
				slog.Any("value", value),
				slog.Any("dataType", dataType),
			)
		}
	case bool:
		switch dataType {
		case schema.Bit, schema.Boolean:
			return fmt.Sprint(value), nil
		default:
			slog.Error("bool value with non-bool column type",
				slog.Bool("value", castValue),
				slog.Any("dataType", dataType),
			)
		}
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
		}
	default:
		slog.Error("unexpected value for primary key",
			slog.Any("value", value),
			slog.String("type", fmt.Sprintf("%T", value)),
			slog.Any("dataType", dataType),
		)
	}
	// legacy behavior
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

func NewScanner(db *sql.DB, table Table, cfg scan.ScannerConfig) (*scan.Scanner, error) {
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
