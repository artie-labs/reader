package postgres

import (
	"database/sql"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"time"

	"github.com/artie-labs/transfer/lib/retry"
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
	case schema.InvalidDataType:
		return false, fmt.Errorf("invalid data type")
	case
		schema.Bit,       // Fails: operator does not exist: bit >= boolean (SQLSTATE 42883)
		schema.Time,      // Fails: invalid input syntax for type time: "45296000" (SQLSTATE 22007)
		schema.Interval,  // Fails: operator does not exist: interval >= bigint (SQLSTATE 42883)
		schema.Array,     // Fails: This doesn't work: need to serialize to Postgres array format "{1,2,3}"
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
	switch castValue := value.(type) {
	case time.Time:
		return QuoteLiteral(castValue.Format(time.RFC3339)), nil
	default:
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
}

func NewScanner(db *sql.DB, t *Table, cfg scan.ScannerConfig) (scan.Scanner[*Table], error) {
	return scan.NewScanner(db, t, cfg, _scan)
}

func _scan(s *scan.Scanner[*Table], primaryKeys []primary_key.Key, isFirstRow bool) ([]map[string]any, error) {
	query, err := scanTableQuery(scanTableQueryArgs{
		Schema:              s.Table.Schema,
		TableName:           s.Table.Name,
		PrimaryKeys:         primaryKeys,
		Columns:             s.Table.Columns,
		InclusiveLowerBound: isFirstRow,
		Limit:               s.BatchSize,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to generate query: %w", err)
	}
	slog.Info(fmt.Sprintf("Query looks like: %v", query))

	rows, err := retry.WithRetriesAndResult(s.RetryCfg, func(_ int, _ error) (*sql.Rows, error) {
		return s.DB.Query(query)
	})
	if err != nil {
		return nil, fmt.Errorf("failed to scan table: %w", err)
	}

	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	// TODO: Remove this check once we're confident columns isn't different from table.Columns
	for idx, col := range columns {
		if col != s.Table.Columns[idx].Name {
			return nil, fmt.Errorf("column mismatch: expected %v, got %v", s.Table.Columns[idx].Name, col)
		}
	}

	count := len(columns)
	values := make([]any, count)
	scanArgs := make([]any, count)
	for i := range values {
		scanArgs[i] = &values[i]
	}

	var rowsData []map[string]any
	for rows.Next() {
		err = rows.Scan(scanArgs...)
		if err != nil {
			return nil, err
		}

		row := make(map[string]any)
		for idx, v := range values {
			col := s.Table.Columns[idx]

			value, err := parse.ParseValue(col.Type, v)
			if err != nil {
				return nil, err
			}

			row[col.Name] = value
		}
		rowsData = append(rowsData, row)
	}
	return rowsData, nil
}
