package postgres

import (
	"database/sql"
	"fmt"
	"log/slog"
	"slices"
	"strings"

	"github.com/artie-labs/transfer/lib/retry"
	"github.com/jackc/pgx/v5"

	"github.com/artie-labs/reader/lib/postgres/schema"
	"github.com/artie-labs/reader/lib/rdbms/primary_key"
	"github.com/artie-labs/reader/lib/rdbms/scan"
	"github.com/artie-labs/reader/lib/timeutil"
)

func (t *Table) NewScanner(db *sql.DB, cfg scan.ScannerConfig) (scan.Scanner[*Table], error) {
	return scan.NewScanner(db, t, cfg, _scan)
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
		castedColumns[idx] = castColumn(col)
	}

	startingValues, endingValues, err := keysToValueList(args.PrimaryKeys, args.Columns)
	if err != nil {
		return "", err
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
	case schema.Float,
		schema.Int16,
		schema.Int32,
		schema.Int64,
		schema.Bit,
		schema.Boolean,
		schema.Interval, // TODO: This may be wrong, check using a real database
		schema.Array:    // TODO: This may be wrong, check using a real database
		return false, nil
	case schema.VariableNumeric,
		schema.Money,
		schema.Numeric,
		schema.TextThatRequiresEscaping,
		schema.Text,
		schema.HStore,
		schema.UUID,
		schema.UserDefinedText,
		schema.JSON,
		schema.Timestamp,
		schema.Time,
		schema.Date,
		schema.Point,
		schema.Geometry,
		schema.Geography:
		return true, nil
	default:
		return false, fmt.Errorf("unsupported data type: %v", dataType)
	}
}

func keysToValueList(keys []primary_key.Key, columns []schema.Column) ([]string, []string, error) {
	convertToString := func(value any) string {
		// This is needed because we need to cast the time.Time object into a string for pagination.
		return fmt.Sprint(timeutil.ConvertTimeToString(value))
	}

	var startValues []string
	var endValues []string
	for _, pk := range keys {
		colIndex := slices.IndexFunc(columns, func(col schema.Column) bool { return col.Name == pk.Name })
		if colIndex == -1 {
			return nil, nil, fmt.Errorf("primary key %v not found in columns", pk.Name)
		}

		shouldQuote, err := shouldQuoteValue(columns[colIndex].Type)
		if err != nil {
			return nil, nil, err
		}

		startVal := convertToString(pk.StartingValue)
		endVal := convertToString(pk.EndingValue)

		if shouldQuote {
			startVal = QuoteLiteral(startVal)
			endVal = QuoteLiteral(endVal)
		}

		startValues = append(startValues, startVal)
		endValues = append(endValues, endVal)
	}
	return startValues, endValues, nil
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

			value, err := ParseValue(col.Type, ParseValueArgs{
				ValueWrapper: ValueWrapper{
					Value: v,
				},
			})
			if err != nil {
				return nil, err
			}

			row[col.Name] = value.Value
		}
		rowsData = append(rowsData, row)
	}
	return rowsData, nil
}
