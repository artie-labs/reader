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
)

const (
	jitterBaseMs = 300
	jitterMaxMs  = 5000
)

type scanner struct {
	// immutable
	db        *sql.DB
	table     *Table
	batchSize uint
	retryCfg  retry.RetryConfig

	// mutable
	primaryKeys *primary_key.Keys
	isFirstRow  bool
	done        bool
}

func (t *Table) NewScanner(db *sql.DB, primaryKeys *primary_key.Keys, batchSize uint, errorRetries int) (scanner, error) {
	retryCfg, err := retry.NewJitterRetryConfig(jitterBaseMs, jitterMaxMs, errorRetries, retry.AlwaysRetry)
	if err != nil {
		return scanner{}, fmt.Errorf("failed to build retry config: %w", err)
	}

	return scanner{
		db:          db,
		table:       t,
		batchSize:   batchSize,
		retryCfg:    retryCfg,
		primaryKeys: primaryKeys.Clone(),
		isFirstRow:  true,
		done:        false,
	}, nil
}

type scanTableQueryArgs struct {
	Schema              string
	TableName           string
	PrimaryKeys         *primary_key.Keys
	Columns             []schema.Column
	InclusiveLowerBound bool
	Limit               uint
}

func scanTableQuery(args scanTableQueryArgs) (string, error) {
	castedColumns := make([]string, len(args.Columns))
	for idx, col := range args.Columns {
		castedColumns[idx] = castColumn(col)
	}

	startingValues, err := keysToValueList(args.PrimaryKeys, args.Columns, false)
	if err != nil {
		return "", err
	}
	endingValues, err := keysToValueList(args.PrimaryKeys, args.Columns, true)
	if err != nil {
		return "", err
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
		strings.Join(QuotedIdentifiers(args.PrimaryKeys.Keys()), ","), lowerBoundComparison, strings.Join(startingValues, ","),
		// AND row(pk) <= row(123)
		strings.Join(QuotedIdentifiers(args.PrimaryKeys.Keys()), ","), strings.Join(endingValues, ","),
		// ORDER BY
		strings.Join(QuotedIdentifiers(args.PrimaryKeys.Keys()), ","),
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

func keysToValueList(k *primary_key.Keys, columns []schema.Column, end bool) ([]string, error) {
	var valuesToReturn []string
	for _, pk := range k.KeysList() {
		val := pk.StartingValue
		if end {
			val = pk.EndingValue
		}

		colIndex := slices.IndexFunc(columns, func(col schema.Column) bool { return col.Name == pk.Name })
		if colIndex == -1 {
			return nil, fmt.Errorf("primary key %v not found in columns", pk.Name)
		}

		shouldQuote, err := shouldQuoteValue(columns[colIndex].Type)
		if err != nil {
			return nil, err
		}

		// TODO: look into storing primary key values as their raw types instead of converting them to strings
		strVal := fmt.Sprint(val)

		if shouldQuote {
			valuesToReturn = append(valuesToReturn, QuoteLiteral(strVal))
		} else {
			valuesToReturn = append(valuesToReturn, strVal)
		}
	}
	return valuesToReturn, nil
}

func (s *scanner) scan() ([]map[string]interface{}, error) {
	query, err := scanTableQuery(scanTableQueryArgs{
		Schema:              s.table.Schema,
		TableName:           s.table.Name,
		PrimaryKeys:         s.primaryKeys,
		Columns:             s.table.Columns,
		InclusiveLowerBound: s.isFirstRow,
		Limit:               s.batchSize,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to generate query: %w", err)
	}
	slog.Info(fmt.Sprintf("Query looks like: %v", query))

	rows, err := retry.WithRetriesAndResult(s.retryCfg, func(_ int, _ error) (*sql.Rows, error) {
		return s.db.Query(query)
	})
	if err != nil {
		return nil, fmt.Errorf("failed to scan table: %w", err)
	}

	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	// TODO: Remove this check once we're confident columns isn't different from s.table.Columns
	for idx, col := range columns {
		if col != s.table.Columns[idx].Name {
			return nil, fmt.Errorf("column mismatch: expected %v, got %v", s.table.Columns[idx].Name, col)
		}
	}

	count := len(columns)
	values := make([]interface{}, count)
	scanArgs := make([]interface{}, count)
	for i := range values {
		scanArgs[i] = &values[i]
	}

	var rowsData []map[string]ValueWrapper
	for rows.Next() {
		err = rows.Scan(scanArgs...)
		if err != nil {
			return nil, err
		}

		row := make(map[string]ValueWrapper)
		for idx, v := range values {
			col := s.table.Columns[idx]

			value, err := ParseValue(col.Type, ParseValueArgs{
				ValueWrapper: ValueWrapper{
					Value: v,
				},
			})
			if err != nil {
				return nil, err
			}

			row[col.Name] = value
		}
		rowsData = append(rowsData, row)
	}

	if len(rowsData) == 0 {
		return make([]map[string]interface{}, 0), nil
	}

	// Update the starting key so that the next scan will pick off where we last left off.
	lastRow := rowsData[len(rowsData)-1]
	for _, pk := range s.primaryKeys.Keys() {
		col, err := s.table.GetColumnByName(pk)
		if err != nil {
			return nil, err
		}

		val, err := ParseValue(col.Type, ParseValueArgs{
			ValueWrapper: lastRow[pk],
			ParseTime:    true,
		})
		if err != nil {
			return nil, err
		}

		if err := s.primaryKeys.UpdateStartingValue(pk, val.String()); err != nil {
			return nil, err
		}
	}

	var parsedRows []map[string]interface{}
	for _, row := range rowsData {
		parsedRow := make(map[string]interface{})
		for key, value := range row {
			parsedRow[key] = value.Value
		}

		parsedRows = append(parsedRows, parsedRow)
	}

	return parsedRows, nil
}

func (s *scanner) HasNext() bool {
	return !s.done
}

func (s *scanner) Next() ([]map[string]interface{}, error) {
	if !s.HasNext() {
		return nil, fmt.Errorf("no more rows to scan")
	}

	rows, err := s.scan()
	if err != nil {
		s.done = true
		return nil, err
	}

	if len(rows) == 0 || s.primaryKeys.IsExhausted() {
		slog.Info("Finished scanning", slog.String("table", s.table.Name))
		s.done = true
	}

	s.isFirstRow = false

	return rows, nil
}
