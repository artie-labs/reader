package postgres

import (
	"database/sql"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/artie-labs/transfer/lib/jitter"
	"github.com/artie-labs/transfer/lib/ptr"
	"github.com/jackc/pgx/v5"

	"github.com/artie-labs/reader/lib/postgres/primary_key"
	"github.com/artie-labs/reader/lib/postgres/schema"
)

const (
	jitterBaseMs = 300
	jitterMaxMs  = 5000
)

type scanner struct {
	// immutable
	db           *sql.DB
	table        *Table
	batchSize    uint
	errorRetries int

	// mutable
	primaryKeys *primary_key.Keys
	isFirstRow  bool
	isLastRow   bool
	done        bool
}

func (t *Table) NewScanner(db *sql.DB, batchSize uint, errorRetries int) scanner {
	return scanner{
		db:           db,
		table:        t,
		batchSize:    batchSize,
		errorRetries: errorRetries,
		primaryKeys:  t.PrimaryKeys.Clone(),
		isFirstRow:   true,
		isLastRow:    false,
		done:         false,
	}
}

type Comparison string

const (
	GreaterThan        Comparison = ">"
	GreaterThanEqualTo Comparison = ">="
)

func (c Comparison) SQLString() string {
	if (c == GreaterThan) || (c == GreaterThanEqualTo) {
		return string(c)
	}
	panic(fmt.Sprintf("invalid comparison: '%v'", c))
}

type scanTableQueryArgs struct {
	Schema      string
	TableName   string
	PrimaryKeys *primary_key.Keys
	Columns     []schema.Column

	// First where clause
	FirstWhere Comparison
	// Second where clause
	SecondWhere Comparison

	Limit uint
}

func scanTableQuery(args scanTableQueryArgs) string {
	castedColumns := make([]string, len(args.Columns))
	for idx, col := range args.Columns {
		castedColumns[idx] = castColumn(col)
	}

	startingValues := args.PrimaryKeys.KeysToValueList(args.Columns, false)
	endingValues := args.PrimaryKeys.KeysToValueList(args.Columns, true)

	return fmt.Sprintf(`SELECT %s FROM %s WHERE row(%s) %s row(%s) AND NOT row(%s) %s row(%s) ORDER BY %s LIMIT %d`,
		strings.Join(castedColumns, ","),
		pgx.Identifier{args.Schema, args.TableName}.Sanitize(),
		// WHERE row(pk) > row(123)
		strings.Join(QuotedIdentifiers(args.PrimaryKeys.Keys()), ","), args.FirstWhere.SQLString(), strings.Join(startingValues, ","),
		// AND NOT row(pk) < row(123)
		strings.Join(QuotedIdentifiers(args.PrimaryKeys.Keys()), ","), args.SecondWhere.SQLString(), strings.Join(endingValues, ","),
		strings.Join(QuotedIdentifiers(args.PrimaryKeys.Keys()), ","),
		args.Limit,
	)
}

func (s *scanner) scan(errorAttempts int) ([]map[string]interface{}, error) {
	firstWhereClause := GreaterThan
	if s.isFirstRow {
		firstWhereClause = GreaterThanEqualTo
	}

	secondWhereClause := GreaterThan
	if s.isLastRow {
		secondWhereClause = GreaterThanEqualTo
	}

	query := scanTableQuery(scanTableQueryArgs{
		Schema:      s.table.Schema,
		TableName:   s.table.Name,
		PrimaryKeys: s.primaryKeys,
		Columns:     s.table.Columns,

		FirstWhere: firstWhereClause,

		SecondWhere: secondWhereClause,

		Limit: s.batchSize,
	})

	slog.Info(fmt.Sprintf("Query looks like: %v", query))
	rows, err := s.db.Query(query)
	if err != nil {
		if attemptsLeft := s.errorRetries - errorAttempts; attemptsLeft > 0 {
			sleepDuration := jitter.Jitter(jitterBaseMs, jitterMaxMs, errorAttempts)
			slog.Info(fmt.Sprintf("We still have %v attempts", attemptsLeft), slog.Duration("sleep", sleepDuration), slog.Any("err", err))
			time.Sleep(sleepDuration)
			return s.scan(errorAttempts + 1)
		}

		return nil, err
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
	var lastRow map[string]ValueWrapper
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
		lastRow = row
	}

	// Update the starting key so that the next scan will pick off where we last left off.
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

		s.primaryKeys.Upsert(pk, ptr.ToString(val.String()), nil)
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
	rows, err := s.scan(0)
	if err != nil {
		s.done = true
		return nil, err
	} else if len(rows) == 0 {
		slog.Info("Finished scanning", slog.String("table", s.table.Name))
		s.done = true
		return nil, nil
	}
	s.isFirstRow = false
	// The reason why lastRow exists is because in the past, we had queries only return partial results but it wasn't fully done
	s.isLastRow = s.batchSize > uint(len(rows))
	return rows, nil
}
