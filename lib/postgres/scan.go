package postgres

import (
	"database/sql"
	"fmt"
	"log/slog"
	"time"

	"github.com/artie-labs/transfer/lib/ptr"

	"github.com/artie-labs/reader/lib"
	"github.com/artie-labs/reader/lib/postgres/primary_key"
	"github.com/artie-labs/reader/lib/postgres/queries"
)

const (
	jitterBaseMs = 300
	jitterMaxMs  = 5000
)

type ScanningArgs struct {
	PrimaryKeys *primary_key.Keys

	Limit      uint
	IsFirstRow bool
	IsLastRow  bool

	errorAttempts        int // Which attempt are you on?
	NumberOfErrorRetries int // How many retries do you have?
}

func (s *ScanningArgs) RetryAttempts() int {
	return s.NumberOfErrorRetries - s.errorAttempts
}

func NewScanningArgs(primaryKeys *primary_key.Keys, limit uint, errorRetries int, isFirstRow, isLastRow bool) ScanningArgs {
	return ScanningArgs{
		PrimaryKeys:          primaryKeys,
		Limit:                limit,
		IsFirstRow:           isFirstRow,
		IsLastRow:            isLastRow,
		errorAttempts:        0,
		NumberOfErrorRetries: errorRetries,
	}
}

func (t *Table) startScanning(db *sql.DB, scanningArgs ScanningArgs) ([]map[string]interface{}, error) {
	firstWhereClause := queries.GreaterThan
	if scanningArgs.IsFirstRow {
		firstWhereClause = queries.GreaterThanEqualTo
	}

	secondWhereClause := queries.GreaterThan
	if scanningArgs.IsLastRow {
		secondWhereClause = queries.GreaterThanEqualTo
	}

	startKeys := scanningArgs.PrimaryKeys.KeysToValueList(t.Config.Fields.GetOptionalSchema(), false)
	endKeys := scanningArgs.PrimaryKeys.KeysToValueList(t.Config.Fields.GetOptionalSchema(), true)

	query := queries.ScanTableQuery(queries.ScanTableQueryArgs{
		Schema:        t.Schema,
		TableName:     t.Name,
		PrimaryKeys:   t.PrimaryKeys.Keys(),
		ColumnsToScan: t.ColumnsCastedForScanning,

		FirstWhere:   firstWhereClause,
		StartingKeys: startKeys,

		SecondWhere: secondWhereClause,
		EndingKeys:  endKeys,

		OrderBy: t.PrimaryKeys.Keys(),
		Limit:   scanningArgs.Limit,
	})

	slog.Info(fmt.Sprintf("Query looks like: %v", query))
	rows, err := db.Query(query)
	if err != nil {
		if attempts := scanningArgs.RetryAttempts(); attempts > 0 {
			sleepMs := lib.JitterMs(jitterBaseMs, jitterMaxMs, scanningArgs.errorAttempts)
			slog.Info(fmt.Sprintf("We still have %v attempts", attempts), slog.Int("sleepMs", sleepMs), slog.Any("err", err))
			scanningArgs.errorAttempts += 1
			time.Sleep(time.Duration(sleepMs) * time.Millisecond)
			return t.startScanning(db, scanningArgs)
		}

		return nil, err
	}

	columns, err := rows.Columns()
	if err != nil {
		return nil, err
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
		for k, v := range values {
			colName := columns[k]
			value, err := t.Config.ParseValue(ParseValueArgs{
				ColName: colName,
				ValueWrapper: ValueWrapper{
					Value: v,
				},
			})

			if err != nil {
				return nil, err
			}

			row[colName] = value
		}

		rowsData = append(rowsData, row)
		lastRow = row
	}

	// Update the starting key so that the next scan will pick off where we last left off.
	for _, pk := range scanningArgs.PrimaryKeys.Keys() {
		val, err := t.Config.ParseValue(ParseValueArgs{
			ColName:      pk,
			ValueWrapper: lastRow[pk],
			ParseTime:    true,
		})

		if err != nil {
			return nil, err
		}

		scanningArgs.PrimaryKeys.Upsert(pk, ptr.ToString(val.String()), nil)
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

func (t *Table) NewScanner(db *sql.DB, batchSize uint, errorRetries int) scanner {
	return scanner{
		db:           db,
		table:        t,
		batchSize:    batchSize,
		errorRetries: errorRetries,
		firstRow:     true,
		lastRow:      false,
		done:         false,
	}
}

type scanner struct {
	db           *sql.DB
	table        *Table
	batchSize    uint
	errorRetries int

	firstRow bool
	lastRow  bool
	done     bool
}

func (s *scanner) HasNext() bool {
	return !s.done
}

func (s *scanner) Next() ([]map[string]interface{}, error) {
	if !s.HasNext() {
		return nil, fmt.Errorf("no more rows to scan")
	}
	rows, err := s.table.startScanning(s.db,
		NewScanningArgs(s.table.PrimaryKeys, s.batchSize, s.errorRetries, s.firstRow, s.lastRow),
	)
	if err != nil {
		s.done = true
		return nil, err
	} else if len(rows) == 0 {
		slog.Info("Finished scanning", slog.String("table", s.table.Name))
		s.done = true
		return nil, nil
	}
	s.firstRow = false
	// The reason why lastRow exists is because in the past, we had queries only return partial results but it wasn't fully done
	s.lastRow = s.batchSize > uint(len(rows))
	return rows, nil
}
