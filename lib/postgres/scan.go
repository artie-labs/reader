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

func (s *scanner) startScanning(errorAttempts int) ([]map[string]interface{}, error) {
	firstWhereClause := queries.GreaterThan
	if s.firstRow {
		firstWhereClause = queries.GreaterThanEqualTo
	}

	secondWhereClause := queries.GreaterThan
	if s.lastRow {
		secondWhereClause = queries.GreaterThanEqualTo
	}

	startKeys := s.primaryKeys.KeysToValueList(s.table.Config.Fields.GetOptionalSchema(), false)
	endKeys := s.primaryKeys.KeysToValueList(s.table.Config.Fields.GetOptionalSchema(), true)

	query := queries.ScanTableQuery(queries.ScanTableQueryArgs{
		Schema:        s.table.Schema,
		TableName:     s.table.Name,
		PrimaryKeys:   s.table.PrimaryKeys.Keys(),
		ColumnsToScan: s.table.ColumnsCastedForScanning,

		FirstWhere:   firstWhereClause,
		StartingKeys: startKeys,

		SecondWhere: secondWhereClause,
		EndingKeys:  endKeys,

		OrderBy: s.table.PrimaryKeys.Keys(),
		Limit:   s.batchSize,
	})

	slog.Info(fmt.Sprintf("Query looks like: %v", query))
	rows, err := s.db.Query(query)
	if err != nil {
		if attempts := (s.errorRetries - errorAttempts); attempts > 0 {
			sleepMs := lib.JitterMs(jitterBaseMs, jitterMaxMs, errorAttempts)
			slog.Info(fmt.Sprintf("We still have %v attempts", attempts), slog.Int("sleepMs", sleepMs), slog.Any("err", err))
			time.Sleep(time.Duration(sleepMs) * time.Millisecond)
			return s.startScanning(errorAttempts + 1)
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
			value, err := s.table.Config.ParseValue(ParseValueArgs{
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
	for _, pk := range s.primaryKeys.Keys() {
		val, err := s.table.Config.ParseValue(ParseValueArgs{
			ColName:      pk,
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

func (t *Table) NewScanner(db *sql.DB, batchSize uint, errorRetries int) scanner {
	return scanner{
		db:           db,
		table:        t,
		batchSize:    batchSize,
		errorRetries: errorRetries,
		primaryKeys:  t.PrimaryKeys, // TODO: We should be passing in a copy of the primary keys
		firstRow:     true,
		lastRow:      false,
		done:         false,
	}
}

type scanner struct {
	// immutable
	db           *sql.DB
	table        *Table
	batchSize    uint
	errorRetries int

	// mutable
	primaryKeys *primary_key.Keys
	firstRow    bool
	lastRow     bool
	done        bool
}

func (s *scanner) HasNext() bool {
	return !s.done
}

func (s *scanner) Next() ([]map[string]interface{}, error) {
	if !s.HasNext() {
		return nil, fmt.Errorf("no more rows to scan")
	}
	rows, err := s.startScanning(0)
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
