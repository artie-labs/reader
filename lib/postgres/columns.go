package postgres

import (
	"database/sql"
	"fmt"

	"github.com/lib/pq"

	"github.com/artie-labs/reader/lib/postgres/queries"
)

func (t *Table) RetrieveColumns(db *sql.DB) error {
	describeQuery, describeArgs := queries.DescribeTableQuery(queries.DescribeTableArgs{
		Name:   t.Name,
		Schema: t.Schema,
	})

	rows, err := db.Query(describeQuery, describeArgs...)
	if err != nil {
		return fmt.Errorf("failed to query: %s, args: %v, err: %w", describeQuery, describeArgs, err)
	}

	for rows.Next() {
		var colName string
		var colKind string
		var numericPrecision *string
		var numericScale *string
		var udtName *string
		err = rows.Scan(&colName, &colKind, &numericPrecision, &numericScale, &udtName)
		if err != nil {
			return err
		}

		t.Config.UpdateCols(colName, colKind, numericPrecision, numericScale, udtName)
	}

	query := fmt.Sprintf("SELECT * from %s.%s LIMIT 1", pq.QuoteIdentifier(t.Schema), pq.QuoteIdentifier(t.Name))
	rows, err = db.Query(query)
	if err != nil {
		return fmt.Errorf("failed to query, query: %v, err: %v", query, err)
	}

	columns, err := rows.Columns()
	if err != nil {
		return err
	}

	for _, column := range columns {
		// Add to original columns before mutation
		t.OriginalColumns = append(t.OriginalColumns, column)
		t.ColumnsCastedForScanning = append(t.ColumnsCastedForScanning, t.Config.GetColEscaped(column))
	}

	return t.FindStartAndEndPrimaryKeys(db)
}
