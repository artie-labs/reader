package postgres

import (
	"database/sql"
	"fmt"
	"log/slog"

	"github.com/artie-labs/reader/lib/postgres/schema"
	"github.com/jackc/pgx/v5"
)

func (t *Table) PopulateColumns(db *sql.DB) error {
	cols, err := schema.DescribeTable(db, t.Schema, t.Name)
	if err != nil {
		return fmt.Errorf("failed to describe table %s.%s: %w", t.Schema, t.Name, err)
	}

	for _, col := range cols {
		t.Fields.AddField(col.Name, col.Type, col.Opts)
		// Add to original columns before mutation
		t.OriginalColumns = append(t.OriginalColumns, col.Name)
		t.ColumnsCastedForScanning = append(t.ColumnsCastedForScanning, castColumn(col.Name, col.Type))
	}

	return t.FindStartAndEndPrimaryKeys(db)
}

// castColumn will take a colName and return the escaped version of what we should be using to call Postgres.
func castColumn(rawColName string, colKind schema.DataType) string {
	colName := pgx.Identifier{rawColName}.Sanitize()
	switch colKind {
	case schema.InvalidDataType:
		return colName
	case schema.Money, schema.TextThatRequiresEscaping:
		return fmt.Sprintf("%s::text", colName)
	case schema.Time, schema.Interval:
		// We want to extract(epoch from interval) will emit this in ms
		// However, Debezium wants this in macro seconds, so we are multiplying this by 1000.
		// We need to use CAST, because regular ::int makes this into a bytes array.
		// extract from epoch outputs in seconds, default multiplier to ms.
		multiplier := 1000
		if colKind == schema.Interval {
			// ms to macro seconds.
			multiplier = 1000 * 1000
		}

		return fmt.Sprintf(`cast(extract(epoch from %s)*%d as bigint) as "%s"`, colName, multiplier, rawColName)
	case schema.Array:
		return fmt.Sprintf(`ARRAY_TO_JSON(%s)::TEXT as "%s"`, colName, rawColName)
	case schema.Int16, schema.Int32, schema.Int64, schema.UUID, schema.UserDefinedText,
		schema.VariableNumeric, schema.Numeric, schema.Text, schema.Boolean, schema.Date, schema.Timestamp, schema.HStore, schema.JSON, schema.Bit:
		// These are all the columns that do not need to be escaped.
		return colName
	default:
		slog.Info("Unexpected column kind", slog.Any("colKind", colKind), slog.String("colName", colName))
		return colName
	}
}
