package postgres

import (
	"fmt"

	"github.com/artie-labs/reader/lib/postgres/schema"
	"github.com/jackc/pgx/v5"
)

// castColumn will take a colName and return the escaped version of what we should be using to call Postgres.
func castColumn(col schema.Column) (string, error) {
	colName := pgx.Identifier{col.Name}.Sanitize()
	switch col.Type {
	case schema.Inet:
		return fmt.Sprintf("%s::text", colName), nil
	case schema.TimeWithTimeZone:
		// If we don't convert `time with time zone` to UTC we end up with strings like `10:23:54-02`
		// And pgtype.Time doesn't parse the offset propertly.
		// See https://github.com/jackc/pgx/issues/1940
		return fmt.Sprintf(`%s AT TIME ZONE 'UTC' AS "%s"`, colName, col.Name), nil
	case schema.Array:
		return fmt.Sprintf(`ARRAY_TO_JSON(%s)::TEXT as "%s"`, colName, col.Name), nil
	case schema.Int16, schema.Int32, schema.Int64, schema.Real, schema.Double, schema.UUID,
		schema.UserDefinedText, schema.Text,
		schema.Money, schema.VariableNumeric, schema.Numeric,
		schema.Boolean, schema.Bit, schema.Bytea,
		schema.Time, schema.Date, schema.Timestamp, schema.Interval, schema.HStore, schema.JSON,
		schema.Point, schema.Geography, schema.Geometry:
		// These are all the columns that do not need to be escaped.
		return colName, nil
	default:
		return "", fmt.Errorf("unsupported column type: DataType(%d)", col.Type)
	}
}
