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
	case schema.Time:
		// If we don't convert "time with time zone" to UTC we end up with strings like `10:23:54+02
		// And pgtype.Time doesn't parse the offset propertly.
		return fmt.Sprintf(`%s AT TIME ZONE 'UTC' AS "%s"`, colName, col.Name), nil
	case schema.Interval:
		// We want to extract(epoch from interval) will emit this in ms
		// However, Debezium wants this in macro seconds, so we are multiplying this by 1000.
		// We need to use CAST, because regular ::int makes this into a bytes array.
		// extract from epoch outputs in seconds, default multiplier to ms.
		return fmt.Sprintf(`cast(extract(epoch from %s)*%d as bigint) as "%s"`, colName, 1000*1000, col.Name), nil
	case schema.Array:
		return fmt.Sprintf(`ARRAY_TO_JSON(%s)::TEXT as "%s"`, colName, col.Name), nil
	case schema.Int16, schema.Int32, schema.Int64, schema.Float, schema.UUID,
		schema.UserDefinedText, schema.Text,
		schema.Money, schema.VariableNumeric, schema.Numeric,
		schema.Boolean, schema.Bit, schema.Bytea,
		schema.Date, schema.Timestamp, schema.HStore, schema.JSON,
		schema.Point, schema.Geography, schema.Geometry:
		// These are all the columns that do not need to be escaped.
		return colName, nil
	default:
		return "", fmt.Errorf("unsupported column type: DataType(%d)", col.Type)
	}
}
