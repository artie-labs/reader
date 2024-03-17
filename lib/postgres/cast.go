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
	case schema.TimeWithTimeZone:
		// If we don't convert `time with time zone` to UTC we end up with strings like `10:23:54-02`
		// And pgtype.Time doesn't parse the offset propertly.
		// See https://github.com/jackc/pgx/issues/1940
		return fmt.Sprintf(`%s AT TIME ZONE 'UTC' AS "%s"`, colName, col.Name), nil
	case schema.Array:
		return fmt.Sprintf(`ARRAY_TO_JSON(%s)::TEXT as "%s"`, colName, col.Name), nil
	default:
		return colName, nil
	}
}
