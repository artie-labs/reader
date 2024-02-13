package postgres

import (
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5"

	"github.com/artie-labs/reader/lib/postgres/debezium"
)

type NumericDetails struct {
	FieldName  string
	Parameters map[string]interface{}
}

type Config struct {
	Fields *debezium.Fields
}

func NewPostgresConfig() *Config {
	return &Config{Fields: debezium.NewFields()}
}

// GetColEscaped will take a colName and return the escaped version of what we should be using to call Postgres.
// If it doesn't exist, we'll return an empty string.
func (c *Config) GetColEscaped(rawColName string) string {
	colName := pgx.Identifier{rawColName}.Sanitize()

	colKind := c.Fields.GetDataType(rawColName)
	switch colKind {
	case debezium.InvalidDataType:
		return colName
	case debezium.Money, debezium.TextThatRequiresEscaping:
		return fmt.Sprintf("%s::text", colName)
	case debezium.Time, debezium.Interval:
		// We want to extract(epoch from interval) will emit this in ms
		// However, Debezium wants this in macro seconds, so we are multiplying this by 1000.
		// We need to use CAST, because regular ::int makes this into a bytes array.
		// extract from epoch outputs in seconds, default multiplier to ms.
		multiplier := 1000
		if colKind == debezium.Interval {
			// ms to macro seconds.
			multiplier = 1000 * 1000
		}

		return fmt.Sprintf(`cast(extract(epoch from %s)*%d as bigint) as "%s"`, colName, multiplier, rawColName)
	case debezium.Array:
		return fmt.Sprintf(`ARRAY_TO_JSON(%s)::TEXT as "%s"`, colName, rawColName)
	case debezium.Int16, debezium.Int32, debezium.Int64, debezium.UUID, debezium.UserDefinedText,
		debezium.VariableNumeric, debezium.Numeric, debezium.Text, debezium.Boolean, debezium.Date, debezium.Timestamp, debezium.HStore, debezium.JSON, debezium.Bit:
		// These are all the columns that do not need to be escaped.
		return colName
	default:
		slog.Info("Unexpected column kind", slog.Any("colKind", colKind), slog.String("colName", colName))
		return colName
	}
}
