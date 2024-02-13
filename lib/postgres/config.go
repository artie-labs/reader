package postgres

import (
	"fmt"
	"log/slog"
	"strings"

	"github.com/artie-labs/transfer/lib/ptr"
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

func colKindToDebezium(colKind string, precision, scale *string, udtName *string) (debezium.DataType, *debezium.Opts) {
	colKind = strings.ToLower(colKind)
	switch colKind {
	case "point":
		return debezium.Point, nil
	case "real", "double precision":
		return debezium.Float, nil
	case "smallint":
		return debezium.Int16, nil
	case "integer":
		return debezium.Int32, nil
	case "bigint", "oid":
		return debezium.Int64, nil
	case "array":
		return debezium.Array, nil
	case "bit":
		return debezium.Bit, nil
	case "boolean":
		return debezium.Boolean, nil
	case "date":
		return debezium.Date, nil
	case "uuid":
		return debezium.UUID, nil
	case "user-defined":
		if udtName != nil && *udtName == "hstore" {
			return debezium.HStore, nil
		} else if udtName != nil && *udtName == "geometry" {
			return debezium.Geometry, nil
		} else if udtName != nil && *udtName == "geography" {
			return debezium.Geography, nil
		} else {
			return debezium.UserDefinedText, nil
		}
	case "interval":
		return debezium.Interval, nil
	case "time with time zone", "time without time zone":
		return debezium.Time, nil
	case "money":
		return debezium.Money, &debezium.Opts{
			Scale: ptr.ToString("2"),
		}
	case "character varying", "text":
		return debezium.Text, nil
	case "character":
		return debezium.TextThatRequiresEscaping, nil
	case "json", "jsonb":
		return debezium.JSON, nil
	case "timestamp without time zone", "timestamp with time zone":
		return debezium.Timestamp, nil
	default:
		if strings.Contains(colKind, "numeric") {
			if precision == nil && scale == nil {
				return debezium.VariableNumeric, nil
			} else {
				return debezium.Numeric, &debezium.Opts{
					Scale:     scale,
					Precision: precision,
				}
			}
		}

		for _, textBasedCol := range debezium.TextBasedColumns {
			// char (m) or character
			if strings.Contains(colKind, textBasedCol) {
				return debezium.TextThatRequiresEscaping, nil
			}
		}
	}

	return debezium.InvalidDataType, nil
}

func (c *Config) AddColumn(colName, colKind string, precision, scale *string, udtName *string) {
	dataType, opts := colKindToDebezium(colKind, precision, scale, udtName)
	if dataType == debezium.InvalidDataType {
		slog.Warn("Column type did not get mapped in our message schema, so it will not be automatically created by transfer",
			slog.String("colName", colName),
			slog.String("colKind", colKind),
		)
	} else {
		c.Fields.AddField(colName, dataType, opts)
	}
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
