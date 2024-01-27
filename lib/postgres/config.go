package postgres

import (
	"fmt"
	"log/slog"
	"strings"

	"github.com/artie-labs/transfer/lib/ptr"
	"github.com/lib/pq"

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

// UpdateCols will inspect the colKind, if it's a special data type - it'll add it to `colsToType` with the right data type
func (c *Config) UpdateCols(colName, colKind string, precision, scale *string, udtName *string) {
	colKind = strings.ToLower(colKind)
	switch colKind {
	case "point":
		c.Fields.AddField(colName, debezium.Point, nil)
	case "real", "double precision":
		c.Fields.AddField(colName, debezium.Float, nil)
	case "smallint":
		c.Fields.AddField(colName, debezium.Int16, nil)
	case "integer":
		c.Fields.AddField(colName, debezium.Int32, nil)
	case "bigint", "oid":
		c.Fields.AddField(colName, debezium.Int64, nil)
	case "array":
		c.Fields.AddField(colName, debezium.Array, nil)
	case "bit":
		c.Fields.AddField(colName, debezium.Bit, nil)
	case "boolean":
		c.Fields.AddField(colName, debezium.Boolean, nil)
	case "date":
		c.Fields.AddField(colName, debezium.Date, nil)
	case "uuid":
		c.Fields.AddField(colName, debezium.UUID, nil)
	case "user-defined":
		if udtName != nil && *udtName == "hstore" {
			c.Fields.AddField(colName, debezium.HStore, nil)
		} else if udtName != nil && *udtName == "geometry" {
			c.Fields.AddField(colName, debezium.Geometry, nil)
		} else {
			c.Fields.AddField(colName, debezium.UserDefinedText, nil)
		}
	case "interval":
		c.Fields.AddField(colName, debezium.Interval, nil)
	case "time with time zone", "time without time zone":
		c.Fields.AddField(colName, debezium.Time, nil)
	case "money":
		c.Fields.AddField(colName, debezium.Money, &debezium.Opts{
			Scale: ptr.ToString("2"),
		})
	case "character varying", "text":
		c.Fields.AddField(colName, debezium.Text, nil)
	case "character":
		c.Fields.AddField(colName, debezium.TextThatRequiresEscaping, nil)
	case "json", "jsonb":
		c.Fields.AddField(colName, debezium.JSON, nil)
	case "timestamp without time zone", "timestamp with time zone":
		c.Fields.AddField(colName, debezium.Timestamp, nil)
	default:
		var found bool
		if strings.Contains(colKind, "numeric") {
			found = true
			if precision == nil && scale == nil {
				c.Fields.AddField(colName, debezium.VariableNumeric, nil)
			} else {
				c.Fields.AddField(colName, debezium.Numeric, &debezium.Opts{
					Scale:     scale,
					Precision: precision,
				})
			}
		}

		for _, textBasedCol := range debezium.TextBasedColumns {
			// char (m) or character
			if strings.Contains(colKind, textBasedCol) {
				c.Fields.AddField(colName, debezium.TextThatRequiresEscaping, nil)
				found = true
				break
			}
		}

		if !found {
			slog.Warn("Column type did not get mapped in our message schema, so it will not be automatically created by transfer",
				slog.String("colName", colName),
				slog.String("colKind", colKind),
			)
		}
	}
}

// GetColEscaped will take a colName and return the escaped version of what we should be using to call Postgres.
// If it doesn't exist, we'll return an empty string.
func (c *Config) GetColEscaped(rawColName string) string {
	colName := pq.QuoteIdentifier(rawColName)

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
