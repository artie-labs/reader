package postgres

import (
	"database/sql"
	"fmt"
	"log/slog"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/artie-labs/reader/lib/postgres/debezium"
	"github.com/artie-labs/reader/lib/postgres/queries"
	"github.com/artie-labs/transfer/lib/ptr"
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

		dataType, opts := colKindToDataType(colKind, numericPrecision, numericScale, udtName)
		if dataType == debezium.InvalidDataType {
			slog.Warn("Column type did not get mapped in our message schema, so it will not be automatically created by transfer",
				slog.String("colName", colName),
				slog.String("colKind", colKind),
			)
		} else {
			t.Config.Fields.AddField(colName, dataType, opts)
		}
	}

	query := fmt.Sprintf("SELECT * FROM %s LIMIT 1", pgx.Identifier{t.Schema, t.Name}.Sanitize())
	rows, err = db.Query(query)
	if err != nil {
		return fmt.Errorf("failed to query, query: %v, err: %w", query, err)
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

func colKindToDataType(colKind string, precision, scale *string, udtName *string) (debezium.DataType, *debezium.Opts) {
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
