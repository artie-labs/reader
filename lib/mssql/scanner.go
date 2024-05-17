package mssql

import (
	"database/sql"
	"fmt"
	"github.com/artie-labs/transfer/clients/mssql/dialect"
	mssql "github.com/microsoft/go-mssqldb"
	"slices"
	"strings"

	"github.com/artie-labs/reader/lib/mssql/parse"
	"github.com/artie-labs/reader/lib/mssql/schema"
	"github.com/artie-labs/reader/lib/rdbms/column"
	"github.com/artie-labs/reader/lib/rdbms/primary_key"
	"github.com/artie-labs/reader/lib/rdbms/scan"
)

var supportedPrimaryKeyDataType = []schema.DataType{
	schema.Bit,
	schema.Bytes,

	schema.Int16,
	schema.Int32,
	schema.Int64,
	schema.Numeric,
	schema.Float,

	schema.Money,
	schema.Date,
	schema.String,

	schema.Time,
	schema.TimeMicro,
	schema.TimeNano,

	schema.Datetime2,
	schema.Datetime2Micro,
	schema.Datetime2Nano,

	schema.DatetimeOffset,
}

func NewScanner(db *sql.DB, table Table, columns []schema.Column, cfg scan.ScannerConfig) (*scan.Scanner, error) {
	for _, key := range table.PrimaryKeys {
		_column, err := column.GetColumnByName(columns, key)
		if err != nil {
			return nil, fmt.Errorf("missing column with name: %q", key)
		}

		if !slices.Contains(supportedPrimaryKeyDataType, _column.Type) {
			return nil, fmt.Errorf("DataType(%d) for column %q is not supported for use as a primary key", _column.Type, _column.Name)
		}
	}

	primaryKeyBounds, err := table.GetPrimaryKeysBounds(db)
	if err != nil {
		return nil, err
	}

	adapter := scanAdapter{schema: table.Schema, tableName: table.Name, columns: columns}
	return scan.NewScanner(db, primaryKeyBounds, cfg, adapter)
}

type scanAdapter struct {
	schema    string
	tableName string
	columns   []schema.Column
}

func (s scanAdapter) ParsePrimaryKeyValue(columnName string, value string) (any, error) {
	columnIdx := slices.IndexFunc(s.columns, func(x schema.Column) bool { return x.Name == columnName })
	if columnIdx < 0 {
		return nil, fmt.Errorf("primary key column does not exist: %q", columnName)
	}

	_column := s.columns[columnIdx]
	if !slices.Contains(supportedPrimaryKeyDataType, _column.Type) {
		return nil, fmt.Errorf("DataType(%d) for column %q is not supported for use as a primary key", _column.Type, _column.Name)
	}

	fmt.Println("val", value, "column.Type", _column.Type)
	return value, fmt.Errorf("hello")

	//switch column.Type {
	//case schema.Boolean:
	//	boolValue, err := strconv.ParseBool(value)
	//	if err != nil {
	//		return nil, fmt.Errorf("unable to convert %q to a bool: %w", value, err)
	//	}
	//	return boolValue, nil
	//case schema.Int16:
	//	intValue, err := strconv.ParseInt(value, 10, 16)
	//	if err != nil {
	//		return nil, fmt.Errorf("unable to convert %q to an int16: %w", value, err)
	//	}
	//	return int16(intValue), nil
	//case schema.Int32:
	//	intValue, err := strconv.ParseInt(value, 10, 32)
	//	if err != nil {
	//		return nil, fmt.Errorf("unable to convert %q to an int32: %w", value, err)
	//	}
	//	return int32(intValue), nil
	//case schema.Int64:
	//	intValue, err := strconv.ParseInt(value, 10, 64)
	//	if err != nil {
	//		return nil, fmt.Errorf("unable to convert %q to an int64: %w", value, err)
	//	}
	//	return intValue, nil
	//case schema.Real:
	//	floatValue, err := strconv.ParseFloat(value, 32)
	//	if err != nil {
	//		return nil, fmt.Errorf("unable to convert %q to a float32: %w", value, err)
	//	}
	//	return float32(floatValue), nil
	//case schema.Double:
	//	floatValue, err := strconv.ParseFloat(value, 64)
	//	if err != nil {
	//		return nil, fmt.Errorf("unable to convert %q to a float64: %w", value, err)
	//	}
	//	return floatValue, nil
	//case
	//	schema.Numeric,
	//	schema.VariableNumeric,
	//	schema.Text,
	//	schema.Money,
	//	schema.UserDefinedText,
	//	schema.Time,
	//	schema.Date,
	//	schema.Timestamp,
	//	schema.TimestampWithTimeZone,
	//	schema.Interval,
	//	schema.UUID,
	//	schema.JSON:
	//	return value, nil
	//default:
	//	return nil, fmt.Errorf("primary key value parsing not implemented for DataType(%d)", column.Type)
	//}
}

func queryPlaceholders(count int) []string {
	var results []string
	for range count {
		results = append(results, "?")
	}

	return results
}

func mssqlVarCharJoin(values []mssql.VarChar, sep string) string {
	parts := make([]string, len(values))
	for i, val := range values {
		parts[i] = string(val)
	}
	return strings.Join(parts, sep)
}

func (s scanAdapter) BuildQuery(primaryKeys []primary_key.Key, isFirstBatch bool, batchSize uint) (string, []any) {
	mssqlDialect := dialect.MSSQLDialect{}

	colNames := make([]string, len(s.columns))
	for idx, col := range s.columns {
		colNames[idx] = mssqlDialect.QuoteIdentifier(col.Name)
	}

	startingValues := make([]any, len(primaryKeys))
	endingValues := make([]any, len(primaryKeys))
	for i, pk := range primaryKeys {
		startingValues[i] = pk.StartingValue
		endingValues[i] = pk.EndingValue
	}

	quotedKeyNames := make([]mssql.VarChar, len(primaryKeys))
	for i, key := range primaryKeys {
		quotedKeyNames[i] = mssql.VarChar(key.Name)
	}

	lowerBoundComparison := ">"
	if isFirstBatch {
		lowerBoundComparison = ">="
	}

	return fmt.Sprintf(`SELECT TOP %d %s FROM %s.%s WHERE (%s) %s (%s) AND (%s) <= (%s) ORDER BY %s`,
		// TOP
		batchSize,
		// SELECT
		strings.Join(colNames, ","),
		// FROM
		mssqlDialect.QuoteIdentifier(s.schema), mssqlDialect.QuoteIdentifier(s.tableName),
		// WHERE (pk) > (123)
		mssqlVarCharJoin(quotedKeyNames, ","), lowerBoundComparison, strings.Join(queryPlaceholders(len(startingValues)), ","),
		// AND NOT (pk) <= (123)
		mssqlVarCharJoin(quotedKeyNames, ","), strings.Join(queryPlaceholders(len(endingValues)), ","),
		// ORDER BY
		mssqlVarCharJoin(quotedKeyNames, ","),
	), slices.Concat(startingValues, endingValues)
}

func (s scanAdapter) ParseRow(values []any) error {
	for i, value := range values {
		var err error
		if values[i], err = parse.ParseValue(s.columns[i].Type, value); err != nil {
			return fmt.Errorf("failed to parse column: %s: %w", s.columns[i].Name, err)
		}
	}
	return nil
}
