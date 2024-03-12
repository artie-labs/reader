package scanner

import (
	"database/sql"
	"fmt"
	"slices"
	"strconv"
	"time"

	"github.com/artie-labs/reader/lib/mysql"
	"github.com/artie-labs/reader/lib/mysql/schema"
	"github.com/artie-labs/reader/lib/rdbms/primary_key"
	"github.com/artie-labs/reader/lib/rdbms/scan"
)

func NewScanner(db *sql.DB, table mysql.Table, cfg scan.ScannerConfig) (*scan.Scanner, error) {
	primaryKeyBounds, err := table.GetPrimaryKeysBounds(db)
	if err != nil {
		return nil, err
	}

	adapter := scanAdapter{tableName: table.Name, columns: table.Columns}
	return scan.NewScanner(db, primaryKeyBounds, cfg, adapter)
}

type scanAdapter struct {
	tableName string
	columns   []schema.Column
}

func (s scanAdapter) ParsePrimaryKeyValue(columnName string, value string) (any, error) {
	columnIdx := slices.IndexFunc(s.columns, func(x schema.Column) bool { return x.Name == columnName })
	if columnIdx < 0 {
		return nil, fmt.Errorf("primary key column does not exist: %s", columnName)
	}
	column := s.columns[columnIdx]
	switch column.Type {
	case schema.Bit, schema.Boolean:
		boolValue, err := strconv.ParseBool(value)
		if err != nil {
			return nil, fmt.Errorf(`unable to convert "%s" to a bool`, value)
		}
		return boolValue, nil
	case
		schema.TinyInt,
		schema.SmallInt,
		schema.MediumInt,
		schema.Int,
		schema.BigInt,
		schema.Year:
		intValue, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return nil, fmt.Errorf(`unable to convert "%s" to an int64: %w`, value, err)
		}
		return intValue, nil
	case schema.Float:
		floatValue, err := strconv.ParseFloat(value, 32)
		if err != nil {
			return nil, fmt.Errorf(`unable to convert "%s" to a float32: %w`, value, err)
		}
		return float32(floatValue), nil
	case schema.Double:
		floatValue, err := strconv.ParseFloat(value, 64)
		if err != nil {
			return nil, fmt.Errorf(`unable to convert "%s" to a float64: %w`, value, err)
		}
		return floatValue, nil
	case schema.DateTime, schema.Timestamp:
		timeValue, err := time.Parse(schema.DateTimeFormat, value)
		if err != nil {
			return nil, err
		}
		return timeValue, nil
	case
		schema.Decimal,
		schema.Time,
		schema.Date,
		schema.Char,
		schema.Varchar,
		schema.Text,
		schema.TinyText,
		schema.MediumText,
		schema.LongText,
		schema.Enum,
		schema.Set,
		schema.JSON:
		return value, nil
	default:
		return nil, fmt.Errorf("primary key value parsing not implemented for DataType(%d)", column.Type)
	}
}

func (s scanAdapter) BuildQuery(primaryKeys []primary_key.Key, isFirstBatch bool, batchSize uint) (string, []any, error) {
	return buildScanTableQuery(buildScanTableQueryArgs{
		TableName:           s.tableName,
		PrimaryKeys:         primaryKeys,
		Columns:             s.columns,
		InclusiveLowerBound: isFirstBatch,
		Limit:               batchSize,
	})
}

func (s scanAdapter) ParseRow(values []any) (map[string]any, error) {
	convertedValues, err := schema.ConvertValues(values, s.columns)
	if err != nil {
		return nil, err
	}

	row := make(map[string]any)
	for idx, value := range convertedValues {
		row[s.columns[idx].Name] = value
	}
	return row, nil
}
