package adapter

import (
	"fmt"
	"strings"

	"github.com/artie-labs/transfer/lib/debezium"

	"github.com/artie-labs/reader/lib/debezium/converters"
	"github.com/artie-labs/reader/lib/mysql"
	"github.com/artie-labs/reader/lib/mysql/schema"
)

type mysqlAdapter struct {
	table        mysql.Table
	fields       []debezium.Field
	rowConverter converters.RowConverter
}

func NewMySQLAdapter(table mysql.Table) (mysqlAdapter, error) {
	fields := make([]debezium.Field, len(table.Columns))
	valueConverters := map[string]converters.ValueConverter{}
	for i, col := range table.Columns {
		converter, err := valueConverterForType(col.Type, col.Opts)
		if err != nil {
			return mysqlAdapter{}, err
		}
		fields[i] = converter.ToField(col.Name)
		valueConverters[col.Name] = converter
	}

	return mysqlAdapter{
		table:        table,
		fields:       fields,
		rowConverter: converters.NewRowConverter(valueConverters),
	}, nil
}

func (m mysqlAdapter) TableName() string {
	return m.table.Name
}

func (m mysqlAdapter) TopicSuffix() string {
	return strings.ReplaceAll(m.table.Name, `"`, ``)
}

func (m mysqlAdapter) Fields() []debezium.Field {
	return m.fields
}

// PartitionKey returns a map of primary keys and their values for a given row.
func (m mysqlAdapter) PartitionKey(row map[string]any) map[string]any {
	result := make(map[string]any)
	for _, key := range m.table.PrimaryKeys {
		result[key.Name] = row[key.Name]
	}
	return result
}

func (m mysqlAdapter) ConvertRowToDebezium(row map[string]any) (map[string]any, error) {
	return m.rowConverter.Convert(row)
}

func valueConverterForType(d schema.DataType, opts *schema.Opts) (converters.ValueConverter, error) {
	switch d {
	case schema.Bit, schema.Boolean:
		return converters.BooleanPassthrough{}, nil
	case schema.TinyInt, schema.SmallInt:
		return converters.Int16Passthrough{}, nil
	case schema.MediumInt, schema.Int:
		return converters.Int32Passthrough{}, nil
	case schema.BigInt:
		return converters.Int64Passthrough{}, nil
	case schema.Float:
		return converters.FloatPassthrough{}, nil
	case schema.Double:
		return converters.DoublePassthrough{}, nil
	case schema.Decimal:
		if opts.Scale == nil {
			return nil, fmt.Errorf("scale is required for decimal type")
		}

		return converters.NewDecimalConverter(*opts.Scale, opts.Precision), nil
	case schema.Char, schema.Text, schema.Varchar, schema.TinyText, schema.MediumText, schema.LongText:
		return converters.StringPassthrough{}, nil
	case schema.Binary, schema.Varbinary, schema.Blob:
		return converters.BytesPassthrough{}, nil
	case schema.Time:
		return converters.MicroTimeConverter{}, nil
	case schema.Date:
		return converters.DateConverter{}, nil
	case schema.DateTime, schema.Timestamp:
		return converters.TimestampConverter{}, nil
	case schema.Year:
		return converters.YearConverter{}, nil
	case schema.Enum:
		return converters.EnumConverter{}, nil
	case schema.Set:
		return converters.EnumSetConverter{}, nil
	case schema.JSON:
		return converters.JSONConverter{}, nil
	}
	return nil, fmt.Errorf("unable get value converter for DataType(%d)", d)
}
